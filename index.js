const _ = require('lodash')
const uuid = require('uuid')
const RID = '@rid'
const TYPE = '@type'
const VERTEX = 'VERTEX'
const EDGE = 'EDGE'
const CLASS = '@class'
const REF = '@ref'
const UPSERT = '@upsert'
const DELETED = '@deleted'
const REFS = '@refs'
const { AsyncGenerator } = require('generator-extensions')

const of = (x) => AsyncGenerator.of(Promise.resolve(typeof x === 'function' ? x() : x))

const isTraversal = (t) => t && t instanceof Traversal
function assertIsTraversal (t) {
  if (!isTraversal(t)) throw new Error('Expected Traversal')
}
const isVertex = (v) => v && v[TYPE] === VERTEX
function assertIsVertex (v) {
  if (!isVertex(v)) throw new Error('Expected Vertex')
}
const isEdge = (e) => e && e[TYPE] === EDGE
function assertIsEdge (e) {
  if (!isEdge(e)) throw new Error('Expected Edge')
}
const isLabel = (s) => s && typeof s === 'string'
function assertIsLabel (s) {
  if (!isLabel(s)) throw new Error('Expected non-empty string as Label')
}
const isNode = (n) => n && n[TYPE] === VERTEX || n[TYPE] === EDGE
function assertIsNode (s) {
  if (!isNode(s)) throw new Error('Expected Vertex or Edge')
}

const merge = (parent, traversals) => new Traversal(parent.graph, parent, AsyncGenerator.from([
  ...traversals.map(x => isTraversal(x) ? x.nodes : x)
]).flat())

const singleVertex = async (v) => {
  if (isTraversal(v)) return singleVertex(await v.toArray())
  if (isVertex(v)) return v
  if (Array.isArray(v)) {
    if (v.length !== 1) throw new Error(`Expected 1 Vertex. Got ${v.length}.`)
    return singleVertex(v[0])
  }
  throw new Error('Expected Vertex, Traversal, or Array')
}
const singleEdge = async (v) => {
  if (isTraversal(v)) return singleEdge(await v.toArray())
  if (isEdge(v)) return v
  if (Array.isArray(v)) {
    if (v.length !== 1) throw new Error(`Expected 1 Edge. Got ${v.length}.`)
    return singleEdge(v[0])
  }
  throw new Error('Expected Edge, Traversal, or Array')
}

const outKey = (label) => `out_${label}`
const inKey = (label) => `in_${label}`
const identity = x => x
const truthy = x => !!x

class MemoryDriver {
  constructor() {
    this.data = {
      [VERTEX]: new Map(),
      [EDGE]: new Map()
    }
  }
  async newId (type, obj) {
    return uuid.v4()
  }
  async read(type, rid) {
    return this.data[type][rid] || null
  }
  async write(type, rid, data) {
    this.data[type][rid] = data
  }
  async delete(type, rid) {
    delete this.data[type][rid]
  }
}

class Graph {
  constructor(driver = new MemoryDriver) {
    this._driver = driver
    this._mutations = new Map()
  }
  /** @private */
  async mutate (objs, fn) {
    if (!Array.isArray(objs)) return (await this.mutate([objs], ([obj]) => fn(obj)))[0]
    const activeObjs = objs.map(obj => {
      const id = obj[RID]
      if (!id) throw new Error('Mutated object just have an id')
      if (!this._mutations.has(id)) {
        this._mutations.set(id, obj)
        obj[REFS] = 1 // Reference count
      } else {
        obj = this._mutations.get(id)
        obj[REFS]++
      }
      return obj
    })
    await fn(activeObjs)
    await Promise.all(activeObjs.map(async obj => {
      obj[REFS]--
      if (obj[REFS] === 0) {
        delete obj[REFS]
        this._mutations.delete(obj[RID])
        if (obj[DELETED]) await this._driver.delete(obj[TYPE], obj[RID])
        else await this._driver.write(obj[TYPE], obj[RID], obj)
      }
    }))
    return activeObjs
  }
  addVertex (obj) {
    return new Traversal(this, null, of(async () => {
      const rec = {
        [RID]: null,
        [TYPE]: VERTEX,
        ...obj
      }
      rec[RID] = await this._driver.newId(VERTEX, rec)
      const vertex = await this.mutate(rec, identity)
      return vertex
    }))
  }
  addEdge (outV, inV, label, obj = {}) {
    return new Traversal(this, null, of(async () => {
      outV = await singleVertex(outV)
      inV = await singleVertex(inV)
      assertIsLabel(label)
      const rec = {
        [RID]: null,
        [TYPE]: EDGE,
        label,
        out: outV[RID],
        in: inV[RID],
      }
      rec[RID] = await this._driver.newId(EDGE, rec)
      const [edge] = await this.mutate([rec, outV, inV], async ([edge, outV, inV]) => {
        const ok = outKey(label)
        const ik = inKey(label)
        if (!outV[ok]) outV[ok] = []
        if (!inV[ik]) inV[ik] = []
        outV[ok].push(edge[RID])
        inV[ik].push(edge[RID])
      })
      return edge
    }))
  }
  async remove (node) {
    if (node[TYPE] === VERTEX) {
      await this.mutate(node, async vertex => {
        const edges = Object.keys(vertex)
          .filter(k => k.startsWith('out_') || k.startsWith('in_'))
          .flatMap(k => vertex[k])
        await AsyncGenerator.from(edges)
          .forEach(async e => await this.remove(await this._driver.read(EDGE, e)))
        vertex[DELETED] = true
      })
    } else if (node[TYPE] === EDGE) {
      await this.mutate(node, async edge => {
        const [outV, inV] = await Promise.all([
          this._driver.read(VERTEX, edge.out),
          this._driver.read(VERTEX, edge.in)
        ])
        await this.mutate([outV, inV], async ([outV, inV]) => {
          const ok = outKey(edge.label)
          const ik = inKey(edge.label)
          const outIdx = outV[ok].indexOf(edge[RID])
          const inIdx = inV[ik].indexOf(edge[RID])
          if (outIdx >= 0) outV[ok].splice(outIdx, 1)
          if (inIdx >= 0) inV[ik].splice(inIdx, 1)
        })
        edge[DELETED] = true
      })
    }
  }
  v (rid) {
    if (rid && rid[RID]) rid = rid[RID]
    if (rid) {
      return new Traversal(this, null, AsyncGenerator.of(this._driver.read(VERTEX, rid))
        .filter(identity).filter(x => x[TYPE] === VERTEX))
    } else {
      throw new Error('No vertex id provided')
      // Should not be supported. You should have a starting point, otherwise this is a scan.
      return new Traversal(this, null, AsyncGenerator.of(this._driver.read(VERTEX, null)))
    }
  }
  e (rid) {
    if (rid && rid[RID]) rid = rid[RID]
    if (rid) {
      return new Traversal(this, null, AsyncGenerator.of(this._driver.read(EDGE, rid))
        .filter(identity).filter(x => x[TYPE] === EDGE))
    } else {
      throw new Error('No edge id provided')
      // Should not be supported. Traversals should start at a vertex, definitely not scan all edges.
      return new Traversal(this, null, AsyncGenerator.of(this._driver.read(EDGE, null)))
    }
  }
}

class Traversal {
  constructor (graph, parent, nodes) {
    /** @private */
    this.parent = parent
    /** @private */
    this.graph = graph
    /** @private */
    this.nodes = nodes
    if (!nodes[Symbol.asyncIterator]) {
      throw new Error('Traversal nodes must be an async iterator')
    }
    /** @private */
    this._store = parent ? Object.create(parent._store) : {}
    /** @private */
    this.name = null
  }
  /** @private */
  next (fn) {
    return new Traversal(this.graph, this, (async function*() {
      if (!this.nodes || !this.nodes[Symbol.asyncIterator]) {
        throw new Error('Traversal nodes must be an async iterator')
      }
      const nextNodes = await fn(this.nodes)
      if (!nextNodes || !nextNodes[Symbol.asyncIterator]) {
        throw new Error(`Function did not return an iterator (${fn.toString()})`)
      }
      yield* nextNodes
    }).call(this))
  }

  addEdge (label, other, obj = {}) {
    if (!isTraversal(other)) other = this.graph.v(other)
    return this.next(async nodes => nodes.flatMap(async node => {
      const others = await other.toArray()
      return others.map(other => this.graph.addEdge(node, other, label, obj))
    }))
  }
  has (obj) {
    return this.next(async nodes => {
      return nodes.filter(node => _.isMatch(node, obj))
    })
  }
  
  hasNot (obj) {
    return this.next(async nodes => {
      return nodes.filter(node => !_.isMatch(node, obj))
    })
  }
  outE (label) {
    const key = outKey(label)
    return this.next(async nodes => nodes
      .flatMap(async node => node[key])
      .map(rid => this.graph._driver.read(EDGE, rid)))
  }
  inE (label) {
    const key = inKey(label)
    return this.next(async nodes => nodes
      .flatMap(async node => node[key])
      .map(rid => this.graph._driver.read(EDGE, rid)))
  }
  bothE (label) {
    const ik = inKey(label)
    const ok = outKey(label)
    return this.next(async nodes => nodes
      .flatMap(async node => [...(node[ik]||[]), ...(node[ok]||[])])
      .map(rid => this.graph._driver.read(EDGE, rid))
    )
  }
  outV () {
    return this.next(async nodes => nodes
      .map(async node => node.out)
      .filter(truthy)
      .map(rid => this.graph._driver.read(VERTEX, rid)))
  }
  inV () {
    return this.next(async nodes => nodes
      .map(async node => node.in)
      .filter(truthy)
      .map(rid => this.graph._driver.read(VERTEX, rid)))
  }
  bothV () {
    return this.next(async nodes => nodes
      .flatMap(async node => [node.in, node.out])
      .filter(truthy)
      .map(rid => this.graph._driver.read(VERTEX, rid))
    )
  }
  out (label) {
    return this.outE(label).inV()
  }
  in (label) {
    return this.inE(label).outV()
  }
  both (label) {
    return this.bothE(label).bothV()
  }
  dedup () {
    var rids = new Set()
    return this._next(
      this.nodes.filter((n) =>
        rids.has(n[RID]) ? false : rids.add(n[RID])
      )
    )
  }
  or (conditions) {
    return this._boolean((a, b) => a || b, false, Array.from(arguments))
  }
  and (conditions) {
    return this._boolean((a, b) => a && b, true, Array.from(arguments))
  }
  except (conditions) {
    return this._boolean((a, b) => a && !b, true, Array.from(arguments))
  }
  retain (conditions) {
    return this.and(merge(this, conditions))
  }
  _boolean (operator, initial, conditions) {
    return this.next(async nodes =>
      nodes.filter(node => {
        var result = initial
        for (var i = 0; i < conditions.length; i++) {
          result = operator(result, conditions[i](node))
        }
        return result
      })
    )
  }
  as (name) {
    const next = this.next(async nodes => {
      const savedNodes = await nodes.toArray()
      next._savedNodes = savedNodes
      return AsyncGenerator.from(savedNodes)
    })
    next.name = name
    return next
  }
  back (name) {
    return this.next(async nodes => {
      // Process all prior nodes before returning upstream
      await nodes.forEach(() => {})
      let current = this
      while (current) {
        if (current.name === name) {
          return AsyncGenerator.from(current._savedNodes)
        }
        current = current.parent
      }
      throw new Error(`No traversal named ${name} found`)
    })
  }
  filter (predicate) {
    return this.next(async nodes => nodes.filter(predicate))
  }
  interval (prop, lower, upper) {
    return this.next(async nodes => nodes.filter(node => {
      const value = node[prop]
      return value >= lower && value <= upper
    }))
  }
  random (bias) {
    return this.next(async nodes => nodes.filter(node => Math.random() < bias))
  }
  map (fields) {
    fields = flatten(Array.from(arguments)).concat(RID, TYPE)
    return this.next(async nodes => nodes.map(n => _.pick(n, fields)))
  }
  toArray () {
    return this.next(async nodes => AsyncGenerator.of(nodes.toArray()))
  }
  first () {
    return this.next(async nodes => nodes.take(1))
  }
  remove () {
    return this.next(async nodes => {
      await nodes.forEach(node => this.graph.remove(node))
      return AsyncGenerator.from([])
    })
  }
  then(onResolve, onReject) {
    return this.nodes.toArray()
      .then(result => {
        if (result.length > 1) {
          throw new Error(`Expected one result, got ${result.length}. Try using .first() or .toArray().`)
        } else if (result.length === 0) {
          return null
        } else {
          return result[0]
        }
      })
      .then(onResolve, onReject)
  }
}

exports.RID = RID
exports.TYPE = TYPE
exports.VERTEX = VERTEX
exports.EDGE = EDGE
exports.CLASS = CLASS
exports.REF = REF
exports.UPSERT = UPSERT
exports.isTraversal = isTraversal
exports.assertIsTraversal = assertIsTraversal
exports.isVertex = isVertex
exports.assertIsVertex = assertIsVertex
exports.isEdge = isEdge
exports.assertIsEdge = assertIsEdge
exports.isLabel = isLabel
exports.assertIsLabel = assertIsLabel
exports.isNode = isNode
exports.assertIsNode = assertIsNode
exports.merge = merge
exports.outKey = outKey
exports.inKey = inKey
exports.identity = identity
exports.truthy = truthy
exports.Graph = Graph
exports.Traversal = Traversal
