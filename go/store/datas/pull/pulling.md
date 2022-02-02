# Dataset pulling algorithm
The approach is to explore the chunk graph of both sink and source in order of decreasing ref-height. As the code walks, it uses the knowledge gained about which chunks are present in the sink to both prune the source-graph-walk and build up a set of `hints` that can be sent to a remote Database to aid in chunk validation.

## Basic algorithm

- let `sink` be the *sink* database
- let `source` be the *source* database
- let `snkQ` and `srcQ` be priority queues of `Ref` prioritized by highest `Ref.height`
- let `hints` be a map of `hash => hash`
- let `reachableChunks` be a set of hashes
- let `snkHdRef` be the ref (of `Commit`) of the head of the *sink* dataset
- let `srcHdRef` be the ref of the *source* `Commit`, which must descend from the `Commit` indicated by `snkHdRef`

- let `traverseSource(srcRef, srcQ, sink, source, reachableChunks)` be
  - pop `srcRef` from `srcQ`
    - if `!sink.has(srcRef)`
      - let `c` = `source.batchStore().Get(srcRef.targetHash)`
      - let `v` = `types.DecodeValue(c, source)`
      - insert all child refs, `cr`, from `v` into `srcQ` and into reachableRefs
      - `sink.batchStore().Put(c, srcRef.height, no hints)`
        - (hints will all be gathered and handed to sink.batchStore at the end)


- let `traverseSink(sinkRef, snkQ, sink, hints)` be
  - pop `snkRef` from `snkQ`
  - if `snkRef.height` > 1
    - let `v` = `sink.readValue(snkRef.targetHash)`
    - insert all child refs, `cr`, from `v` into `snkQ` and `hints[cr] = snkRef`


- let `traverseCommon(comRef, snkHdRef, snkQ, srcQ, sink, hints)` be
  - pop `comRef` from both `snkQ` and `srcQ`
  - if `comRef.height` > 1
    - if `comRef` is a `Ref` of `Commit`
      - let `v` = `sink.readValue(comRef.targetHash)`
      - if `comRef` == snkHdRef
        - *ignore all parent refs*
        - insert each other child ref `cr` from `v` into `snkQ` *only*, set `hints[cr] = comRef`
      - else
        - insert each child ref `cr` from `v` into both `snkQ` and `srcQ`, set `hints[cr] = comRef`


- let `pull(source, sink, srcHdRef, sinkHdRef)
  - insert `snkHdRef` into `snkQ` and `srcHdRef` into `srcQ`
  - create empty `hints` and `reachableChunks`
  - while `srcQ` is non-empty
    - let `srcHt` and `snkHt` be the respective heights of the *top* `Ref` in each of `srcQ` and `snkQ`
    - if `srcHt` > `snkHt`, for every `srcHdRef` in `srcQ` which is of greater height than `snkHt`
      - `traverseSource(srcHdRef, srcQ, sink, source)`
    - else if `snkHt` > `srcHt`, for every `snkHdRef` in `snkQ` which is of greater height than `srcHt`
      - `traverseSink(snkHdRef, snkQ, sink)`
    - else
      - for every `comRef` in which is common to `snkQ` and `srcQ` which is of height `srcHt` (and `snkHt`)
        - `traverseCommon(comRef, snkHdRef, snkQ, srcQ, sink, hints)`
      - for every `ref` in `srcQ` which is of height `srcHt`
        - `traverseSource(ref, srcQ, sink, source, reachableChunks)`
      - for every `ref` in `snkQ` which is of height `snkHt`
        - `traverseSink(ref, snkQ, sink, hints)`
  - for all `hash` in `reachableChunks`
    - sink.batchStore().addHint(hints[hash])


## Isomorphic, but less clear, algorithm

- let all identifiers be as above
- let `traverseSource`, `traverseSink`, and `traverseCommon` be as above

- let `higherThan(refA, refB)` be
  - if refA.height == refB.height
    - return refA.targetHash < refB.targetHash
  - return refA.height > refB.height

- let `pull(source, sink, srcHdRef, sinkHdRef)
  - insert `snkHdRef` into `snkQ` and `srcHdRef` into `srcQ`
  - create empty `hints` and `reachableChunks`
  - while `srcQ` is non-empty
    - if `sinkQ` is empty
      - pop `ref` from `srcQ`
      - `traverseSource(ref, srcQ, sink, source, reachableChunks))
    - else if `higherThan(head of srcQ, head of snkQ)`
      - pop `ref` from `srcQ`
      - `traverseSource(ref, srcQ, sink, source, reachableChunks))
    - else if `higherThan(head of snkQ, head of srcQ)`
      - pop `ref` from `snkQ`
      - `traverseSink(ref, snkQ, sink, hints)`
    - else, heads of both queues are the same
      - pop `comRef` from `snkQ` and `srcQ`
      - `traverseCommon(comRef, snkHdRef, snkQ, srcQ, sink, hints)`
  - for all `hash` in `reachableChunks`
    - sink.batchStore().addHint(hints[hash])


