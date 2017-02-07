# Noms GraphQL

An experimental bridge between noms and [GraphQL](http://graphql.org/)

# Status

 * All Noms types are supported except
   * Blob
   * Type
   * Unions with non-`Struct` component types

 * Noms collections (`List`, `Set`, `Map`) are expressed as graphql Structs with a list-valued `elements` field.
   * Lists support argumemts `at` and `count` to narrow the range of returned elements
   * Sets and Map support argument `count` which results in the first `count` values being returned
   * `Map<K,V>` is expressed as a list of "entry-struct", e.g.
   * `Ref<T>` is expressed as a graphql struct with a `targetHash` and `targetValue` field.

List:
```
type FooList {
  size: Float!
  elements: [Foo!]!
}
```

Set:
```
type FooSet {
  size: Float!
  elements: [Foo!]!
}
```

Map:
```
type StringFooMap {
  size: Float!
  elements: [StringFooEntry!]!
}

type StringFloatEntry {
  key: String!
  value: Float!
}
```

Ref:
```
type FooRef {
  targetHash: String!
  targetValue: Foo!
}
```

 * Mutations not yet supported
 * Higher-level operations (such as set-intersection/union) not yet supported.
 * Perf has not been evaluated or addressed and is probably unimpresssive.
