# Noms GraphQL

An experimental bridge between Noms and [GraphQL](http://graphql.org/)

This is to be used with https://github.com/attic-labs/graphql which is a fork of https://github.com/graphql-go/graphql to handle Noms values. It disables some validations that do not work for Noms due to Noms being schemaless (or more precisely  the schema is a function of the value in the dataset).

*ngql* provides an API to convert Noms types/values to and from GraphQL types/values, as well as some functions that can be used to implement a GraphQL endpoint using https://github.com/attic-labs/graphql.

# Status

 * All Noms types are supported except
   * Blob
   * Type
   * Unions with non-`Struct` component types
   * GraphQL does not support unions in input types which limits the input types that can be used.

# Type conversion rules

## Value

Allmost all Noms values can be represented by GraphQL. All Noms values except the primitives (`Bool`, `Number` & `String`) are represented by a GraphQL struct.

## Bool

Is represented by a non nullable `Bool`

## Number

Is represented by a non nullable `Float`

## String

Is represented by a non nullable `String`

## List

Noms lists are expressed as a GraphQL struct with the fields

* `values` - The values in the list.
* `size` - The number of values in the list.

Lists takes a few optional arguments:

* `at` - The index to start at, defaults to `0`.
* `count` - The number of values to return, defaults to all of the values.

```graphql
type FooList {
  size: Float!
  values: [Foo!]!
}
```

## Set

Noms sets are expressed as a GraphQL struct with the fields

* `values` - The values in the set.
* `size` - The number of values in the set.

Sets takes a few optional arguments:

* `at` - The index to start at, defaults to `0`.
* `count` - The number of values to return, defaults to all of the values.
* `key` - The value to start at.
* `through` - The value to end at (inclusive).
* `keys` - When provided only values that matches the keys are included in the result.

```graphql
type FooSet {
  size: Float!
  values: [Foo!]!
}
```

## Map

Noms maps are expressed as a GraphQL struct with the fields

* `values` - The values in the map.
* `keys` - The keys in the map.
* `entries` - The entries in the map. An entry is a struct with `key` and `value` fields.
* `size` - The number of values in the map.

Sets takes a few optional arguments:

* `at` - The index to start at, defaults to `0`
* `count` - The number of elements to return, defaults to all of the elements.
* `key` - The value to start at
* `through` - The value to end at (inclusive)
* `keys` - When provided only values/keys/entries that matches the keys are included in the result.

```graphql
type StringFooMap {
  size: Float!
  elements: [StringFooEntry!]!
}

type StringFloatEntry {
  key: String!
  value: Float!
}
```

## Struct

Noms structs are expressed as GraphQL structs, with an extra `hash` field.

If the field in the Noms struct is optional then the GraphQL type for that field is nullable.

## Ref

Noms refs are expressed as a GraphQL struct with a `targetHash` and `targetValue` field.

```graphql
type FooRef {
  targetHash: String!
  targetValue: Foo!
}
```
