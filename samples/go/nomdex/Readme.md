# Nomdex 

Nomdex demonstrates how Noms maps can be used to index values in a database and provides a simple query language to search for objects.

## Description
This program experiments with using ordinary Noms Maps as indexes. It leverages the fact that Maps in Noms are implemented by prolly-trees which are similar to B-Trees in many important ways that make them ideal for use as indexes. They are balanced, sorted, require relatively few accesses to find any leaf node and efficient to update.

###Building Indexes
Nomdex constructs indexes as Maps that are keyed by either Strings or Numbers. The values in the index are sets of objects. The following command can be used to build an index:
```
nomdex up --in-path <absolute noms path> --by <relative noms path> --out-ds <dataset name>
```
The ***'in-path'*** argument must be a ValueSpec(see [Spelling In Noms](../../../doc/spelling.md#spelling-values)) that designates the root of an object hierarchy to be scanned for "indexable" objects.

The ***'by'*** argument must be a relative path. Nomdex traverses every value reachable from 'in-path' and attempts to resolve this relative ***'by'*** path from it. Any value that has a String, Number, or Bool  index using the relative attribute as it's key.

The ***'out-ds'*** argument specifies a dataset name that will be used to store the new index.

In addition, there are arguments that allow values to be transformed before using them as keys in the index by applying regex expressions functions. Consult to the help text and code to see how those can be used.

### Queries in Nomdex
Once an index is built, it can be queried against using the nomdex find command. For example, given a database that contains structs of the following type representing cities:
```
struct Row {
  City: String,
  State: String,
  GeoPos: struct {
    Latitude: Number,
    Longitude: Number,
  }
}
```
The following commands could be used to build indexes on the City, State, Latitude and Longitude attibutes.
```
nomdex up --in-path http://localhost:8000::cities --by .City --out-ds by-name
nomdex up --in-path http://localhost:8000::cities --by .State --out-ds by-state
nomdex up --in-path http://localhost:8000::cities --by .GeoPos.Latitude --out-ds by-lat
nomdex up --in-path http://localhost:8000::cities --by .GeoPos.Longitude --out-ds by-lon
```
Once these indexes are created, the following queries could be made using the find command:
```
// find all cities in California
nomdex find --db http://localhost:8000 'by-state = "California"'

// find all cities whose name begins with A, B, or C
nomdex find --db http://localhost:8000 'by-name >= "A" and by-name < "D"'

// Find all tropical cities whose name begins with A, B, or C
nomdex find --db http://localhost:8000 '(by-name >= "A" and by-name < "D") and (by-lat >= -23.5 and by-lat <= 23.5)
```
The nomdex query language is simple, it consists of comparison expressions which take the form of '*indexName comparisonOperator constantValue*'. Index names are the dataset given as the ***'out-ds'*** argument to the *build* command. Comparison operators can be one of: <, <=, >, >=, =, !=. Constants are either String values which are quoted: "hi, I'm a string constant", and Numbers which consist of digits and an optional decimal point and minus sign: 1, -1, 2.3, -3.2.

In addition, comparison expressions can be combined using "and" and "or". Parenthesis can, and should be used to express the order that evaluation should take place.

Note: nomdex is not a complete query system. It's purpose is only to illustrate the fact that Noms maps have all the necessary properties to be used as indexes. A complete query system would have many additional features and the ability to optimize queries in an intelligent way.
