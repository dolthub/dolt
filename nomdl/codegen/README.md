To update the codegen there are some subtleties because the code depends on generated code.

## Build a working version

First step is to build a binary.

```
cd nomdl/codegen/
go build
```

## Change templates

Not much to say here but you can see the result without breaking things

```
./codegen --in=test/struct.noms
```

This generates `test.noms.go` in the current directory. Iterate until it looks correct.

## Change system go files

There are a few files that are generated that codegen itself depends on.

1. `types/compound_blob_struct.noms.go`
1. `datas/types.noms.go`

Both of these can be updated by running `go generate` in their respective directories

There is also one more file that is generated but it requires manual intervention

### `types/package_set_of_ref.go`

This one is generated from `types/package_set_of_ref.noms`. However, it uses the symbol
`Package` to refer to a `types.Package`. Currently we have no convenient way to make this work
out of the box. However, it is pretty straight forward to make it work.

1. Open `nomdl/pkg/grammar.pg`
2. Find `UInt64`
3. At that line, add one more builtin type called `Package`.
4. Run `go generate` `in nomdl/pkg`
5. Run `go run ../nomdl/codegen/codegen.go --in=package_set_of_ref.noms` in `types/`.

Here is the diff:

```diff
--- a/nomdl/pkg/grammar.peg
+++ b/nomdl/pkg/grammar.peg
@@ -159,7 +159,7 @@ CompoundType <- `List` _ `(` _ t:Type _ `)` _ {
        return types.MakeCompoundTypeRef(types.RefKind, t.(types.Type)), nil
 }

-PrimitiveType <- p:(`UInt64` / `UInt32` / `UInt16` / `UInt8` / `Int64` / `Int32` / `Int16` / `Int8` / `Float64` / `Float32` / `Bool` / `String` / `Blob` / `Value` / `Type`) {
+PrimitiveType <- p:(`UInt64` / `UInt32` / `UInt16` / `UInt8` / `Int64` / `Int32` / `Int16` / `Int8` / `Float64` / `Float32` / `Bool` / `String` / `Blob` / `Value` / `Type` / `Package`) {
        return types.MakePrimitiveTypeRefByString(string(p.([]uint8))), nil
 }
 ```
 
 Once [#577](https://github.com/attic-labs/noms/issues/577) is fixed this will need no manual intervention.

