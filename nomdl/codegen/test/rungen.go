package test

//go:generate go run ../codegen.go --package=test --in=gen/enum_struct.noms --out=enum_struct.go
//go:generate go run ../codegen.go --package=test --in=gen/list_int64.noms --out=list_int64.go
//go:generate go run ../codegen.go --package=test --in=gen/map.noms --out=map.go
//go:generate go run ../codegen.go --package=test --in=gen/set.noms --out=set.go
//go:generate go run ../codegen.go --package=test --in=gen/struct_primitives.noms --out=struct_primitives.go
//go:generate go run ../codegen.go --package=test --in=gen/struct_with_list.noms --out=struct_with_list.go
//go:generate go run ../codegen.go --package=test --in=gen/struct_with_union_field.noms --out=struct_with_union_field.go
//go:generate go run ../codegen.go --package=test --in=gen/struct_with_unions.noms --out=struct_with_unions.go
//go:generate go run ../codegen.go --package=test --in=gen/struct.noms --out=struct.go
