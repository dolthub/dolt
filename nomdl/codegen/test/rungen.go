package test

//go:generate go run ../codegen.go -deps-dir=gen -ldb=/tmp/depGenTest -package-ds=testDeps -in=../testDeps/leafDep/leafDep.noms -out=../testDeps/leafDep/leafDep.go

//go:generate go run ../codegen.go -deps-dir=gen -ldb=/tmp/depGenTest -package-ds=testDeps -in=../testDeps/dep.noms -out=../testDeps/dep.go

//go:generate go run ../codegen.go -deps-dir=gen -ldb=/tmp/depGenTest -package-ds=testDeps

//go:generate rm -rf /tmp/depGenTest
