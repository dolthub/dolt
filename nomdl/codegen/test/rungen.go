package test

//go:generate rm -rf /tmp/depGenTest

//go:generate go run ../codegen.go -ldb=/tmp/depGenTest -package-ds=testDeps -in=../testDeps/leafDep/leafDep.noms -out=../testDeps/leafDep/leafDep.go

//go:generate go run ../codegen.go -out-dir=gen -ldb=/tmp/depGenTest -package-ds=testDeps

//go:generate rm -rf /tmp/depGenTest
