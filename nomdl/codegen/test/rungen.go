package test

//go:generate rm -rf /tmp/depGenTest

//go:generate go run ../codegen.go -ldb=/tmp/depGenTest -package-ds=testDeps -in=../testDeps/leafDep/leafDep.noms -out-dir=../testDeps/leafDep
//go:generate go run ../codegen.go -ldb=/tmp/depGenTest -package-ds=testDeps -in=../testDeps/leafDep/leafDep.noms -out-dir=../testDeps/leafDep -out-lang=js

//go:generate go run ../codegen.go -out-dir=gen -ldb=/tmp/depGenTest -package-ds=testDeps
//go:generate go run ../codegen.go -out-dir=gen -ldb=/tmp/depGenTest -package-ds=testDeps -out-lang=js

//go:generate rm -rf /tmp/depGenTest
