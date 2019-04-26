// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package splore

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/verbose"
	"github.com/stretchr/testify/assert"
)

func TestNomsSplore(t *testing.T) {
	assert := assert.New(t)

	dir, err := ioutil.TempDir("", "TestNomsSplore")
	d.PanicIfError(err)
	defer os.RemoveAll(dir)

	quiet := verbose.Quiet()
	defer verbose.SetQuiet(quiet)
	verbose.SetQuiet(true)

	getNode := func(id string) string {
		lchan := make(chan net.Listener)
		httpServe = func(l net.Listener, h http.Handler) error {
			lchan <- l
			http.Serve(l, h) // this will error because of the l.Close() below
			return nil
		}

		go func() { run(context.Background(), &http.ServeMux{}, 0, false, "nbs:"+dir) }()
		l := <-lchan
		defer l.Close()

		r, err := http.Get(fmt.Sprintf("http://%s/getNode?id=%s", l.Addr().String(), id))
		assert.NoError(err)
		defer r.Body.Close()
		body, err := ioutil.ReadAll(r.Body)
		return string(body)
	}

	// No data yet:
	assert.JSONEq(`{
		"children": [],
		"hasChildren": false,
		"id": "",
		"name": "Map(0)"
	}`, getNode(""))

	// Path not found:
	assert.JSONEq(`{"error": "not found"}`, getNode(".notfound"))

	// Test with real data:
	sp, err := spec.ForDataset(fmt.Sprintf("nbs:%s::ds", dir))
	d.PanicIfError(err)
	defer sp.Close()
	db := sp.GetDatabase(context.Background())
	strct := types.NewStruct("StructName", types.StructData{
		"blob":          types.NewBlob(context.Background(), db),
		"bool":          types.Bool(true),
		"list":          types.NewList(context.Background(), db, types.Float(1), types.Float(2)),
		"map":           types.NewMap(context.Background(), db, types.String("a"), types.String("b"), types.String("c"), types.String("d")),
		"number":        types.Float(42),
		"ref":           db.WriteValue(context.Background(), types.Bool(true)),
		"set":           types.NewSet(context.Background(), db, types.Float(3), types.Float(4)),
		"string":        types.String("hello world"),
		"typeCompound":  types.MakeMapType(types.StringType, types.MakeListType(types.BoolType)),
		"typePrimitive": types.FloaTType,
		"typeStruct":    types.MakeStructType("StructType", types.StructField{Name: "x", Type: types.StringType}, types.StructField{Name: "y", Type: types.MakeStructType("")}),
	})
	sp.GetDatabase(context.Background()).CommitValue(context.Background(), sp.GetDataset(context.Background()), strct)

	// The dataset head hash changes whenever the test data changes, so instead
	// of updating it all the time, use string replacement.
	dsHash := sp.GetDataset(context.Background()).HeadRef().TargetHash().String()
	test := func(expectJSON string, id string) {
		expectJSON = strings.Replace(expectJSON, "{{dsHash}}", dsHash, -1)
		assert.JSONEq(expectJSON, getNode(id))
	}

	// Root => datasets:
	test(`{
		"children": [
		{
			"key": {
				"hasChildren": false,
				"id": "@at(0)@key",
				"name": "\"ds\""
			},
			"label": "",
			"value": {
				"hasChildren": true,
				"id": "@at(0)",
				"name": "Value#{{dsHash}}"
			}
		}
		],
		"hasChildren": true,
		"id": "",
		"name": "Map(1)"
	}`, "")

	// Dataset 0 (ds) => dataset head ref.
	test(`{
		"children": [
		{
			"key": {
				"hasChildren": false,
				"id": "",
				"name": ""
			},
			"label": "",
			"value": {
				"hasChildren": true,
				"id": "@at(0)@target",
				"name": "Value#{{dsHash}}"
			}
		}
		],
		"hasChildren": true,
		"id": "@at(0)",
		"name": "Value#{{dsHash}}"
	}`, "@at(0)")

	// ds head ref => ds head:
	// There is a %s replacement here for the ID root, because this expectation
	// is used both for fetching via @at(0)@target and as an absolute ref.
	expectDSHeadFmt := `{
		"children": [
		{
			"key": {
				"hasChildren": false,
				"id": "",
				"name": ""
			},
			"label": "meta",
			"value": {
				"hasChildren": false,
				"id": "%[1]s.meta",
				"name": "{}"
			}
		},
		{
			"key": {
				"hasChildren": false,
				"id": "",
				"name": ""
			},
			"label": "parents",
			"value": {
				"hasChildren": false,
				"id": "%[1]s.parents",
				"name": "Set(0)"
			}
		},
		{
			"key": {
				"hasChildren": false,
				"id": "",
				"name": ""
			},
			"label": "value",
			"value": {
				"hasChildren": true,
				"id": "%[1]s.value",
				"name": "StructName"
			}
		}
		],
		"hasChildren": true,
		"id": "%[1]s",
		"name": "Commit"
	}`

	test(fmt.Sprintf(expectDSHeadFmt, "@at(0)@target"), "@at(0)@target")
	test(fmt.Sprintf(expectDSHeadFmt, "#"+dsHash), "%23"+dsHash)

	// ds head value => strct.
	test(`{
		"children": [
		{
			"key": {
				"hasChildren": false,
				"id": "",
				"name": ""
			},
			"label": "blob",
			"value": {
				"hasChildren": false,
				"id": "@at(0)@target.value.blob",
				"name": "Blob(0 B)"
			}
		},
		{
			"key": {
				"hasChildren": false,
				"id": "",
				"name": ""
			},
			"label": "bool",
			"value": {
				"hasChildren": false,
				"id": "@at(0)@target.value.bool",
				"name": "true"
			}
		},
		{
			"key": {
				"hasChildren": false,
				"id": "",
				"name": ""
			},
			"label": "list",
			"value": {
				"hasChildren": true,
				"id": "@at(0)@target.value.list",
				"name": "List(2)"
			}
		},
		{
			"key": {
				"hasChildren": false,
				"id": "",
				"name": ""
			},
			"label": "map",
			"value": {
				"hasChildren": true,
				"id": "@at(0)@target.value.map",
				"name": "Map(2)"
			}
		},
		{
			"key": {
				"hasChildren": false,
				"id": "",
				"name": ""
			},
			"label": "number",
			"value": {
				"hasChildren": false,
				"id": "@at(0)@target.value.number",
				"name": "42"
			}
		},
		{
			"key": {
				"hasChildren": false,
				"id": "",
				"name": ""
			},
			"label": "ref",
			"value": {
				"hasChildren": true,
				"id": "@at(0)@target.value.ref",
				"name": "Bool#g19moobgrm32dn083bokhksuobulq28c"
			}
		},
		{
			"key": {
				"hasChildren": false,
				"id": "",
				"name": ""
			},
			"label": "set",
			"value": {
				"hasChildren": true,
				"id": "@at(0)@target.value.set",
				"name": "Set(2)"
			}
		},
		{
			"key": {
				"hasChildren": false,
				"id": "",
				"name": ""
			},
			"label": "string",
			"value": {
				"hasChildren": false,
				"id": "@at(0)@target.value.string",
				"name": "\"hello world\""
			}
		},
		{
			"key": {
				"hasChildren": false,
				"id": "",
				"name": ""
			},
			"label": "typeCompound",
			"value": {
				"hasChildren": true,
				"id": "@at(0)@target.value.typeCompound",
				"name": "Map"
			}
		},
		{
			"key": {
				"hasChildren": false,
				"id": "",
				"name": ""
			},
			"label": "typePrimitive",
			"value": {
				"hasChildren": false,
				"id": "@at(0)@target.value.typePrimitive",
				"name": "Float"
			}
		},
		{
			"key": {
				"hasChildren": false,
				"id": "",
				"name": ""
			},
			"label": "typeStruct",
			"value": {
				"hasChildren": true,
				"id": "@at(0)@target.value.typeStruct",
				"name": "struct StructType"
			}
		}
		],
		"hasChildren": true,
		"id": "@at(0)@target.value",
		"name": "StructName"
	}`, "@at(0)@target.value")

	// strct.blob:
	test(`{
		"children": [],
		"hasChildren": false,
		"id": "@at(0)@target.value.blob",
		"name": "Blob(0 B)"
	}`, "@at(0)@target.value.blob")

	// strct.bool:
	test(`{
		"children": [],
		"hasChildren": false,
		"id": "@at(0)@target.value.bool",
		"name": "true"
	}`, "@at(0)@target.value.bool")

	// strct.list:
	test(`{
		"children": [
		{
			"key": {
				"hasChildren": false,
				"id": "",
				"name": ""
			},
			"label": "",
			"value": {
				"hasChildren": false,
				"id": "@at(0)@target.value.list[0]",
				"name": "1"
			}
		},
		{
			"key": {
				"hasChildren": false,
				"id": "",
				"name": ""
			},
			"label": "",
			"value": {
				"hasChildren": false,
				"id": "@at(0)@target.value.list[1]",
				"name": "2"
			}
		}
		],
		"hasChildren": true,
		"id": "@at(0)@target.value.list",
		"name": "List(2)"
	}`, "@at(0)@target.value.list")

	// strct.map:
	test(`{
		"children": [
		{
			"key": {
				"hasChildren": false,
				"id": "@at(0)@target.value.map@at(0)@key",
				"name": "\"a\""
			},
			"label": "",
			"value": {
				"hasChildren": false,
				"id": "@at(0)@target.value.map@at(0)",
				"name": "\"b\""
			}
		},
		{
			"key": {
				"hasChildren": false,
				"id": "@at(0)@target.value.map@at(1)@key",
				"name": "\"c\""
			},
			"label": "",
			"value": {
				"hasChildren": false,
				"id": "@at(0)@target.value.map@at(1)",
				"name": "\"d\""
			}
		}
		],
		"hasChildren": true,
		"id": "@at(0)@target.value.map",
		"name": "Map(2)"
	}`, "@at(0)@target.value.map")

	// strct.number:
	test(`{
		"children": [],
		"hasChildren": false,
		"id": "@at(0)@target.value.number",
		"name": "42"
	}`, "@at(0)@target.value.number")

	// strct.ref:
	test(`{
		"children": [
		{
			"key": {
				"hasChildren": false,
				"id": "",
				"name": ""
			},
			"label": "",
			"value": {
				"hasChildren": true,
				"id": "@at(0)@target.value.ref@target",
				"name": "Bool#g19moobgrm32dn083bokhksuobulq28c"
			}
		}
		],
		"hasChildren": true,
		"id": "@at(0)@target.value.ref",
		"name": "Bool#g19moobgrm32dn083bokhksuobulq28c"
	}`, "@at(0)@target.value.ref")

	// strct.set:
	test(`{
		"children": [
		{
			"key": {
				"hasChildren": false,
				"id": "",
				"name": ""
			},
			"label": "",
			"value": {
				"hasChildren": false,
				"id": "@at(0)@target.value.set@at(0)",
				"name": "3"
			}
		},
		{
			"key": {
				"hasChildren": false,
				"id": "",
				"name": ""
			},
			"label": "",
			"value": {
				"hasChildren": false,
				"id": "@at(0)@target.value.set@at(1)",
				"name": "4"
			}
		}
		],
		"hasChildren": true,
		"id": "@at(0)@target.value.set",
		"name": "Set(2)"
	}`, "@at(0)@target.value.set")

	// strct.string:
	test(`{
		"children": [],
		"hasChildren": false,
		"id": "@at(0)@target.value.string",
		"name": "\"hello world\""
	}`, "@at(0)@target.value.string")

	// strct.typeCompound:
	test(`{
		"children": [
		{
			"key": {
				"hasChildren": false,
				"id": "",
				"name": ""
			},
			"label": "",
			"value": {
				"hasChildren": false,
				"id": "@at(0)@target.value.typeCompound[0]",
				"name": "String"
			}
		},
		{
			"key": {
				"hasChildren": false,
				"id": "",
				"name": ""
			},
			"label": "",
			"value": {
				"hasChildren": true,
				"id": "@at(0)@target.value.typeCompound[1]",
				"name": "List"
			}
		}
		],
		"hasChildren": true,
		"id": "@at(0)@target.value.typeCompound",
		"name": "Map"
	}`, "@at(0)@target.value.typeCompound")

	// strct.typePrimitive:
	test(`{
		"children": [],
		"hasChildren": false,
		"id": "@at(0)@target.value.typePrimitive",
		"name": "Float"
	}`, "@at(0)@target.value.typePrimitive")

	// strct.typeStruct:
	test(`{
		"children": [
		{
			"key": {
				"hasChildren": false,
				"id": "",
				"name": ""
			},
			"label": "x",
			"value": {
				"hasChildren": false,
				"id": "@at(0)@target.value.typeStruct.x",
				"name": "String"
			}
		},
		{
			"key": {
				"hasChildren": false,
				"id": "",
				"name": ""
			},
			"label": "y",
			"value": {
				"hasChildren": false,
				"id": "@at(0)@target.value.typeStruct.y",
				"name": "struct {}"
			}
		}
		],
		"hasChildren": true,
		"id": "@at(0)@target.value.typeStruct",
		"name": "struct StructType"
	}`, "@at(0)@target.value.typeStruct")
}

func TestNomsSploreGetMetaChildren(t *testing.T) {
	assert := assert.New(t)

	storage := &chunks.TestStorage{}
	db := datas.NewDatabase(storage.NewView())
	defer db.Close()

	// A bunch of lists with just numbers or ref<number>s in them. None of these
	// should be detected as meta sequences:

	l1 := types.NewList(context.Background(), db)
	assert.Nil(getMetaChildren(l1))

	l2 := types.NewList(context.Background(), db, types.Float(1))
	assert.Nil(getMetaChildren(l2))

	l3 := types.NewList(context.Background(), db, types.Float(1), types.Float(2))
	assert.Nil(getMetaChildren(l3))

	l4 := types.NewList(context.Background(), db, db.WriteValue(context.Background(), types.Float(1)))
	assert.Nil(getMetaChildren(l4))

	l5 := types.NewList(context.Background(), db, db.WriteValue(context.Background(), types.Float(1)), types.Float(2))
	assert.Nil(getMetaChildren(l5))

	l6 := types.NewList(context.Background(), db, db.WriteValue(context.Background(), types.Float(1)), db.WriteValue(context.Background(), types.Float(2)))
	assert.Nil(getMetaChildren(l6))

	l7 := types.NewList(context.Background(), db, l1)
	assert.Nil(getMetaChildren(l7))

	l8 := types.NewList(context.Background(), db, l4)
	assert.Nil(getMetaChildren(l8))

	// List with more or equal ref<list> than elements. This can't possibly be a meta
	// sequence, because there are no empty leaf sequences:

	l1Ref := db.WriteValue(context.Background(), l1)
	l2Ref := db.WriteValue(context.Background(), l2)
	l3Ref := db.WriteValue(context.Background(), l3)
	listRefList := types.NewList(context.Background(), db, l1Ref, l2Ref, l3Ref)

	l9 := types.NewList(context.Background(), db, listRefList)
	assert.Nil(getMetaChildren(l9))

	l10 := types.NewList(context.Background(), db, types.Float(1), listRefList)
	assert.Nil(getMetaChildren(l10))

	l11 := listRefList
	assert.Nil(getMetaChildren(l11))

	l12 := types.NewList(context.Background(), db, types.Float(1), types.Float(2), listRefList)
	assert.Nil(getMetaChildren(l12))

	l13 := types.NewList(context.Background(), db, types.Float(1), db.WriteValue(context.Background(), types.Float(2)), listRefList)
	assert.Nil(getMetaChildren(l13))

	// List with fewer ref<list> as children. For now this is the closet
	// approximation for detecting meta sequences:

	l1Hash := "#" + l1Ref.TargetHash().String()
	l2Hash := "#" + l2Ref.TargetHash().String()
	l3Hash := "#" + l3Ref.TargetHash().String()
	expectNodeChildren := []nodeChild{
		{Value: nodeInfo{HasChildren: true, ID: l1Hash, Name: "List" + l1Hash}},
		{Value: nodeInfo{HasChildren: true, ID: l2Hash, Name: "List" + l2Hash}},
		{Value: nodeInfo{HasChildren: true, ID: l3Hash, Name: "List" + l3Hash}},
	}

	l14 := types.NewList(context.Background(), db, types.Float(1), types.Float(2), types.Float(3), listRefList)
	assert.Equal(expectNodeChildren, getMetaChildren(l14))

	l15 := types.NewList(context.Background(), db, types.Float(1), types.Float(2), db.WriteValue(context.Background(), types.Float(3)), listRefList)
	assert.Equal(expectNodeChildren, getMetaChildren(l15))

	l16 := types.NewList(context.Background(), db, types.Float(1), types.Float(2), types.Float(3), types.Float(4), listRefList)
	assert.Equal(expectNodeChildren, getMetaChildren(l16))

	l17 := types.NewList(context.Background(), db, types.Float(1), types.Float(2), db.WriteValue(context.Background(), types.Float(3)), db.WriteValue(context.Background(), types.Float(4)), listRefList)
	assert.Equal(expectNodeChildren, getMetaChildren(l17))
}
