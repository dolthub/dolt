// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package splore

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"reflect"

	"github.com/attic-labs/noms/cmd/util"
	"github.com/attic-labs/noms/go/config"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/verbose"
	humanize "github.com/dustin/go-humanize"
	flag "github.com/juju/gnuflag"
	"github.com/skratchdot/open-golang/open"
)

var (
	NomsSplore = &util.Command{
		Run:       run,
		Flags:     setupFlags,
		Nargs:     1,
		UsageLine: "splore",
	}

	browse    = false
	httpServe = http.Serve
	mux       = &http.ServeMux{}
	port      = 0
)

const indexHtml = `<!DOCTYPE html>
<html>
	<head>
		<title>noms splore %s</title>
		<style>
		body { margin: 0; user-select: none; }
		</style>
	</head>
	<body>
		<div id="splore"></div>
		<script src="/out.js"></script>
	</body>
</html>`

func setupFlags() *flag.FlagSet {
	flagSet := flag.NewFlagSet("splore", flag.ExitOnError)
	flagSet.BoolVar(&browse, "b", false, "Immediately open a web browser.")
	flagSet.IntVar(&port, "p", 0, "Server port. Defaults to a random port.")
	verbose.RegisterVerboseFlags(flagSet)
	return flagSet
}

type node struct {
	nodeInfo
	Children []nodeChild `json:"children"`
}

type nodeInfo struct {
	HasChildren bool   `json:"hasChildren"`
	ID          string `json:"id"`
	Name        string `json:"name"`
}

type nodeChild struct {
	Key   nodeInfo `json:"key"`
	Label string   `json:"label"`
	Value nodeInfo `json:"value"`
}

func run(args []string) int {
	var sp spec.Spec
	var getValue func() types.Value

	cfg := config.NewResolver()
	if pathSp, err := spec.ForPath(cfg.ResolvePathSpec(args[0])); err == nil {
		sp = pathSp
		getValue = func() types.Value { return sp.GetValue() }
	} else if dbSp, err := spec.ForDatabase(cfg.ResolveDbSpec(args[0])); err == nil {
		sp = dbSp
		getValue = func() types.Value { return sp.GetDatabase().Datasets() }
	} else {
		d.CheckError(fmt.Errorf("Not a path or database: %s", args[0]))
	}

	defer sp.Close()

	req := func(w http.ResponseWriter, contentType string) {
		sp.GetDatabase().Rebase()
		w.Header().Add("Content-Type", contentType)
		w.Header().Add("Cache-Control", "max-age=0,no-cache")
	}

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		req(w, "text/html")
		fmt.Fprintf(w, indexHtml, sp.String())
	})

	mux.HandleFunc("/out.js", func(w http.ResponseWriter, r *http.Request) {
		req(w, "application/javascript")
		// To develop JS, uncomment this line and run `yarn start`:
		//http.ServeFile(w, r, "splore/out.js")
		// To build noms-splore. uncomment this line and run `yarn buildgo`:
		fmt.Fprint(w, outJs)
	})

	mux.HandleFunc("/getNode", func(w http.ResponseWriter, r *http.Request) {
		req(w, "application/json")
		r.ParseForm()
		id := r.Form.Get("id")

		var v types.Value
		switch {
		case id == "":
			v = getValue()
		case id[0] == '#':
			abspath, err := spec.NewAbsolutePath(id)
			d.PanicIfError(err)
			v = abspath.Resolve(sp.GetDatabase())
		default:
			path := types.MustParsePath(id)
			v = path.Resolve(getValue(), sp.GetDatabase())
		}

		if v == nil {
			http.Error(w, `{"error": "not found"}`, http.StatusNotFound)
			return
		}

		err := json.NewEncoder(w).Encode(node{
			nodeInfo: info(v, id),
			Children: getNodeChildren(v, id),
		})
		d.PanicIfError(err)
	})

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	d.PanicIfError(err)

	if browse {
		open.Run("http://" + l.Addr().String())
	}

	if !verbose.Quiet() {
		fmt.Println("Listening on", l.Addr().String())
	}

	d.PanicIfError(httpServe(l, mux))
	return 0
}

func getNodeChildren(v types.Value, parentPath string) (children []nodeChild) {
	atPath := func(i int, suffix string) string {
		return fmt.Sprintf("%s@at(%d)%s", parentPath, i, suffix)
	}

	switch v := v.(type) {
	case types.Bool, types.Number, types.String:
		children = []nodeChild{}
	case types.Blob:
		children = getMetaChildren(v)
		if children == nil {
			children = []nodeChild{}
		}
	case types.List:
		children = getMetaChildren(v)
		if children == nil {
			children = make([]nodeChild, v.Len())
			v.IterAll(func(vi types.Value, i uint64) {
				children[i] = nodeChild{
					Value: info(vi, fmt.Sprintf("%s[%d]", parentPath, i)),
				}
			})
		}
	case types.Map:
		children = getMetaChildren(v)
		if children == nil {
			children = make([]nodeChild, v.Len())
			i := 0
			v.IterAll(func(k, v types.Value) {
				children[i] = nodeChild{
					Key:   info(k, atPath(i, "@key")),
					Value: info(v, atPath(i, "")),
				}
				i++
			})
		}
	case types.Set:
		children = getMetaChildren(v)
		if children == nil {
			children = make([]nodeChild, v.Len())
			i := 0
			v.IterAll(func(v types.Value) {
				children[i] = nodeChild{
					Value: info(v, atPath(i, "")),
				}
				i++
			})
		}
	case types.Ref:
		children = []nodeChild{{
			Value: info(v, parentPath+"@target"),
		}}
	case types.Struct:
		children = make([]nodeChild, v.Len())
		i := 0
		v.IterFields(func(name string, v types.Value) {
			children[i] = nodeChild{
				Label: name,
				Value: info(v, fmt.Sprintf("%s.%s", parentPath, name)),
			}
			i++
		})
	default:
		panic(fmt.Errorf("unsupported value type %T", v))
	}
	return
}

func nodeName(v types.Value) string {
	typeName := func(iface interface{}) string {
		return reflect.TypeOf(iface).Name()
	}

	switch v := v.(type) {
	case types.Bool, types.Number, types.String:
		return fmt.Sprintf("%#v", v)
	case types.Blob:
		return fmt.Sprintf("%s(%s)", typeName(v), humanize.Bytes(v.Len()))
	case types.List, types.Map, types.Set:
		return fmt.Sprintf("%s(%d)", typeName(v), v.(types.Collection).Len())
	case types.Ref:
		kind := v.TargetType().Desc.Kind()
		return fmt.Sprintf("%s#%s", kind.String(), v.TargetHash().String())
	case types.Struct:
		if v.Name() == "" {
			return "Struct{…}"
		}
		return v.Name()
	}
	panic("unreachable")
}

// getMetaChildren returns the nodeChild children, as refs, of v if it's backed
// by a meta sequence, or nil if not.
//
// This isn't exposed directly on the API but for now just guess it:
// - If there are no chunks, it must be a leaf.
// - If there are MORE chunks than the length of the blob/collection then it
//   can only be a leaf with multiple ref values per entry.
// - If there are EQUAL then it could be either, but heuristically assume
//   that it's a leaf with a ref value per entry. It's highly unlikely that a
//   blob/collection will chunk with single elements.
// - If there are LESS then it could be either a chunked blob/collection or a
//   collection of mixed types, but heuristically assume that's it's chunked.
func getMetaChildren(v types.Value) (children []nodeChild) {
	var l uint64
	if col, ok := v.(types.Collection); ok {
		l = col.Len()
	} else {
		l = v.(types.Blob).Len()
	}

	vKind := types.TypeOf(v).Desc.Kind()
	v.WalkRefs(func(r types.Ref) {
		if r.TargetType().Desc.Kind() == vKind {
			children = append(children, nodeChild{
				Value: info(r, "#"+r.TargetHash().String()),
			})
		}
	})

	if uint64(len(children)) >= l {
		children = nil
	}
	return
}

func nodeHasChildren(v types.Value) bool {
	switch k := types.TypeOf(v).Desc.Kind(); k {
	case types.BlobKind, types.BoolKind, types.NumberKind, types.StringKind:
		return false
	case types.RefKind:
		return true
	case types.ListKind, types.SetKind, types.MapKind:
		return v.(types.Collection).Len() > 0
	case types.StructKind:
		return v.(types.Struct).Len() > 0
	default:
		panic(fmt.Errorf("unreachable kind %s", k.String()))
	}
}

func info(v types.Value, id string) nodeInfo {
	return nodeInfo{
		HasChildren: nodeHasChildren(v),
		ID:          id,
		Name:        nodeName(v),
	}
}
