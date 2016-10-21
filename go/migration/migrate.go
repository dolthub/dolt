// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package migration

import (
	"fmt"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/types"
	v7types "github.com/attic-labs/noms/go/types"
)

// MigrateFromVersion7 migrates a Noms value of format version 7 to the current version.
func MigrateFromVersion7(source v7types.Value, sourceStore v7types.ValueReadWriter, sinkStore types.ValueReadWriter) (dest types.Value, err error) {
	switch source := source.(type) {
	case v7types.Bool:
		return types.Bool(bool(source)), nil
	case v7types.Number:
		return types.Number(float64(source)), nil
	case v7types.String:
		return types.String(string(source)), nil
	case v7types.Blob:
		return types.NewStreamingBlob(sourceStore, source.Reader()), nil
	case v7types.List:
		vc := make(chan types.Value, 1024)
		lc := types.NewStreamingList(sinkStore, vc)
		for i := uint64(0); i < source.Len(); i++ {
			var nv types.Value
			nv, err = MigrateFromVersion7(source.Get(i), sourceStore, sinkStore)
			if err != nil {
				break
			}
			vc <- nv
		}
		close(vc)
		dest = <-lc
		return
	case v7types.Map:
		kvc := make(chan types.Value, 1024)
		mc := types.NewStreamingMap(sinkStore, kvc)
		source.Iter(func(k, v v7types.Value) (stop bool) {
			var nk, nv types.Value
			nk, err = MigrateFromVersion7(k, sourceStore, sinkStore)
			if err == nil {
				nv, err = MigrateFromVersion7(v, sourceStore, sinkStore)
			}
			if err != nil {
				stop = true
			} else {
				kvc <- nk
				kvc <- nv
			}
			return
		})
		close(kvc)
		dest = <-mc
		return
	case v7types.Set:
		vc := make(chan types.Value, 1024)
		sc := types.NewStreamingSet(sinkStore, vc)
		source.Iter(func(v v7types.Value) (stop bool) {
			var nv types.Value
			nv, err = MigrateFromVersion7(v, sourceStore, sinkStore)
			if err != nil {
				stop = true
			} else {
				vc <- nv
			}
			return
		})
		close(vc)
		dest = <-sc
		return
	case v7types.Struct:
		t := migrateType(source.Type())
		sd := source.Type().Desc.(v7types.StructDesc)
		fields := make([]types.Value, 0, sd.Len())
		sd.IterFields(func(name string, _ *v7types.Type) {
			if err == nil {
				var fv types.Value
				fv, err = MigrateFromVersion7(source.Get(name), sourceStore, sinkStore)
				fields = append(fields, fv)
			}
		})
		if err == nil {
			dest = types.NewStructWithType(t, fields)
		}
		return
	case *v7types.Type:
		return migrateType(source), nil
	case v7types.Ref:
		var val types.Value
		v7val := source.TargetValue(sourceStore)
		val, err = MigrateFromVersion7(v7val, sourceStore, sinkStore)
		if err == nil {
			dest = sinkStore.WriteValue(val)
		}
		return
	}

	panic(fmt.Sprintf("unreachable type: %T", source))
}

func migrateType(source *v7types.Type) *types.Type {
	migrateChildTypes := func() []*types.Type {
		sc := source.Desc.(v7types.CompoundDesc).ElemTypes
		dest := make([]*types.Type, 0, len(sc))
		for i := 0; i < len(sc); i++ {
			dest = append(dest, migrateType(sc[i]))
		}
		return dest
	}

	switch source.Kind() {
	case v7types.BoolKind:
		return types.BoolType
	case v7types.NumberKind:
		return types.NumberType
	case v7types.StringKind:
		return types.StringType
	case v7types.BlobKind:
		return types.BlobType
	case v7types.ValueKind:
		return types.ValueType
	case v7types.ListKind:
		return types.MakeListType(migrateChildTypes()[0])
	case v7types.MapKind:
		ct := migrateChildTypes()
		d.Chk.Equal(2, len(ct))
		return types.MakeMapType(ct[0], ct[1])
	case v7types.SetKind:
		return types.MakeSetType(migrateChildTypes()[0])
	case v7types.RefKind:
		return types.MakeRefType(migrateChildTypes()[0])
	case v7types.UnionKind:
		return types.MakeUnionType(migrateChildTypes()...)
	case v7types.TypeKind:
		return types.TypeType
	case v7types.StructKind:
		source = v7types.ToUnresolvedType(source)
		sd := source.Desc.(v7types.StructDesc)
		names := make([]string, 0, sd.Len())
		typs := make([]*types.Type, 0, sd.Len())
		sd.IterFields(func(name string, t *v7types.Type) {
			names = append(names, name)
			typs = append(typs, migrateType(t))
		})
		return types.MakeStructType(sd.Name, names, typs)
	case v7types.CycleKind:
		return types.MakeCycleType(uint32(source.Desc.(types.CycleDesc)))
	}

	panic(fmt.Sprintf("unreachable kind: %d", source.Kind()))
}
