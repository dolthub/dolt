// Copyright 2026 Dolthub, Inc.
// Licensed under the Apache License, Version 2.0.
package main

import (
	"bytes"
	"context"
	_ "embed"
	"testing"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/store/types"
)

// This fixture contains only the diagnosed foreign-key metadata, no rows.
//
//go:embed testdata/damaged-fk.bin
var damaged []byte

func TestRepairMessage(t *testing.T) {
	original := append([]byte(nil), damaged...)
	if _, err := doltdb.DeserializeForeignKeys(context.Background(), types.Format_DOLT, types.SerialMessage(damaged)); err == nil {
		t.Fatal("fixture should fail normal decoding")
	}
	fixed, err := repairMessage(types.SerialMessage(damaged))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(original, damaged) {
		t.Fatal("input modified")
	}
	fkc, err := doltdb.DeserializeForeignKeys(context.Background(), types.Format_DOLT, fixed)
	if err != nil {
		t.Fatal(err)
	}
	keys := fkc.AllKeys()
	if len(keys) != 1 {
		t.Fatalf("keys: %v", keys)
	}
	fk := keys[0]
	if fk.Name != "i9u233qd" || fk.TableName.Name != "vacuum_tubes" || fk.ReferencedTableName.Name != "tubes_type" ||
		fk.TableIndex != "type" || fk.ReferencedTableIndex != "id" ||
		len(fk.TableColumns) != 1 || fk.TableColumns[0] != 14843 || len(fk.ReferencedTableColumns) != 1 || fk.ReferencedTableColumns[0] != 3062 {
		t.Fatalf("incorrect repair: %+v", fk)
	}
	// Check that restoring the names preserves the constraint's actions.
	var old, new serial.ForeignKeyCollection
	if err := serial.InitForeignKeyCollectionRoot(&old, damaged, serial.MessagePrefixSz); err != nil {
		t.Fatal(err)
	}
	if err := serial.InitForeignKeyCollectionRoot(&new, fixed, serial.MessagePrefixSz); err != nil {
		t.Fatal(err)
	}
	var a, b serial.ForeignKey
	if _, err := old.TryForeignKeys(&a, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := new.TryForeignKeys(&b, 0); err != nil {
		t.Fatal(err)
	}
	if a.OnDelete() != b.OnDelete() || a.OnUpdate() != b.OnUpdate() || a.MatchType() != b.MatchType() || a.IsNotValid() != b.IsNotValid() {
		t.Fatal("constraint behavior changed")
	}
	if _, err := repairMessage(fixed); err == nil {
		t.Fatal("must refuse already repaired metadata")
	}
}

func TestRejectUnknownMessage(t *testing.T) {
	for _, input := range [][]byte{nil, []byte("invalid"), append(append([]byte(nil), damaged...), 0)} {
		if _, err := repairMessage(types.SerialMessage(input)); err == nil {
			t.Fatal("accepted unknown metadata")
		}
	}
}
