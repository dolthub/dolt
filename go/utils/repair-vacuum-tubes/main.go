// Copyright 2026 Dolthub, Inc.
// Licensed under the Apache License, Version 2.0.

// repair-vacuum-tubes repairs the specific legacy migration damage diagnosed in
// captainstabs/vacuum-tubes. It deliberately refuses other foreign-key objects.
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

const damagedFK = "4jnflfmh77fvhemaduakn7ndv8jgi8oq"

func main() {
	db := flag.String("db", "", "database directory (required; stop any SQL server first)")
	branch := flag.String("branch", "master", "local branch to repair")
	apply := flag.Bool("apply", false, "create a backup and commit the repair; default only checks")
	flag.Parse()
	if err := run(context.Background(), *db, *branch, *apply); err != nil {
		fmt.Fprintln(os.Stderr, "repair failed:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, path, branch string, apply bool) error {
	if path == "" {
		return fmt.Errorf("-db is required")
	}
	path, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	fs, err := filesys.LocalFilesysWithWorkingDir(path)
	if err != nil {
		return err
	}
	ddb, err := doltdb.LoadDoltDB(ctx, types.Format_DOLT, doltdb.LocalDirDoltDB, fs)
	if err != nil {
		return err
	}
	defer ddb.Close()
	headRef := ref.NewBranchRef(branch)
	head, err := ddb.ResolveCommitRef(ctx, headRef)
	if err != nil {
		return err
	}
	root, err := head.GetRootValue(ctx)
	if err != nil {
		return err
	}
	wsRef, err := ref.WorkingSetRefForHead(headRef)
	if err != nil {
		return err
	}
	ws, err := ddb.ResolveWorkingSet(ctx, wsRef)
	if err != nil {
		return err
	}
	if ws.MergeActive() || ws.RebaseActive() {
		return fmt.Errorf("merge or rebase is active")
	}
	rootHash, err := root.HashOf()
	if err != nil {
		return err
	}
	for _, r := range []doltdb.RootValue{ws.WorkingRoot(), ws.StagedRoot()} {
		h, err := r.HashOf()
		if err != nil {
			return err
		}
		if h != rootHash {
			return fmt.Errorf("refusing to repair a branch with uncommitted changes")
		}
	}
	prev, err := ws.HashOf()
	if err != nil {
		return err
	}
	msg, ok := root.NomsValue().(types.SerialMessage)
	if !ok || serial.GetFileID(msg) != serial.RootValueFileID {
		return fmt.Errorf("unsupported root format")
	}
	var rv serial.RootValue
	if err := serial.InitRootValueRoot(&rv, msg, serial.MessagePrefixSz); err != nil {
		return err
	}
	fkHash := hash.New(rv.ForeignKeyAddrBytes())
	if fkHash.String() != damagedFK {
		return fmt.Errorf("foreign-key object is %s, expected %s; already repaired or different database", fkHash, damagedFK)
	}
	for _, expected := range []struct {
		table, column string
		tag           uint64
	}{
		{"vacuum_tubes", "type", 14843}, {"tubes_type", "id", 3062},
	} {
		table, found, err := root.GetTable(ctx, doltdb.TableName{Name: expected.table})
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("missing table %s", expected.table)
		}
		sch, err := table.GetSchema(ctx)
		if err != nil {
			return err
		}
		col, found := sch.GetAllCols().GetByTag(expected.tag)
		if !found || col.Name != expected.column {
			return fmt.Errorf("column tag mismatch for %s.%s", expected.table, expected.column)
		}
	}
	value, err := ddb.ValueReadWriter().ReadValue(ctx, fkHash)
	if err != nil {
		return err
	}
	fkMsg, ok := value.(types.SerialMessage)
	if !ok {
		return fmt.Errorf("foreign-key object is not a serial message")
	}
	fixed, err := repairMessage(fkMsg)
	if err != nil {
		return err
	}
	fkc, err := doltdb.DeserializeForeignKeys(ctx, ddb.Format(), fixed)
	if err != nil {
		return err
	}
	fmt.Printf("Verified %s: restore i9u233qd, vacuum_tubes(type) -> tubes_type(id)\n", branch)
	if !apply {
		fmt.Println("Check only. Run with -apply to back up the database and add a repair commit.")
		return nil
	}

	// Copy while the local store is held open, before writing any repair chunks.
	// The user must stop SQL servers / other writers before invoking this tool.
	backup, err := os.MkdirTemp(filepath.Dir(path), filepath.Base(path)+".before-fk-repair-")
	if err != nil {
		return err
	}
	if err := os.CopyFS(backup, os.DirFS(path)); err != nil {
		return fmt.Errorf("backup incomplete at %s: %w", backup, err)
	}
	fmt.Println("Backup:", backup)
	repaired, err := root.PutForeignKeyCollection(ctx, fkc)
	if err != nil {
		return err
	}
	// Verify that every table object (schema, rows and indexes) is unchanged.
	names, err := root.GetAllTableNames(ctx, true)
	if err != nil {
		return err
	}
	for _, name := range names {
		before, _, err := root.GetTableHash(ctx, name)
		if err != nil {
			return err
		}
		after, found, err := repaired.GetTableHash(ctx, name)
		if err != nil {
			return err
		}
		if !found || before != after {
			return fmt.Errorf("table changed unexpectedly: %s", name)
		}
	}
	meta, err := datas.NewCommitMeta("vacuum-tubes repair", "repair@localhost", "Restore foreign-key table names lost during format migration")
	if err != nil {
		return err
	}
	pending, err := ddb.NewPendingCommit(ctx, doltdb.Roots{Head: root, Working: repaired, Staged: repaired}, nil, hash.Hash{}, meta)
	if err != nil {
		return err
	}
	commit, err := ddb.CommitWithWorkingSet(ctx, headRef, wsRef, pending, ws.WithWorkingRoot(repaired).WithStagedRoot(repaired), prev, doltdb.TodoWorkingSetMeta(), nil)
	if err != nil {
		return err
	}
	h, err := commit.HashOf()
	if err != nil {
		return err
	}
	fmt.Printf("Repair committed: %s\nAll table hashes unchanged. Historical commits and remote refs are unchanged.\n", h)
	return nil
}

// Append replacement FlatBuffer strings and redirect their existing slots. All
// other fields survive byte-for-byte; normal Dolt serialization writes the result.
func repairMessage(original types.SerialMessage) (types.SerialMessage, error) {
	if h, err := original.Hash(types.Format_DOLT); err != nil || h.String() != damagedFK {
		return nil, fmt.Errorf("refusing an unrecognized foreign-key message")
	}
	b := append(types.SerialMessage(nil), original...)
	var collection serial.ForeignKeyCollection
	if err := serial.InitForeignKeyCollectionRoot(&collection, b, serial.MessagePrefixSz); err != nil {
		return nil, err
	}
	if collection.ForeignKeysLength() != 1 {
		return nil, fmt.Errorf("expected one foreign key")
	}
	var fk serial.ForeignKey
	if _, err := collection.TryForeignKeys(&fk, 0); err != nil {
		return nil, err
	}
	if string(fk.Name()) != "i9u233qd" || len(fk.ChildTableName()) != 0 || len(fk.ParentTableName()) != 0 {
		return nil, fmt.Errorf("unexpected foreign-key fields")
	}
	table := fk.Table()
	for i, name := range []string{"vacuum_tubes", "tubes_type"} {
		offset := table.Offset(6)
		if i == 1 {
			offset = table.Offset(12)
		}
		if offset == 0 {
			return nil, fmt.Errorf("missing table-name slot")
		}
		pos := int(table.Pos) + int(offset)
		for len(b)%4 != 0 {
			b = append(b, 0)
		}
		start := len(b)
		b = binary.LittleEndian.AppendUint32(b, uint32(len(name)))
		b = append(b, name...)
		b = append(b, 0)
		binary.LittleEndian.PutUint32(b[pos:pos+4], uint32(start-pos))
	}
	// The serial message prefix includes a 24-bit big-endian payload length.
	b[1], b[2], b[3] = byte((len(b)-4)>>16), byte((len(b)-4)>>8), byte(len(b)-4)
	return b, nil
}
