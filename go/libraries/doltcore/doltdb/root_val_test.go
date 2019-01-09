package doltdb

import (
	"github.com/attic-labs/noms/go/types"
	"testing"
)

func TestTableDiff(t *testing.T) {
	ddb := LoadDoltDB(InMemDoltDB)
	ddb.WriteEmptyRepo("billy bob", "bigbillieb@fake.horse")

	cs, _ := NewCommitSpec("head", "master")
	cm, _ := ddb.Resolve(cs)

	root := cm.GetRootValue()
	added, modified, removed := root.TableDiff(root)

	if len(added)+len(modified)+len(removed) != 0 {
		t.Error("Bad table diff when comparing two repos")
	}

	sch := createTestSchema()
	tbl1, _ := createTestTable(ddb.ValueReadWriter(), sch, types.NewMap(ddb.ValueReadWriter()))

	root2 := root.PutTable(ddb, "tbl1", tbl1)

	added, modified, removed = root2.TableDiff(root)

	if len(added) != 1 || added[0] != "tbl1" || len(modified)+len(removed) != 0 {
		t.Error("Bad table diff after adding a single table")
	}

	added, modified, removed = root.TableDiff(root2)

	if len(removed) != 1 || removed[0] != "tbl1" || len(modified)+len(added) != 0 {
		t.Error("Bad table diff after adding a single table")
	}

	rowData, _ := createTestRowData(ddb.ValueReadWriter(), sch)
	tbl1Updated, _ := createTestTable(ddb.ValueReadWriter(), sch, rowData)

	root3 := root.PutTable(ddb, "tbl1", tbl1Updated)

	added, modified, removed = root3.TableDiff(root2)

	if len(modified) != 1 || modified[0] != "tbl1" || len(added)+len(removed) != 0 {
		t.Error("Bad table diff after adding a single table")
	}

	added, modified, removed = root2.TableDiff(root3)

	if len(modified) != 1 || modified[0] != "tbl1" || len(added)+len(removed) != 0 {
		t.Error("Bad table diff after adding a single table")
	}

	root4 := root3.PutTable(ddb, "tbl2", tbl1)

	added, modified, removed = root2.TableDiff(root4)
	if len(modified) != 1 || modified[0] != "tbl1" || len(removed) != 1 || removed[0] != "tbl2" || +len(added) != 0 {
		t.Error("Bad table diff after adding a second table")
	}

	added, modified, removed = root4.TableDiff(root2)
	if len(modified) != 1 || modified[0] != "tbl1" || len(added) != 1 || added[0] != "tbl2" || +len(removed) != 0 {
		t.Error("Bad table diff after adding a second table")
	}
}
