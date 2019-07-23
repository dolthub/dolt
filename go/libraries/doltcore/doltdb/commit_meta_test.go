package doltdb

import (
	"reflect"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

func TestCommitMetaToAndFromNomsStruct(t *testing.T) {
	cm, _ := NewCommitMeta("Bill Billerson", "bigbillieb@fake.horse", "This is a test commit")
	cmSt := cm.toNomsStruct(types.Format_7_18)
	result, err := commitMetaFromNomsSt(cmSt)

	if err != nil {
		t.Fatal("Failed to convert from types.Struct to CommitMeta")
	} else if !reflect.DeepEqual(cm, result) {
		t.Error("CommitMeta was not converted without error.")
	}

	t.Log(cm.String())
}
