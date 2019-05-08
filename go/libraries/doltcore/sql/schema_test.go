package sql

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"testing"
)

const expectedSQL = `CREATE TABLE table_name (
  id int not null comment 'tag:0',
  first varchar not null comment 'tag:1',
  last varchar not null comment 'tag:2',
  is_married bool comment 'tag:3',
  age int comment 'tag:4',
  rating float comment 'tag:6',
  uuid UUID comment 'tag:7',
  num_episodes int unsigned comment 'tag:8',
  primary key (id)
);`

func TestSchemaAsCreateStmt(t *testing.T) {
	tSchema := createPeopleTestSchema()
	str, _ := SchemaAsCreateStmt("table_name", tSchema)

	if str != expectedSQL {
		t.Error("\n", str, "\n\t!=\n", expectedSQL)
	}
}

func TestFmtCol(t *testing.T) {
	tests := []struct {
		Col       schema.Column
		Indent    int
		NameWidth int
		TypeWidth int
		Expected  string
	}{
		{
			schema.NewColumn("first", 0, types.StringKind, true),
			0,
			0,
			0,
			"first varchar comment 'tag:0'",
		},
		{
			schema.NewColumn("last", 123, types.IntKind, true),
			2,
			0,
			0,
			"  last int comment 'tag:123'",
		},
		{
			schema.NewColumn("title", 2, types.UintKind, true),
			0,
			10,
			0,
			"     title int unsigned comment 'tag:2'",
		},
		{
			schema.NewColumn("aoeui", 52, types.UintKind, true),
			0,
			10,
			15,
			"     aoeui    int unsigned comment 'tag:52'",
		},
	}

	for _, test := range tests {
		t.Run(test.Expected, func(t *testing.T) {
			actual := FmtCol(test.Indent, test.NameWidth, test.TypeWidth, test.Col)

			if actual != test.Expected {
				t.Errorf("\n'%s' \n\t!= \n'%s'", actual, test.Expected)
			}
		})
	}
}