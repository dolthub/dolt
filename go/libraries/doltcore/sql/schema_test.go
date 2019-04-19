package sql

import (
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
  num_episodes unsigned int comment 'tag:8',
  primary key (id)
);`

func TestSchemaAsCreateStmt(t *testing.T) {
	tSchema := createPeopleTestSchema()
	str, _ := SchemaAsCreateStmt("table_name", tSchema)

	if str != expectedSQL {
		t.Error("\n", str, "\n\t!=\n", expectedSQL)
	}
}
