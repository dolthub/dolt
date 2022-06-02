// Copyright 2022 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package enginetest

import (
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
)

// Tests in this file are a grab bag of DDL queries, many of them ported from older parts of the Dolt codebase
// before enginetest format adoption. Typically you shouldn't add things here instead of in the enginetest package in
// go-mysql-server, but it's appropriate for dolt-specific tests of DDL operations.

var SimpsonsSetup = []string{
	`create table people (id int primary key,
		first_name varchar(100) not null,
		last_name varchar(100) not null,
		is_married tinyint,
		age int,
		rating float,
		uuid varchar(64),
		num_episodes int unsigned);`,
	`create table episodes (id int primary key,
		name varchar(100) not null,
		air_date datetime,
		rating float);`,
	`create table appearances (character_id int not null,
		episode_id int not null,
		comments varchar(100),
		primary key (character_id, episode_id));`,
	`insert into people values 
		(0, "Homer", "Simpson", 1, 40, 8.5, null, null),
		(1, "Marge", "Simpson", 1, 38, 8, "00000000-0000-0000-0000-000000000001", 111),
		(2, "Bart", "Simpson", 0, 10, 9, "00000000-0000-0000-0000-000000000002", 222),
		(3, "Lisa", "Simpson", 0, 8, 10, "00000000-0000-0000-0000-000000000003", 333),
		(4, "Moe", "Szyslak", 0, 48, 6.5, "00000000-0000-0000-0000-000000000004", 444),
		(5, "Barney", "Gumble", 0, 40, 4, "00000000-0000-0000-0000-000000000005", 555);
`,
	`insert into episodes values 
		(1, "Simpsons Roasting On an Open Fire", "1989-12-18 03:00:00", 8.0),
		(2, "Bart the Genius", "1990-01-15 03:00:00", 9.0),
		(3, "Homer's Odyssey", "1990-01-22 03:00:00", 7.0),
		(4, "There's No Disgrace Like Home", "1990-01-29 03:00:00", 8.5);
`,
	`insert into appearances values
		(0, 1, "Homer is great in this one"),
		(1, 1, "Marge is here too"),
		(0, 2, "Homer is great in this one too"),
		(2, 2, "This episode is named after Bart"),
		(3, 2, "Lisa is here too"),
		(4, 2, "I think there's a prank call scene"),
		(0, 3, "Homer is in every episode"),
		(1, 3, "Marge shows up a lot too"),
		(3, 3, "Lisa is the best Simpson"),
		(5, 3, "I'm making this all up");
`,
}

var AllInitialSimpsonsCharacters = []sql.Row{
	{0, "Homer", "Simpson", 1, 40, 8.5, nil, nil},
	{1, "Marge", "Simpson", 1, 38, 8.0, "00000000-0000-0000-0000-000000000001", uint(111)},
	{2, "Bart", "Simpson", 0, 10, 9.0, "00000000-0000-0000-0000-000000000002", uint(222)},
	{3, "Lisa", "Simpson", 0, 8, 10.0, "00000000-0000-0000-0000-000000000003", uint(333)},
	{4, "Moe", "Szyslak", 0, 48, 6.5, "00000000-0000-0000-0000-000000000004", uint(444)},
	{5, "Barney", "Gumble", 0, 40, 4.0, "00000000-0000-0000-0000-000000000005", uint(555)},
}

var ModifyAndChangeColumnScripts = []queries.ScriptTest{
	{
		Name:        "alter modify column reorder middle",
		SetUpScript: SimpsonsSetup,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "alter table people modify column first_name varchar(16383) not null after last_name",
				SkipResultsCheck: true,
			},
			{
				Query: "show create table people",
				Expected: []sql.Row{sql.Row{"people", "CREATE TABLE `people` (\n" +
					"  `id` int NOT NULL,\n" +
					"  `last_name` varchar(100) NOT NULL,\n" +
					"  `first_name` varchar(16383) NOT NULL,\n" +
					"  `is_married` tinyint,\n" +
					"  `age` int,\n" +
					"  `rating` float,\n" +
					"  `uuid` varchar(64),\n" +
					"  `num_episodes` int unsigned,\n" +
					"  PRIMARY KEY (`id`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "select * from people order by 1",
				Expected: []sql.Row{
					{0, "Simpson", "Homer", 1, 40, 8.5, nil, nil},
					{1, "Simpson", "Marge", 1, 38, 8.0, "00000000-0000-0000-0000-000000000001", uint(111)},
					{2, "Simpson", "Bart", 0, 10, 9.0, "00000000-0000-0000-0000-000000000002", uint(222)},
					{3, "Simpson", "Lisa", 0, 8, 10.0, "00000000-0000-0000-0000-000000000003", uint(333)},
					{4, "Szyslak", "Moe", 0, 48, 6.5, "00000000-0000-0000-0000-000000000004", uint(444)},
					{5, "Gumble", "Barney", 0, 40, 4.0, "00000000-0000-0000-0000-000000000005", uint(555)},
				},
			},
		},
	},
	{
		Name:        "alter modify column reorder first",
		SetUpScript: SimpsonsSetup,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "alter table people modify column first_name varchar(16383) not null first",
				SkipResultsCheck: true,
			},
			{
				Query: "show create table people",
				Expected: []sql.Row{sql.Row{"people", "CREATE TABLE `people` (\n" +
					"  `first_name` varchar(16383) NOT NULL,\n" +
					"  `id` int NOT NULL,\n" +
					"  `last_name` varchar(100) NOT NULL,\n" +
					"  `is_married` tinyint,\n" +
					"  `age` int,\n" +
					"  `rating` float,\n" +
					"  `uuid` varchar(64),\n" +
					"  `num_episodes` int unsigned,\n" +
					"  PRIMARY KEY (`id`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "select * from people order by id",
				Expected: []sql.Row{
					{"Homer", 0, "Simpson", 1, 40, 8.5, nil, nil},
					{"Marge", 1, "Simpson", 1, 38, 8.0, "00000000-0000-0000-0000-000000000001", uint(111)},
					{"Bart", 2, "Simpson", 0, 10, 9.0, "00000000-0000-0000-0000-000000000002", uint(222)},
					{"Lisa", 3, "Simpson", 0, 8, 10.0, "00000000-0000-0000-0000-000000000003", uint(333)},
					{"Moe", 4, "Szyslak", 0, 48, 6.5, "00000000-0000-0000-0000-000000000004", uint(444)},
					{"Barney", 5, "Gumble", 0, 40, 4.0, "00000000-0000-0000-0000-000000000005", uint(555)},
				},
			},
		},
	},
	{
		Name:        "alter modify column drop null constraint",
		SetUpScript: SimpsonsSetup,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "alter table people modify column first_name varchar(16383) null",
				SkipResultsCheck: true,
			},
			{
				Query: "show create table people",
				Expected: []sql.Row{sql.Row{"people", "CREATE TABLE `people` (\n" +
					"  `id` int NOT NULL,\n" +
					"  `first_name` varchar(16383),\n" +
					"  `last_name` varchar(100) NOT NULL,\n" +
					"  `is_married` tinyint,\n" +
					"  `age` int,\n" +
					"  `rating` float,\n" +
					"  `uuid` varchar(64),\n" +
					"  `num_episodes` int unsigned,\n" +
					"  PRIMARY KEY (`id`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "select * from people order by id",
				Expected: AllInitialSimpsonsCharacters,
			},
		},
	},
	{
		Name:        "alter change column rename and reorder",
		SetUpScript: SimpsonsSetup,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "alter table people change first_name christian_name varchar(16383) not null after last_name",
				SkipResultsCheck: true,
			},
			{
				Query: "show create table people",
				Expected: []sql.Row{sql.Row{"people", "CREATE TABLE `people` (\n" +
					"  `id` int NOT NULL,\n" +
					"  `last_name` varchar(100) NOT NULL,\n" +
					"  `christian_name` varchar(16383) NOT NULL,\n" +
					"  `is_married` tinyint,\n" +
					"  `age` int,\n" +
					"  `rating` float,\n" +
					"  `uuid` varchar(64),\n" +
					"  `num_episodes` int unsigned,\n" +
					"  PRIMARY KEY (`id`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "select * from people order by id",
				Expected: []sql.Row{
					{0, "Simpson", "Homer", 1, 40, 8.5, nil, nil},
					{1, "Simpson", "Marge", 1, 38, 8.0, "00000000-0000-0000-0000-000000000001", uint(111)},
					{2, "Simpson", "Bart", 0, 10, 9.0, "00000000-0000-0000-0000-000000000002", uint(222)},
					{3, "Simpson", "Lisa", 0, 8, 10.0, "00000000-0000-0000-0000-000000000003", uint(333)},
					{4, "Szyslak", "Moe", 0, 48, 6.5, "00000000-0000-0000-0000-000000000004", uint(444)},
					{5, "Gumble", "Barney", 0, 40, 4.0, "00000000-0000-0000-0000-000000000005", uint(555)},
				},
			},
		},
	},
	{
		Name:        "alter change column rename and reorder first",
		SetUpScript: SimpsonsSetup,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "alter table people change column first_name christian_name varchar(16383) not null first",
				SkipResultsCheck: true,
			},
			{
				Query: "show create table people",
				Expected: []sql.Row{sql.Row{"people", "CREATE TABLE `people` (\n" +
					"  `christian_name` varchar(16383) NOT NULL,\n" +
					"  `id` int NOT NULL,\n" +
					"  `last_name` varchar(100) NOT NULL,\n" +
					"  `is_married` tinyint,\n" +
					"  `age` int,\n" +
					"  `rating` float,\n" +
					"  `uuid` varchar(64),\n" +
					"  `num_episodes` int unsigned,\n" +
					"  PRIMARY KEY (`id`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "select * from people order by id",
				Expected: []sql.Row{
					{"Homer", 0, "Simpson", 1, 40, 8.5, nil, nil},
					{"Marge", 1, "Simpson", 1, 38, 8.0, "00000000-0000-0000-0000-000000000001", uint(111)},
					{"Bart", 2, "Simpson", 0, 10, 9.0, "00000000-0000-0000-0000-000000000002", uint(222)},
					{"Lisa", 3, "Simpson", 0, 8, 10.0, "00000000-0000-0000-0000-000000000003", uint(333)},
					{"Moe", 4, "Szyslak", 0, 48, 6.5, "00000000-0000-0000-0000-000000000004", uint(444)},
					{"Barney", 5, "Gumble", 0, 40, 4.0, "00000000-0000-0000-0000-000000000005", uint(555)},
				},
			},
		},
	},
	{
		Name:        "alter change column drop null constraint",
		SetUpScript: SimpsonsSetup,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "alter table people change column first_name first_name varchar(16383) null",
				SkipResultsCheck: true,
			},
			{
				Query: "show create table people",
				Expected: []sql.Row{sql.Row{"people", "CREATE TABLE `people` (\n" +
					"  `id` int NOT NULL,\n" +
					"  `first_name` varchar(16383),\n" +
					"  `last_name` varchar(100) NOT NULL,\n" +
					"  `is_married` tinyint,\n" +
					"  `age` int,\n" +
					"  `rating` float,\n" +
					"  `uuid` varchar(64),\n" +
					"  `num_episodes` int unsigned,\n" +
					"  PRIMARY KEY (`id`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "select * from people order by id",
				Expected: AllInitialSimpsonsCharacters,
			},
		},
	},
	{
		Name:        "alter modify column not null with type mismatch in default",
		SetUpScript: SimpsonsSetup,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:          "alter table people modify rating double default 'not a number'",
				ExpectedErrStr: "incompatible type for default value",
			},
		},
	},
	{
		Name:        "alter modify column not null, existing null values",
		SetUpScript: SimpsonsSetup,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:       "alter table people modify num_episodes bigint unsigned not null",
				ExpectedErr: sql.ErrInsertIntoNonNullableProvidedNull,
			},
		},
	},
}

var ModifyColumnTypeScripts = []queries.ScriptTest{
	{
		Name: "alter modify column type similar types",
		SetUpScript: []string{
			"create table test(pk bigint primary key, v1 bigint, index (v1))",
			"insert into test values (0, 3), (1, 2)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "alter table test modify column v1 int",
				SkipResultsCheck: true,
			},
			{
				Query: "show create table test",
				Expected: []sql.Row{{"test", "CREATE TABLE `test` (\n" +
					"  `pk` bigint NOT NULL,\n" +
					"  `v1` int,\n" +
					"  PRIMARY KEY (`pk`),\n" +
					"  KEY `v1` (`v1`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "select * from test order by pk",
				Expected: []sql.Row{{0, 3}, {1, 2}},
			},
			{
				Query:    "select * from test where v1 = 3",
				Expected: []sql.Row{{0, 3}},
			},
		},
	},
	{
		Name: "alter modify column type different types",
		SetUpScript: []string{
			"create table test(pk bigint primary key, v1 bigint, index (v1))",
			"insert into test values (0, 3), (1, 2)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "alter table test modify column v1 varchar(20)",
				SkipResultsCheck: true,
			},
			{
				Query: "show create table test",
				Expected: []sql.Row{{"test", "CREATE TABLE `test` (\n" +
					"  `pk` bigint NOT NULL,\n" +
					"  `v1` varchar(20),\n" +
					"  PRIMARY KEY (`pk`),\n" +
					"  KEY `v1` (`v1`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "select * from test order by pk",
				Expected: []sql.Row{{0, "3"}, {1, "2"}},
			},
			{
				Query:    "select * from test where v1 = '3'",
				Expected: []sql.Row{{0, "3"}},
			},
		},
	},
	{
		Name: "alter modify column type different types reversed",
		SetUpScript: []string{
			"create table test(pk bigint primary key, v1 varchar(20), index (v1))",
			`insert into test values (0, "3"), (1, "2")`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "alter table test modify column v1 bigint",
				SkipResultsCheck: true,
			},
			{
				Query: "show create table test",
				Expected: []sql.Row{{"test", "CREATE TABLE `test` (\n" +
					"  `pk` bigint NOT NULL,\n" +
					"  `v1` bigint,\n" +
					"  PRIMARY KEY (`pk`),\n" +
					"  KEY `v1` (`v1`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "select * from test order by pk",
				Expected: []sql.Row{{0, 3}, {1, 2}},
			},
			{
				Query:    "select * from test where v1 = 3",
				Expected: []sql.Row{{0, 3}},
			},
		},
	},
	{
		Name: "alter modify column type primary key",
		SetUpScript: []string{
			"create table test(pk bigint primary key, v1 bigint, index (v1))",
			"insert into test values (0, 3), (1, 2)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "alter table test modify column pk varchar(20)",
				SkipResultsCheck: true,
			},
			{
				Query: "show create table test",
				Expected: []sql.Row{{"test", "CREATE TABLE `test` (\n" +
					"  `pk` varchar(20) NOT NULL,\n" +
					"  `v1` bigint,\n" +
					"  PRIMARY KEY (`pk`),\n" +
					"  KEY `v1` (`v1`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "select * from test order by pk",
				Expected: []sql.Row{{"0", 3}, {"1", 2}},
			},
			{
				Query:    "select * from test where v1 = 3",
				Expected: []sql.Row{{"0", 3}},
			},
		},
	},
	{
		Name: "alter modify column type incompatible types with empty table",
		SetUpScript: []string{
			"create table test(pk bigint primary key, v1 bit(20), index (v1))",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "alter table test modify column pk datetime",
				SkipResultsCheck: true,
			},
			{
				Query: "show create table test",
				Expected: []sql.Row{{"test", "CREATE TABLE `test` (\n" +
					"  `pk` datetime NOT NULL,\n" +
					"  `v1` bit(20),\n" +
					"  PRIMARY KEY (`pk`),\n" +
					"  KEY `v1` (`v1`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "select * from test order by pk",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "alter modify column type incompatible types with non-empty table",
		SetUpScript: []string{
			"create table test(pk bigint primary key, v1 bit(20), index (v1))",
			"insert into test values (1, 1)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:       "alter table test modify column pk datetime",
				ExpectedErr: sql.ErrConvertingToTime,
			},
		},
	},
	{
		Name: "alter modify column type different types incompatible values",
		SetUpScript: []string{
			"create table test(pk bigint primary key, v1 varchar(20), index (v1))",
			"insert into test values (0, 3), (1, 'a')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:       "alter table test modify column v1 bigint",
				ExpectedErr: sql.ErrInvalidValue,
			},
		},
	},
	{
		Name: "alter modify column type foreign key parent",
		SetUpScript: []string{
			"create table test(pk bigint primary key, v1 bigint, index (v1))",
			"create table test2(pk bigint primary key, v1 bigint, index (v1), foreign key (v1) references test(v1))",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:       "alter table test modify column v1 varchar(20)",
				ExpectedErr: sql.ErrForeignKeyTypeChange,
			},
		},
	},
	{
		Name: "alter modify column type foreign key child",
		SetUpScript: []string{
			"create table test(pk bigint primary key, v1 bigint, index (v1))",
			"create table test2(pk bigint primary key, v1 bigint, index (v1), foreign key (v1) references test(v1))",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:       "alter table test2 modify column v1 varchar(20)",
				ExpectedErr: sql.ErrForeignKeyTypeChange,
			},
		},
	},
	{
		Name: "alter modify column type, make primary key spatial",
		SetUpScript: []string{
			"create table point_tbl (p int primary key)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:       "alter table point_tbl modify column p point primary key",
				ExpectedErr: schema.ErrUsingSpatialKey,
			},
		},
	},
}

var DropColumnScripts = []queries.ScriptTest{
	{
		Name:        "alter drop column",
		SetUpScript: SimpsonsSetup,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "alter table people drop rating",
				SkipResultsCheck: true,
			},
			{
				Query: "show create table people",
				Expected: []sql.Row{{"people", "CREATE TABLE `people` (\n" +
					"  `id` int NOT NULL,\n" +
					"  `first_name` varchar(100) NOT NULL,\n" +
					"  `last_name` varchar(100) NOT NULL,\n" +
					"  `is_married` tinyint,\n" +
					"  `age` int,\n" +
					"  `uuid` varchar(64),\n" +
					"  `num_episodes` int unsigned,\n" +
					"  PRIMARY KEY (`id`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "select * from people order by 1",
				Expected: []sql.Row{
					{0, "Homer", "Simpson", 1, 40, nil, nil},
					{1, "Marge", "Simpson", 1, 38, "00000000-0000-0000-0000-000000000001", uint(111)},
					{2, "Bart", "Simpson", 0, 10, "00000000-0000-0000-0000-000000000002", uint(222)},
					{3, "Lisa", "Simpson", 0, 8, "00000000-0000-0000-0000-000000000003", uint(333)},
					{4, "Moe", "Szyslak", 0, 48, "00000000-0000-0000-0000-000000000004", uint(444)},
					{5, "Barney", "Gumble", 0, 40, "00000000-0000-0000-0000-000000000005", uint(555)},
				},
			},
		},
	},
	{
		Name:        "alter drop column with optional column keyword",
		SetUpScript: SimpsonsSetup,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "alter table people drop column rating",
				SkipResultsCheck: true,
			},
			{
				Query: "show create table people",
				Expected: []sql.Row{{"people", "CREATE TABLE `people` (\n" +
					"  `id` int NOT NULL,\n" +
					"  `first_name` varchar(100) NOT NULL,\n" +
					"  `last_name` varchar(100) NOT NULL,\n" +
					"  `is_married` tinyint,\n" +
					"  `age` int,\n" +
					"  `uuid` varchar(64),\n" +
					"  `num_episodes` int unsigned,\n" +
					"  PRIMARY KEY (`id`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "select * from people order by 1",
				Expected: []sql.Row{
					{0, "Homer", "Simpson", 1, 40, nil, nil},
					{1, "Marge", "Simpson", 1, 38, "00000000-0000-0000-0000-000000000001", uint(111)},
					{2, "Bart", "Simpson", 0, 10, "00000000-0000-0000-0000-000000000002", uint(222)},
					{3, "Lisa", "Simpson", 0, 8, "00000000-0000-0000-0000-000000000003", uint(333)},
					{4, "Moe", "Szyslak", 0, 48, "00000000-0000-0000-0000-000000000004", uint(444)},
					{5, "Barney", "Gumble", 0, 40, "00000000-0000-0000-0000-000000000005", uint(555)},
				},
			},
		},
	},
	{
		Name:        "drop primary key column",
		SetUpScript: SimpsonsSetup,
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "alter table people drop column id",
				SkipResultsCheck: true,
			},
			{
				Query: "show create table people",
				Expected: []sql.Row{{"people", "CREATE TABLE `people` (\n" +
					"  `first_name` varchar(100) NOT NULL,\n" +
					"  `last_name` varchar(100) NOT NULL,\n" +
					"  `is_married` tinyint,\n" +
					"  `age` int,\n" +
					"  `rating` float,\n" +
					"  `uuid` varchar(64),\n" +
					"  `num_episodes` int unsigned\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "select * from people order by first_name",
				Expected: []sql.Row{
					{"Barney", "Gumble", 0, 40, 4.0, "00000000-0000-0000-0000-000000000005", uint(555)},
					{"Bart", "Simpson", 0, 10, 9.0, "00000000-0000-0000-0000-000000000002", uint(222)},
					{"Homer", "Simpson", 1, 40, 8.5, nil, nil},
					{"Lisa", "Simpson", 0, 8, 10.0, "00000000-0000-0000-0000-000000000003", uint(333)},
					{"Marge", "Simpson", 1, 38, 8.0, "00000000-0000-0000-0000-000000000001", uint(111)},
					{"Moe", "Szyslak", 0, 48, 6.5, "00000000-0000-0000-0000-000000000004", uint(444)},
				},
			},
		},
	},
}

var BrokenDDLScripts = []queries.ScriptTest{
	{
		Name: "drop first of two primary key columns",
		SetUpScript: []string{
			"create table test (p1 int, p2 int, c1 int, c2 int, index (c1))",
			"insert into test values (0, 1, 2, 3), (4, 5, 6, 7)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:            "alter table test drop column p1",
				SkipResultsCheck: true,
			},
			{
				Query: "show create table test",
				Expected: []sql.Row{{"test", "CREATE TABLE `test` (\n" +
					"  `p2` int,\n" +
					"  `c1` int,\n" +
					"  `c2` int,\n" +
					"  KEY `c1` (`c1`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "select * from test order by pk",
				Expected: []sql.Row{{0, 3}, {1, 2}},
			},
			{
				Query:    "select * from test where v1 = 3",
				Expected: []sql.Row{{0, 3}},
			},
		},
	},
	{
		Name: "alter string column to truncate data",
		SetUpScript: []string{
			"create table t1 (a int primary key, b varchar(3))",
			"insert into t1 values (1, 'hi'), (2, 'bye')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:       "alter table t1 modify b varchar(2)",
				ExpectedErr: sql.ErrInvalidValue, // not sure of the type of error, but it should give one
			},
		},
	},
	{
		Name: "alter datetime column with invalid values",
		SetUpScript: []string{
			"CREATE TABLE t3(pk BIGINT PRIMARY KEY, v1 DATETIME, INDEX(v1))",
			"INSERT INTO t3 VALUES (0,'1999-11-02 17:39:38'),(1,'3021-01-08 02:59:27');",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:       "alter table t3 modify v1 timestamp",
				ExpectedErr: sql.ErrInvalidValue, // not sure of the type of error, but it should give one
			},
		},
	},
}
