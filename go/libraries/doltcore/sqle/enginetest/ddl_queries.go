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

import "github.com/dolthub/go-mysql-server/enginetest/queries"

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
	`insert into people values (0, "Homer", "Simpson", 1, 40, 8.5, null, null),
		(0, "Homer", "Simpson", 1, 40, 8.5, null, null),
		(1, "Marge", "Simpson", 1, 38, 8, "00000000-0000-0000-0000-000000000001", 111),
		(2, "Bart", "Simpson", 0, 10, 9, "00000000-0000-0000-0000-000000000002", 222),
		(3, "Lisa", "Simpson", 0, 8, 10, "00000000-0000-0000-0000-000000000003", 333),
		(4, "Moe", "Szyslak", 0, 48, 6.5, "00000000-0000-0000-0000-000000000004", 444),
		(5, "Barney", "Gumble", 0, 40, 4, "00000000-0000-0000-0000-000000000004", 555);
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
		(5, 3, "I'm making this all up"),
`,
}

// DdlQueries are a grab bag of DDL queries, many of them ported from older parts of the Dolt codebase before
// enginetest adoption. Typically you shouldn't add things here instead of in the enginetest package in go-mysql-server,
// but it's appropriate for dolt-specific tests.
var DdlQueries = []queries.ScriptTest{
	{
		Name:         "",
		SetUpScript:  nil,
		Assertions:   nil,
		Query:        "",
		Expected:     nil,
		ExpectedErr:  nil,
		SkipPrepared: false,
	},
}
