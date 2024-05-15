package enginetest

var VersionedQuerySetup = []string{
	// 2019-01-01
	"create table myhistorytable (i bigint primary key, s text not null)",
	"insert into myhistorytable values (1, 'first row, 1'), (2, 'second row, 1'), (3, 'third row, 1')",
	"call dolt_add('myhistorytable')",
	"call dolt_commit('-m', 'create table')",
	"call dolt_tag('2019-01-01', 'HEAD')",
	// 2019-01-02
	"truncate myhistorytable",
	"insert into myhistorytable values (1, 'first row, 2'), (2, 'second row, 2'), (3, 'third row, 2')",
	"call dolt_add('myhistorytable')",
	"call dolt_commit('-m', 'create table')",
	"call dolt_tag('2019-01-02', 'HEAD')",
	// 2019-01-03
	"drop table myhistorytable",
	"create table myhistorytable (i bigint primary key, s text not null, c text not null)",
	"insert into myhistorytable values (1, 'first row, 3', '1'), (2, 'second row, 3', '2'), (3, 'third row, 3', '3')",
	"call dolt_add('myhistorytable')",
	"call dolt_commit('-m', 'create table')",
	"call dolt_tag('2019-01-03', 'HEAD')",
	//"call dolt_reset('--hard', '2019-01-01')",

	"SET @rev1 = '2019-01-01', @rev2 = '2019-01-02'",
}

var VersionedQueryViews = []string{
	"CREATE VIEW myview1 AS SELECT * FROM myhistorytable",
	"CREATE VIEW myview2 AS SELECT * FROM myview1 WHERE i = 1",
	"CREATE VIEW myview3 AS SELECT i from myview1 union select s from myhistorytable",
	"CREATE VIEW myview4 AS SELECT * FROM myhistorytable where i in (select distinct cast(RIGHT(s, 1) as signed) from myhistorytable)",
	"CREATE VIEW myview5 AS SELECT * FROM (select * from myhistorytable where i in (select distinct cast(RIGHT(s, 1) as signed))) as sq",
}
