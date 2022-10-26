go-sql-server-driver
====================

This is a driver and a test suite for tests interacting with `dolt sql-server`.
These tests describe a setup for the sql-server(s) which should be running, the
interactions which should be run against the servers, and the assertions which
should pass given those interactions.

This is meant to be more declarative and more robust than using `bats` for
these integration tests.

Something belongs in this package if it primarly tests interactions with the
exposed MySQL port on the sql-server.

Something belongs in `bats` if it primarly tests interactions with the `dolt`
binary itself. One example for the `dolt sql-server` command itself would be
testing config validation which results in exit codes or help text displayed.
