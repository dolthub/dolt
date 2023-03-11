# Node Client Integration Tests

## Install

```
$ npm install
```

## Run node tests

To run the node tests, you must make sure you have Dolt installed on your computer and
have run `npm install`. Then update your Dolt config by running:

```shell
sh ../mysql-client-tests-entrypoint.sh
```

And then you can run the tests using the `run-tests.sh` script, which sets up the database, runs the SQL server, runs the provided `.js` test file against the running server, and then tears down the database.

For example, you can run the `workbench.js` tests by running:

```shell
sh run-tests.sh workbench.js
```

## Workbench stability tests

The tests in `workbenchTests` were written to enforce the stability of the SQL workbench
on [Hosted](https://hosted.doltdb.com/). The workbench uses many Dolt system tables,
functions, and procedures, and any changes to these interfaces can break the workbench.
@tbantle22 will be tagged in any GitHub PR that updates those files to ensure appropriate
workbench updates are made for Dolt changes that break these queries.
