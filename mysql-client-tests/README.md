## MySQL Client Tests
We created smoke tests for Dolt's MySQL client integrations and we run these tests through Github Actions
on pull requests.

These tests can be run locally using Docker. From the root directory of this repo, run:
```bash
$ docker build -t mysql-client-tests -f MySQLDockerfile .
$ docker run mysql-client-tests:latest
```

The `docker build` step will take a few minutes to complete as it needs to install all of the
dependencies in the image.

Running the built container will produce output like:
```bash
$ docker run mysql-client-tests:latest
updating dolt config for tests:
Config successfully updated.
Config successfully updated.
Config successfully updated.
Config successfully updated.
Running mysql-client-tests:
1..4
ok 1 python mysql.connector client
ok 2 python pymysql client
ok 3 mysql-connector-java client
ok 4 node mysql client
```
