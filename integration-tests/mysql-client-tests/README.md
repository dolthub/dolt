## MySQL Client Tests
We created smoke tests for Dolt's MySQL client integrations and we run these tests through GitHub Actions
on pull requests.

These tests can be run locally using Docker. Before you can build the image, you also need to copy the go folder
into the integration-tests folder; unfortunately just symlinking doesn't seem to work. From the
integration-tests directory of the dolt repo, run:

```bash
$ cp -r ../go . 
$ docker build -t mysql-client-tests -f MySQLDockerfile .
$ docker run mysql-client-tests:latest
```

The `docker build` step will take a few minutes to complete as it needs to install all the dependencies in the image.

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
