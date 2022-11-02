## ORM Client Tests

Different ORMs come with test packages that run against a standard MySQL database. These
tests run an ORM's test suite against Dolt to continously test MySQL
capabilities. 

These tests can be run locally using Docker. From the root directory of this
repo, run:

```bash
$ docker build -t orm-tests -f ORMDockerfile
$ docker run orm-tests:latest
```

Running the containter should produce output like:

```bash
Updating dolt config for tests:
Config successfully updated.
Config successfully updated.
Config successfully updated.
Config successfully updated.
Running orm-tests:
1..1
not ok 1 peewee client test
```