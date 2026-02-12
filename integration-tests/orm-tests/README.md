## ORM Client Tests

These tests verify that various ORM libraries that support MySQL are compatible with Dolt. Ideally,
we use the same test suite that the ORM provides to test their support with MySQL, but in some
cases we may start with a smaller smoke test to get some quick, initial coverage. 

These tests can be run locally using Docker. Before you can build the image, you also need to copy the go folder
into the integration-tests folder; unfortunately just symlinking doesn't seem to work. From the
integration-tests directory of the dolt repo, run:

```bash
$ cp -r ../go . 
$ docker build -t orm-tests -f ORMDockerfile . 
$ docker run orm-tests:latest
```

Running the container should produce output like:

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

### Future ORM Libraries to Test
- typeorm
- mikro-orm
- hibernate
