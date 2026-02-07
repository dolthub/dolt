## ORM Client Tests

These tests verify that various ORM libraries that support MySQL are compatible with Dolt. Ideally,
we use the same test suite that the ORM provides to test their support with MySQL, but in some
cases we may start with a smaller smoke test to get some quick, initial coverage. 

These tests can be run locally using Docker from the root of the workspace directory that contains `dolt/`:

```bash
$ docker build -t orm-tests -f dolt/integration-tests/orm-tests/Dockerfile .
$ docker run orm-tests:latest
```

The `dolt` binary is built in a separate stage from the ORM test runtime. This speeds up first-time image builds with parallel stage building. Secondly, ORM-only changes will reuse the cached binary from the `dolt_build` stage.

You can also build other Dolt related dependencies from a local source into the Docker `dolt` binary. Copy the required source into the `dolt_build` stage's `build/` directory in the `orm-tests/Dockerfile`. The one caveat is that your repository must be placed in the same parent directory as `dolt`.
```dockerfile
COPY go-mysql-server /build/go-mysql-server
```
The line above has been commented out in the actual Dockerfile so you know the correct placement. You are responsible for updating `go.mod` with the `replace` directive in your local `dolt/` directory.

To stop the build at a specific stage use [`--target <stage_name>`](https://docs.docker.com/build/building/multi-stage/#stop-at-a-specific-build-stage) option. You can then run the build as a normal image afterward, but it'll run within the target stage. Alternatively, use GoLand's bundled Docker plugin. You may have to add `*Dockerfile` as a pattern in your [File Types](https://www.jetbrains.com/help/go/creating-and-registering-file-types.html#register-new-association) settings.