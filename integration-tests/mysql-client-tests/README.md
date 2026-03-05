## MySQL Client Tests
This suite contains smoke tests for Dolt's MySQL and MariaDB client integrations.
It runs in GitHub Actions on pull requests.

## Run Locally With Docker
Build from the workspace root that contains the `dolt/` directory. The Dockerfile copies files using
paths like `dolt/go/...`, so the build context must include that root. You can skip all the commands by using the GoLand's built-in Docker extension.

```bash
$ cd <workspace-root>
$ docker build -t mysql-client-tests -f dolt/integration-tests/mysql-client-tests/Dockerfile .
$ docker run mysql-client-tests:latest
```

The `docker build` step can take several minutes because it installs toolchains and
client dependencies for multiple languages.

Running the built container produces BATS output similar to:

```bash
$ docker run mysql-client-tests:latest
Updating dolt config for tests:
Config successfully updated.
Config successfully updated.
Config successfully updated.
Config successfully updated.
Running mysql-client-tests:
...
```

You can compile local directories by appending another `COPY` right before `go mod install` in the `dolt_build` stage. Make sure to copy the repository (i.e., `go-mysql-server`) into the stage's `/build/` directory. As long as `dolt` references the dependency, it'll automatically resolve.
```dockerfile
COPY dolt/go/go.mod /build/dolt/go/
# COPY go-mysql-server /build/go-mysql-server
WORKDIR /build/dolt/go/
RUN go mod download
```

## Target Specific Stages

When iterating on one area (for example MariaDB CLI behavior), use `--target` to avoid
rebuilding the full multi-stage image.

Build only the MariaDB client stage:

```bash
$ cd <workspace-root>
$ docker build \
    --target mariadb_clients \
    -t mysql-client-tests:mariadb-clients \
    -f dolt/integration-tests/mysql-client-tests/Dockerfile .
```

Inspect and validate that stage manually:

```bash
$ docker run --rm -it --entrypoint /bin/bash mysql-client-tests:mariadb-clients
# ls -la /usr/local
# /usr/local/mariadb-10.11/bin/mariadb --version
# /usr/local/mariadb-11.8/bin/mariadb --version
# ldd /usr/local/mariadb-11.8/bin/mariadb
```
