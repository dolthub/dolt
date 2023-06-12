# SQL server tests

These are the definitions for tests that spin up multiple sql-server instances
to test things aren't easily testable otherwise, such as replication. They're
defined as YAML and run with a custom test runner, but they're just golang unit
tests you can run in your IDE.

These are difficult to debug because they start multiple separate processes, but
there's support for attaching a debugger to make it possible.

# Debugging a test

First set `DOLT_SQL_SERVER_TEST_DEBUG` in your environment. This will increase
the timeouts for queries and other processes to give you time to debug. (Don't
set this when not debugging, as it will lead to failing tests that hang instead
of promptly failing).

Next, find the test you want to debug and reference it in `TestSingle`, like so:

```go
func TestSingle(t *testing.T) {
//	t.Skip()
	RunSingleTest(t, "tests/sql-server-cluster.yaml", "primary comes up and replicates to standby")
}
```

Then edit the test to add a `debug_port` for any servers you want to connect to.

```yaml
- name: primary comes up and replicates to standby
  multi_repos:
  - name: server1
    ... 
    server:
      args: ["--port", "3309"]
      port: 3309
      debug_port: 4009
```

When the test is run, the `sql-server` process will wait for the remote debugger
to connect before starting. You probably want to enable this on every server in
the test definition. Use a different port for each.

In your IDE, set up N+1 run configurations: one for each of the N servers in the
test, and 1 to run the test itself. Follow the instructions here to create a new
remote debug configuration for each server, using the ports you defined in the
YAML file.

https://www.jetbrains.com/help/go/attach-to-running-go-processes-with-debugger.html#step-3-create-the-remote-run-debug-configuration-on-the-client-computer

The main test should be something like
`github.com/dolthub/dolt/integration-tests/go-sql-server-driver#TestSingle`, and
this is where you want to set the `DOLT_SQL_SERVER_TEST_DEBUG` environment
variable if you don't have it set in your main environment.

Then Run or Debug the main test (either works fine), and wait for the console
output that indicates the server is waiting for the debugger to attach:

```
API server listening at: [::]:4009
```

Then Debug the remote-debug configuration(s) you have set up. They should
connect to one of the running server processes, at which point they will
continue execution. Breakpoints and other debugger features should work as
normal.

# Caveats and gotchas

* The `dolt` binary run by these tests is whatever is found on your `$PATH`. If
  you make changes locally, you need to rebuild that binary to see them
  reflected.
* For debugging support, `dlv` needs to be on your `$PATH` as well.
* Some tests restart the server. When this happens, they will once again wait
  for a debugger to connect. You'll need to re-invoke the appropriate
  remote-debugger connection for the process to continue.
* These tests are expected to work on Windows as well. Just have `dolt.exe` and
  `dlv.exe` on your windows `%PATH%` and it should all work.
