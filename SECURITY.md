# Security Policy

## Supported Versions

By default, the most recent release of Dolt is the version which is
supported for all security updates. If you need ongoing security
support for an older version of Dolt, please [contact us](https://www.dolthub.com/contact).

## Scope

Dolt is not fully robust against generating `panic`s in its handling of
untrusted input like SQL queries. When Dolt is running as a SQL server, a
`panic` which unwinds back to the connection handler is recovered; it fails the
query or terminates that client connection, but it does not bring down the
server. Applications which embed Dolt as a Go library should `recover()`
themselves if they need to avoid crashing in these cases. Note that `recover()`
only works in the goroutine where the `panic` occurs, so this is only
sufficient for panics which unwind back into your calling code.

For the time being, these panics are treated as bugs, not security issues.
Please report them in GitHub Issues and we will be happy to address them.

A `panic` which cannot be `recover()`ed is still a security issue. In
particular, a `panic` that reaches the top of a goroutine which Dolt owns or
spawned will crash the process regardless of the surrounding recovery.
Please report those to the address below instead.

## Reporting a Vulnerability

Any security issues with Dolt can be reported to [security@dolthub.com](mailto:security@dolthub.com).

Reports will be responded to within one business day. The majority of
our team operates on Pacific Time and on a US holiday schedule.

DoltHub does not currently run a security bounty program for Dolt.
