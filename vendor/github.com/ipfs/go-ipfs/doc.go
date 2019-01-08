/*
IPFS is a global, versioned, peer-to-peer filesystem

There are sub-packages within the ipfs package for various low-level
utilities, which are in turn assembled into:

core/...:
  The low-level API that gives consumers all the knobs they need,
  which we try hard to keep stable.
shell/...:
  The high-level API that gives consumers easy access to common
  operations (e.g. create a file node from a reader without wrapping
  with metadata). We work really hard to keep this stable.

Then on top of the core/... and shell/... Go APIs, we have:

cmd/...:
  Command-line executables
test/...:
  Integration tests, etc.

To avoid cyclic imports, imports should never pull in higher-level
APIs into a lower-level package.  For example, you could import all of
core and shell from cmd/... or test/..., but you couldn't import any
of shell from core/....
*/
package ipfs
