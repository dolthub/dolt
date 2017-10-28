# How to file a GitHub Issue

We use GitHub Issues to log all of our todos and tasks. Here is
[a good guide](https://guides.github.com/features/issues/) for them if you are
unfamiliar.

When logging an issue with go-ipfs, it would be useful if you specified the
below information, if possible. This will help us triage the issues faster.
Please title your issues with the type. For instance:

- "bug: Cannot add file with `ipfs add`"
- "question: How do I use `ipfs block <hash>`?"

Putting the command in backticks helps us parse the natural language description,
and is generally suggested.

This is a _living guide_. If you see anything that should be here and isn't, or
have ideas on improvement, please open a "meta" issue.

### Type

- "bug": If what you are filing is a bug.
- "meta": If it is something about how we run go-ipfs, and not code related in itself.
- "question": If you have a question.
- "test failure": If the tests are failing
- "panic": If it is a severe bug.
- "enhancement ": If you have a feature you would like that enhances go-ipfs.

### Platform

For platform and processor, just run `ipfs version --all` and include that output.

Your platform.

- "Linux"
- "Windows"
- "Mac"
- Etc.

### Processor

Your processor architecture.

- "x86"
- "amd64"
- "Arm"

### Area

What your issue refers to. Multiple items are OK.

- "api"
- "bandwidth reduction"
- "bit swap"
- "blockstore"
- "commands"
- "containers + vms"
- "core"
- "daemon + init"
- "dat"
- "discovery"
- "encryption"
- "files"
- "fuse"
- "gateway"
- "gx"
- "interior"
- "pins"
- "libp2p"
- "merkledag"
- "nat"
- "releases"
- "repo"
- "routing"
- "tools"
- "tracking"
- "unix vs dag"

### Priority

- Critical - System crash, application panic.
- High - The main functionality of the application does not work, API breakage,
  repo format breakage, etc.
- Medium - A non-essential functionality does not work, performance issues, etc.
- Low - An optional functionality does not work.
- Very Low - Translation or documentation mistake. Something that really does
  not matter much but should be noticed for a future release.
