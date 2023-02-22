# Windows support

Dolt is tested and supported on windows! If you find any problems
specific to Windows, please file an
[issue](https://github.com/dolthub/dolt/issues/) and let us know.

## Installation

Download the latest Microsoft Installer (`.msi` file) in
[releases](https://github.com/dolthub/dolt/releases) and run it.

Package manager releases coming soon!

## Environment

Dolt runs best under the Windows Subsystem for Linux, or WSL. But it
should also work fine with `cmd.exe` or `powershell`. If you find this
isn't true, please file an
[issue](https://github.com/dolthub/dolt/issues/) and let us know.

WSL 2 currently has [known
bugs](https://github.com/dolthub/dolt/issues/992), so we recommend
using WSL 1 for now. Or if you do use WSL 2, we recommend using the
Linux `dolt` binary, rather than the Windows `dolt.exe` binary.
