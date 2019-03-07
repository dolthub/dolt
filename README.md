# Installing the dolt command line

## Prerequisites

* Make sure git is installed
* Setup ssh keys for github authentication: https://help.github.com/articles/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent/
* Install go: https://golang.org/doc/install
* run: git config --global --add url."ssh://git@github.com".insteadOf "https://github.com"
* Clone this repo and pull the latest
  * For compatibility with some tools, it's recommended to clone the repo into a specific directory structure in your go workspace. Specifically, clone source code into `$GOPATH/src/github.com/liquidata-inc/`. This directory structure should become unnecssary at some point in the future, but is necessary for some tools to work properly. See https://golang.org/doc/code.html#Workspaces for details.
* Configure your environment with important variables. Put these in your .bash_profile / .bashrc
  * Set your `GOPATH` environment variable: 
      *     export GOPATH=`go env GOPATH`
      * If your go workspace is somewhere other than the default of `~/go`, set `GOPATH` manually via an export directive in your .bash_profile, e.g. `export GOPATH=/workspaces/liquidata/go`
  * `export NOMS_VERSION_NEXT=1`
  * Add the go binary installation directory to your `PATH` variable (`export PATH=$GOPATH/bin:$PATH`).
* This helper function can also be added to make installation easier:

``` bash
dolt_install() {
  pushd ~/go/src/github.com/liquidata-inc/ld/dolt/go/cmd/dolt
  GO111MODULE=on go install .
  popd
}
```

* Windows installation
  * Do whatever windows stuff you gotta do so that %USERPROFILE%/go/bin is part of the path
 
## Installation

* If you setup dolt_install
  * run: dolt_install
* If not
  * Open a command line in the dolt/go directory
  * run: GO111MODULE=on go install . 
 
 
## First commands

* dolt config --global --add user.email YOU@liquidata.co
* dolt config --global --add user.name "YOUR NAME"
 
## Setting up a repo

* make a new directory with nothing in it
* From the new directory run dolt init
 
## Docs

* The latest documentation will always be in the tool itself.  run "dolt" to see a list of standard git like commands.  
run "dolt table" to see the table subcommands.  To get the help for any command use the --help flag after the name of the command.
* Initial version of the documentation looks like this: https://docs.google.com/document/d/169Zhh_r1hmxZo5V3N_aOx7i6ImJCVBbaB8EBXyH8p3o/edit#
