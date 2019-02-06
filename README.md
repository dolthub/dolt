# Installing the dolt command line

## Prerequisites

 * Make sure git is installed
 * Setup ssh keys for github authentication: https://help.github.com/articles/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent/
 * Clone this repo and pull the latest
 * Install go: https://golang.org/doc/install
 * Set the environment variable NOMS_VERSION_NEXT=1
 * run: git config --global --add url."ssh://git@github.com".insteadOf "https://github.com"
 * add the go binary installation directory to your Path variable
   * on OSX / linux
     * Configure your environment using .bashrc / .bash_profile if you haven't already: https://scriptingosx.com/2017/04/about-bash_profile-and-bashrc-on-macos/
     * Add ~/go/bin to the PATH variable (export PATH=$HOME/go/bin:$PATH).
     * The helper function can also be added to make installation easier:

``` bash
dolt_install() {
  pushd ~/go/src/github.com/liquidata-inc/ld/dolt/go/cmd/dolt
  GO111MODULE=on go install .
  popd
}
```

   * on Windows
     * I dont know.  Do whatever windows stuff you gotta do so that %USERPROFILE%/go/bin is part of the path
 
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
