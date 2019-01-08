# Installing the dolt command line

## Prerequisites

 * Make sure git is installed
 * Setup ssh keys for github authentication: https://help.github.com/articles/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent/
 * Clone this repo and pull the latest
 * Install go: https://golang.org/doc/install
 * Set the environment variable NOMS_VERSION_NEXT=1
 * run: git config --global --add url."ssh://git@github.com".insteadOf "https://github.com"
 * add the go binary installation directory to your Path variable
   * ~/go/bin on OSX
   * %USERPROFILE%/go/bin
 
## Installation

 * Open a command line in the dolt/go directory
 * run: go install github.com/liquidata-inc/ld/dolt/go/cmd/dolt
 
## First commands

 * dolt config --global add user.email YOU@liquidata.co
 * dolt config --global add user.name = YOUR NAME
 
## Setting up a repo

 * make a new directory with nothing in it
 * From the new directory run dolt init
 
## Docs

 * The latest documentation will always be in the tool itself.  run "dolt" to see a list of standard git like commands.  
 run "dolt table" to see the table subcommands.  To get the help for any command use the --help flag after the name of the command.
 * Initial version of the documentation looks like this: https://docs.google.com/document/d/169Zhh_r1hmxZo5V3N_aOx7i6ImJCVBbaB8EBXyH8p3o/edit#
