# Building on Windows

## Install Git For Windows

As Git is used by the Go language to download dependencies, you need
to install Git, for example from http://git-scm.com/.

You also must make sure that the directory that contains the Git For
Windows binary is in the Path environment variable. Note that Git For
Windows has a 'git' binary in a 'Git\bin' directory and another one in
a 'Git\cmd' directory. You should only put the 'Git\cmd' directory in
the Path environment variable.

## Install Go

Please install the Go language as explained on
https://golang.org/doc/install.

To properly install Go, you will need to set some environment
variables. We recommend you to set them globally using the Control
Panel, as explained in the documentation above, so that these
environment variables are automatically available in all the possible
environments that you might want to use like Git Bash, Windows's cmd,
Cygwin's terminal, Windows' PowerShell and so on.

You must make sure that the GOROOT environment variable is set and
that the %GOROOT%/bin directory is in the Path environment variable.

The GOPATH environment variable should also be set to a directory that
you have created, and the %GOPATH/bin directory should also be in the
Path environment variable.

## Download go-ipfs and fix Git authentication

Use the following command to download go-ipfs source code:

```
go get -u github.com/ipfs/go-ipfs
```

The above command uses Git to download go-ipfs from its GitHub
repository. If you get authentication problems with Git, you might
want to take a look at
https://help.github.com/articles/caching-your-github-password-in-git/
and use the suggested solution:

```
git config --global credential.helper wincred
```

## Choose the way you want to proceed

Now there are two ways to download, install the dependencies and to
build go-ipfs:

1) There is the "Manual Way", where you don't need to install anymore
software except the dependencies, but you have a number of commands to
type.

2) There is a way by installing 'make' through Cygwin and using it to
do nearly everything. We call this way the "Cygwin Way". It may take
much more time, because installing Cygwin can take a lot of time, but
after that it might be easier as many procedures are just a 'make'
command away.

So use the next steps below that start with "Manual Way" if that's the
way you want, otherwise scroll down a bit and use the "Cygwin Way"
steps below.

## Manual Way: download and install dependencies

The following commands should download or update go-ipfs dependencies
and then install them:

```
go get -u github.com/whyrusleeping/gx
go get -u github.com/whyrusleeping/gx-go
cd %GOPATH%/src/github.com/ipfs/go-ipfs
gx --verbose install --global
```

## Manual Way: build go-ipfs

To actually build go-ipfs, first go to the cmd/ipfs directory:

```
cd cmd\ipfs
```

Then get the current Git commit:

```
git rev-parse --short HEAD
```

It will output a small number of hex characters that you must pass to
the actual build command (replace XXXXXXX with these characters):

```
go install -ldflags="-X "github.com/ipfs/go-ipfs/repo/config".CurrentCommit=XXXXXXX"
```

After that ipfs should have been built and should be available in
"%GOPATH%\bin".

You can check that the ipfs you built has the right version using:

```
ipfs version --commit
```

It should output something like "ipfs version 0.4.0-dev-XXXXXXX" where
XXXXXXX is the current commit that you passed to the build command.

## Cygwin way: install Cygwin

Install Cygwin as explained in the Cygwin documentation:

http://cygwin.com/install.html

By default Cygwin will not install 'make', so you should click on the
"Devel" category during the Cygwin installation process and then check
the 'make' package.

## Cygwin way: build go-ipfs

To build go-ipfs using Cygwin you just need to open a Cygwin Terminal
and then type the following commands:

```
cd $GOPATH/src/github.com/ipfs/go-ipfs
make install
```

After that ipfs should have been built and should be available in
"%GOPATH%\bin".

You can check that the ipfs you built has the right version using:

```
ipfs version --commit
```
