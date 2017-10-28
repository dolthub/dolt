package config

// CurrentCommit is the current git commit, this is set as a ldflag in the Makefile
var CurrentCommit string

// CurrentVersionNumber is the current application's version literal
const CurrentVersionNumber = "0.4.12-dev"

const ApiVersion = "/go-ipfs/" + CurrentVersionNumber + "/"
