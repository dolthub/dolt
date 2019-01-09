// Package commands contains the command functions executed based on the dolt subcommand specified on the command line.
//
// The goal of the code within this package and sub packages is to be a thin client, handling user input and displaying
// formatted output to the user.  The hope is that someone could take the library code, and implement the
// same functionality contained within the command line with very little work beyond providing a new user interface.
package commands
