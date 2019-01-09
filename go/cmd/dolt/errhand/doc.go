// Package errhand contains error handling functionality that is designed to be formatted and displayed to the
// user on the command line
//
// The VerboseError interface extends error and provides a method Verbose which should give a more verbose message
// about an error, and it's cause.
//
// DError is a displayable error which implements the VerboseError interface.  It is formatted to be able to provide
// clear and concise error messaging back to the user.
package errhand
