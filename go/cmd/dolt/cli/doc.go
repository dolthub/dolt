// Package cli provides utilities for the dolt command line.
//
// cli provides:
//
//   * the interface for creating and managing hierarchical dolt commands. These typically have command lines that look like:
//    app command [<options>]
//    app command subcommand [<options>]
//    app command subcommand1 subcommand2 [<options>]
//    etc.
//
//   * Command help and usage printing
//
//   * The interface for writing output to the user
//
//   * Argument parsing utility methods
package cli
