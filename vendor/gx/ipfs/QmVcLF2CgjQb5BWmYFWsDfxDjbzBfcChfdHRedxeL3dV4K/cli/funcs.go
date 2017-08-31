package cli

// An action to execute when the bash-completion flag is set
type BashCompleteFunc func(*Context)

// An action to execute before any subcommands are run, but after the context is ready
// If a non-nil error is returned, no subcommands are run
type BeforeFunc func(*Context) error

// An action to execute after any subcommands are run, but after the subcommand has finished
// It is run even if Action() panics
type AfterFunc func(*Context) error

// The action to execute when no subcommands are specified
type ActionFunc func(*Context) error

// Execute this function if the proper command cannot be found
type CommandNotFoundFunc func(*Context, string)

// Execute this function if an usage error occurs. This is useful for displaying
// customized usage error messages.  This function is able to replace the
// original error messages.  If this function is not set, the "Incorrect usage"
// is displayed and the execution is interrupted.
type OnUsageErrorFunc func(context *Context, err error, isSubcommand bool) error
