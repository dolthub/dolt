/*
Package commands provides an API for defining and parsing commands.

Supporting nested commands, options, arguments, etc.  The commands
package also supports a collection of marshallers for presenting
output to the user, including text, JSON, and XML marshallers.
*/

package commands

import (
	"errors"
	"fmt"
	"io"
	"reflect"

	"github.com/ipfs/go-ipfs/path"
	logging "gx/ipfs/QmSpJByNKFX1sCsHBEp3R73FL4NF6FnQTEGyNAXHm2GS52/go-log"
)

var log = logging.Logger("command")

// Function is the type of function that Commands use.
// It reads from the Request, and writes results to the Response.
type Function func(Request, Response)

// Marshaler is a function that takes in a Response, and returns an io.Reader
// (or an error on failure)
type Marshaler func(Response) (io.Reader, error)

// MarshalerMap is a map of Marshaler functions, keyed by EncodingType
// (or an error on failure)
type MarshalerMap map[EncodingType]Marshaler

// HelpText is a set of strings used to generate command help text. The help
// text follows formats similar to man pages, but not exactly the same.
type HelpText struct {
	// required
	Tagline               string            // used in <cmd usage>
	ShortDescription      string            // used in DESCRIPTION
	SynopsisOptionsValues map[string]string // mappings for synopsis generator

	// optional - whole section overrides
	Usage           string // overrides USAGE section
	LongDescription string // overrides DESCRIPTION section
	Options         string // overrides OPTIONS section
	Arguments       string // overrides ARGUMENTS section
	Subcommands     string // overrides SUBCOMMANDS section
	Synopsis        string // overrides SYNOPSIS field
}

// Command is a runnable command, with input arguments and options (flags).
// It can also have Subcommands, to group units of work into sets.
type Command struct {
	Options   []Option
	Arguments []Argument
	PreRun    func(req Request) error

	// Run is the function that processes the request to generate a response.
	// Note that when executing the command over the HTTP API you can only read
	// after writing when using multipart requests. The request body will not be
	// available for reading after the HTTP connection has been written to.
	Run        Function
	PostRun    Function
	Marshalers map[EncodingType]Marshaler
	Helptext   HelpText

	// External denotes that a command is actually an external binary.
	// fewer checks and validations will be performed on such commands.
	External bool

	// Type describes the type of the output of the Command's Run Function.
	// In precise terms, the value of Type is an instance of the return type of
	// the Run Function.
	//
	// ie. If command Run returns &Block{}, then Command.Type == &Block{}
	Type        interface{}
	Subcommands map[string]*Command
}

// ErrNotCallable signals a command that cannot be called.
var ErrNotCallable = ClientError("This command can't be called directly. Try one of its subcommands.")

var ErrNoFormatter = ClientError("This command cannot be formatted to plain text")

var ErrIncorrectType = errors.New("The command returned a value with a different type than expected")

// Call invokes the command for the given Request
func (c *Command) Call(req Request) Response {
	res := NewResponse(req)

	cmds, err := c.Resolve(req.Path())
	if err != nil {
		res.SetError(err, ErrClient)
		return res
	}
	cmd := cmds[len(cmds)-1]

	if cmd.Run == nil {
		res.SetError(ErrNotCallable, ErrClient)
		return res
	}

	err = cmd.CheckArguments(req)
	if err != nil {
		res.SetError(err, ErrClient)
		return res
	}

	err = req.ConvertOptions()
	if err != nil {
		res.SetError(err, ErrClient)
		return res
	}

	cmd.Run(req, res)
	if res.Error() != nil {
		return res
	}

	output := res.Output()
	isChan := false
	actualType := reflect.TypeOf(output)
	if actualType != nil {
		if actualType.Kind() == reflect.Ptr {
			actualType = actualType.Elem()
		}

		// test if output is a channel
		isChan = actualType.Kind() == reflect.Chan
	}

	if isChan {
		if ch, ok := output.(<-chan interface{}); ok {
			output = ch

		} else if ch, ok := output.(chan interface{}); ok {
			output = (<-chan interface{})(ch)
		}
	}

	// If the command specified an output type, ensure the actual value
	// returned is of that type
	if cmd.Type != nil && !isChan {
		expectedType := reflect.TypeOf(cmd.Type)

		if actualType != expectedType {
			res.SetError(ErrIncorrectType, ErrNormal)
			return res
		}
	}

	return res
}

// Resolve returns the subcommands at the given path
func (c *Command) Resolve(pth []string) ([]*Command, error) {
	cmds := make([]*Command, len(pth)+1)
	cmds[0] = c

	cmd := c
	for i, name := range pth {
		cmd = cmd.Subcommand(name)

		if cmd == nil {
			pathS := path.Join(pth[:i])
			return nil, fmt.Errorf("Undefined command: '%s'", pathS)
		}

		cmds[i+1] = cmd
	}

	return cmds, nil
}

// Get resolves and returns the Command addressed by path
func (c *Command) Get(path []string) (*Command, error) {
	cmds, err := c.Resolve(path)
	if err != nil {
		return nil, err
	}
	return cmds[len(cmds)-1], nil
}

// GetOptions returns the options in the given path of commands
func (c *Command) GetOptions(path []string) (map[string]Option, error) {
	options := make([]Option, 0, len(c.Options))

	cmds, err := c.Resolve(path)
	if err != nil {
		return nil, err
	}
	cmds = append(cmds, globalCommand)

	for _, cmd := range cmds {
		options = append(options, cmd.Options...)
	}

	optionsMap := make(map[string]Option)
	for _, opt := range options {
		for _, name := range opt.Names() {
			if _, found := optionsMap[name]; found {
				return nil, fmt.Errorf("Option name '%s' used multiple times", name)
			}

			optionsMap[name] = opt
		}
	}

	return optionsMap, nil
}

func (c *Command) CheckArguments(req Request) error {
	args := req.(*request).arguments

	// count required argument definitions
	numRequired := 0
	for _, argDef := range c.Arguments {
		if argDef.Required {
			numRequired++
		}
	}

	// iterate over the arg definitions
	valueIndex := 0 // the index of the current value (in `args`)
	for i, argDef := range c.Arguments {
		// skip optional argument definitions if there aren't
		// sufficient remaining values
		if len(args)-valueIndex <= numRequired && !argDef.Required ||
			argDef.Type == ArgFile {
			continue
		}

		// the value for this argument definition. can be nil if it
		// wasn't provided by the caller
		v, found := "", false
		if valueIndex < len(args) {
			v = args[valueIndex]
			found = true
			valueIndex++
		}

		// in the case of a non-variadic required argument that supports stdin
		if !found && len(c.Arguments)-1 == i && argDef.SupportsStdin {
			found = true
		}

		err := checkArgValue(v, found, argDef)
		if err != nil {
			return err
		}

		// any additional values are for the variadic arg definition
		if argDef.Variadic && valueIndex < len(args)-1 {
			for _, val := range args[valueIndex:] {
				err := checkArgValue(val, true, argDef)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// Subcommand returns the subcommand with the given id
func (c *Command) Subcommand(id string) *Command {
	return c.Subcommands[id]
}

type CommandVisitor func(*Command)

// Walks tree of all subcommands (including this one)
func (c *Command) Walk(visitor CommandVisitor) {
	visitor(c)
	for _, cm := range c.Subcommands {
		cm.Walk(visitor)
	}
}

func (c *Command) ProcessHelp() {
	c.Walk(func(cm *Command) {
		ht := &cm.Helptext
		if len(ht.LongDescription) == 0 {
			ht.LongDescription = ht.ShortDescription
		}
	})
}

// checkArgValue returns an error if a given arg value is not valid for the
// given Argument
func checkArgValue(v string, found bool, def Argument) error {
	if def.Variadic && def.SupportsStdin {
		return nil
	}

	if !found && def.Required {
		return fmt.Errorf("Argument '%s' is required", def.Name)
	}

	return nil
}

func ClientError(msg string) error {
	return &Error{Code: ErrClient, Message: msg}
}
