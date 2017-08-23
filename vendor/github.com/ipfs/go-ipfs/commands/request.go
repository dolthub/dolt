package commands

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"
	"time"

	"github.com/ipfs/go-ipfs/commands/files"
	"github.com/ipfs/go-ipfs/core"
	"github.com/ipfs/go-ipfs/repo/config"
	u "gx/ipfs/QmSU6eubNdhXjFBJBSksTp8kv8YRub8mGAPv8tVJHmL2EU/go-ipfs-util"
)

type OptMap map[string]interface{}

type Context struct {
	Online     bool
	ConfigRoot string
	ReqLog     *ReqLog

	config     *config.Config
	LoadConfig func(path string) (*config.Config, error)

	node          *core.IpfsNode
	ConstructNode func() (*core.IpfsNode, error)
}

// GetConfig returns the config of the current Command exection
// context. It may load it with the providied function.
func (c *Context) GetConfig() (*config.Config, error) {
	var err error
	if c.config == nil {
		if c.LoadConfig == nil {
			return nil, errors.New("nil LoadConfig function")
		}
		c.config, err = c.LoadConfig(c.ConfigRoot)
	}
	return c.config, err
}

// GetNode returns the node of the current Command exection
// context. It may construct it with the provided function.
func (c *Context) GetNode() (*core.IpfsNode, error) {
	var err error
	if c.node == nil {
		if c.ConstructNode == nil {
			return nil, errors.New("nil ConstructNode function")
		}
		c.node, err = c.ConstructNode()
	}
	return c.node, err
}

// NodeWithoutConstructing returns the underlying node variable
// so that clients may close it.
func (c *Context) NodeWithoutConstructing() *core.IpfsNode {
	return c.node
}

// Request represents a call to a command from a consumer
type Request interface {
	Path() []string
	Option(name string) *OptionValue
	Options() OptMap
	SetOption(name string, val interface{})
	SetOptions(opts OptMap) error
	Arguments() []string
	StringArguments() []string
	SetArguments([]string)
	Files() files.File
	SetFiles(files.File)
	Context() context.Context
	SetRootContext(context.Context) error
	InvocContext() *Context
	SetInvocContext(Context)
	Command() *Command
	Values() map[string]interface{}
	Stdin() io.Reader
	VarArgs(func(string) error) error

	ConvertOptions() error
}

type request struct {
	path       []string
	options    OptMap
	arguments  []string
	files      files.File
	cmd        *Command
	ctx        Context
	rctx       context.Context
	optionDefs map[string]Option
	values     map[string]interface{}
	stdin      io.Reader
}

// Path returns the command path of this request
func (r *request) Path() []string {
	return r.path
}

// Option returns the value of the option for given name.
func (r *request) Option(name string) *OptionValue {
	// find the option with the specified name
	option, found := r.optionDefs[name]
	if !found {
		return nil
	}

	// try all the possible names, break if we find a value
	for _, n := range option.Names() {
		val, found := r.options[n]
		if found {
			return &OptionValue{val, found, option}
		}
	}

	return &OptionValue{option.DefaultVal(), false, option}
}

// Options returns a copy of the option map
func (r *request) Options() OptMap {
	output := make(OptMap)
	for k, v := range r.options {
		output[k] = v
	}
	return output
}

func (r *request) SetRootContext(ctx context.Context) error {
	ctx, err := getContext(ctx, r)
	if err != nil {
		return err
	}

	r.rctx = ctx
	return nil
}

// SetOption sets the value of the option for given name.
func (r *request) SetOption(name string, val interface{}) {
	// find the option with the specified name
	option, found := r.optionDefs[name]
	if !found {
		return
	}

	// try all the possible names, if we already have a value then set over it
	for _, n := range option.Names() {
		_, found := r.options[n]
		if found {
			r.options[n] = val
			return
		}
	}

	r.options[name] = val
}

// SetOptions sets the option values, unsetting any values that were previously set
func (r *request) SetOptions(opts OptMap) error {
	r.options = opts
	return r.ConvertOptions()
}

func (r *request) StringArguments() []string {
	return r.arguments
}

// Arguments returns the arguments slice
func (r *request) Arguments() []string {
	if r.haveVarArgsFromStdin() {
		err := r.VarArgs(func(s string) error {
			r.arguments = append(r.arguments, s)
			return nil
		})
		if err != nil && err != io.EOF {
			log.Error(err)
		}
	}

	return r.arguments
}

func (r *request) SetArguments(args []string) {
	r.arguments = args
}

func (r *request) Files() files.File {
	return r.files
}

func (r *request) SetFiles(f files.File) {
	r.files = f
}

func (r *request) Context() context.Context {
	return r.rctx
}

func (r *request) haveVarArgsFromStdin() bool {
	// we expect varargs if we have a string argument that supports stdin
	// and not arguments to satisfy it
	if len(r.cmd.Arguments) == 0 {
		return false
	}

	last := r.cmd.Arguments[len(r.cmd.Arguments)-1]
	return last.SupportsStdin && last.Type == ArgString && (last.Required || last.Variadic) &&
		len(r.arguments) < len(r.cmd.Arguments)
}

// VarArgs can be used when you want string arguments as input
// and also want to be able to handle them in a streaming fashion
func (r *request) VarArgs(f func(string) error) error {
	if len(r.arguments) >= len(r.cmd.Arguments) {
		for _, arg := range r.arguments[len(r.cmd.Arguments)-1:] {
			err := f(arg)
			if err != nil {
				return err
			}
		}

		return nil
	}

	if r.files == nil {
		log.Warning("expected more arguments from stdin")
		return nil
	}

	fi, err := r.files.NextFile()
	if err != nil {
		return err
	}

	var any bool
	scan := bufio.NewScanner(fi)
	for scan.Scan() {
		any = true
		err := f(scan.Text())
		if err != nil {
			return err
		}
	}
	if !any {
		return f("")
	}

	return nil
}

func getContext(base context.Context, req Request) (context.Context, error) {
	tout, found, err := req.Option("timeout").String()
	if err != nil {
		return nil, fmt.Errorf("error parsing timeout option: %s", err)
	}

	var ctx context.Context
	if found {
		duration, err := time.ParseDuration(tout)
		if err != nil {
			return nil, fmt.Errorf("error parsing timeout option: %s", err)
		}

		tctx, _ := context.WithTimeout(base, duration)
		ctx = tctx
	} else {
		cctx, _ := context.WithCancel(base)
		ctx = cctx
	}
	return ctx, nil
}

func (r *request) InvocContext() *Context {
	return &r.ctx
}

func (r *request) SetInvocContext(ctx Context) {
	r.ctx = ctx
}

func (r *request) Command() *Command {
	return r.cmd
}

type converter func(string) (interface{}, error)

var converters = map[reflect.Kind]converter{
	Bool: func(v string) (interface{}, error) {
		if v == "" {
			return true, nil
		}
		return strconv.ParseBool(v)
	},
	Int: func(v string) (interface{}, error) {
		val, err := strconv.ParseInt(v, 0, 32)
		if err != nil {
			return nil, err
		}
		return int(val), err
	},
	Uint: func(v string) (interface{}, error) {
		val, err := strconv.ParseUint(v, 0, 32)
		if err != nil {
			return nil, err
		}
		return int(val), err
	},
	Float: func(v string) (interface{}, error) {
		return strconv.ParseFloat(v, 64)
	},
}

func (r *request) Values() map[string]interface{} {
	return r.values
}

func (r *request) Stdin() io.Reader {
	return r.stdin
}

func (r *request) ConvertOptions() error {
	for k, v := range r.options {
		opt, ok := r.optionDefs[k]
		if !ok {
			continue
		}

		kind := reflect.TypeOf(v).Kind()
		if kind != opt.Type() {
			if kind == String {
				convert := converters[opt.Type()]
				str, ok := v.(string)
				if !ok {
					return u.ErrCast()
				}
				val, err := convert(str)
				if err != nil {
					value := fmt.Sprintf("value '%v'", v)
					if len(str) == 0 {
						value = "empty value"
					}
					return fmt.Errorf("Could not convert %s to type '%s' (for option '-%s')",
						value, opt.Type().String(), k)
				}
				r.options[k] = val

			} else {
				return fmt.Errorf("Option '%s' should be type '%s', but got type '%s'",
					k, opt.Type().String(), kind.String())
			}
		} else {
			r.options[k] = v
		}

		for _, name := range opt.Names() {
			if _, ok := r.options[name]; name != k && ok {
				return fmt.Errorf("Duplicate command options were provided ('%s' and '%s')",
					k, name)
			}
		}
	}

	return nil
}

// NewEmptyRequest initializes an empty request
func NewEmptyRequest() (Request, error) {
	return NewRequest(nil, nil, nil, nil, nil, nil)
}

// NewRequest returns a request initialized with given arguments
// An non-nil error will be returned if the provided option values are invalid
func NewRequest(path []string, opts OptMap, args []string, file files.File, cmd *Command, optDefs map[string]Option) (Request, error) {
	if opts == nil {
		opts = make(OptMap)
	}
	if optDefs == nil {
		optDefs = make(map[string]Option)
	}

	ctx := Context{}
	values := make(map[string]interface{})
	req := &request{
		path:       path,
		options:    opts,
		arguments:  args,
		files:      file,
		cmd:        cmd,
		ctx:        ctx,
		optionDefs: optDefs,
		values:     values,
		stdin:      os.Stdin,
	}
	err := req.ConvertOptions()
	if err != nil {
		return nil, err
	}

	return req, nil
}
