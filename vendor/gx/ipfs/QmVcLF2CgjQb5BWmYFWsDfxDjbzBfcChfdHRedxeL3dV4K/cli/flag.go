package cli

import (
	"flag"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// This flag enables bash-completion for all commands and subcommands
var BashCompletionFlag = BoolFlag{
	Name:   "generate-bash-completion",
	Hidden: true,
}

// This flag prints the version for the application
var VersionFlag = BoolFlag{
	Name:  "version, v",
	Usage: "print the version",
}

// This flag prints the help for all commands and subcommands
// Set to the zero value (BoolFlag{}) to disable flag -- keeps subcommand
// unless HideHelp is set to true)
var HelpFlag = BoolFlag{
	Name:  "help, h",
	Usage: "show help",
}

// Flag is a common interface related to parsing flags in cli.
// For more advanced flag parsing techniques, it is recommended that
// this interface be implemented.
type Flag interface {
	fmt.Stringer
	// Apply Flag settings to the given flag set
	Apply(*flag.FlagSet)
	GetName() string
}

func flagSet(name string, flags []Flag) *flag.FlagSet {
	set := flag.NewFlagSet(name, flag.ContinueOnError)

	for _, f := range flags {
		f.Apply(set)
	}
	return set
}

func eachName(longName string, fn func(string)) {
	parts := strings.Split(longName, ",")
	for _, name := range parts {
		name = strings.Trim(name, " ")
		fn(name)
	}
}

// Generic is a generic parseable type identified by a specific flag
type Generic interface {
	Set(value string) error
	String() string
}

// GenericFlag is the flag type for types implementing Generic
type GenericFlag struct {
	Name   string
	Value  Generic
	Usage  string
	EnvVar string
	Hidden bool
}

// String returns the string representation of the generic flag to display the
// help text to the user (uses the String() method of the generic flag to show
// the value)
func (f GenericFlag) String() string {
	placeholder, usage := unquoteUsage(f.Usage)
	return withEnvHint(f.EnvVar, fmt.Sprintf("%s %v\t%v", prefixedNames(f.Name, placeholder), f.FormatValueHelp(), usage))
}

func (f GenericFlag) FormatValueHelp() string {
	if f.Value == nil {
		return ""
	}
	s := f.Value.String()
	if len(s) == 0 {
		return ""
	}
	return fmt.Sprintf("\"%s\"", s)
}

// Apply takes the flagset and calls Set on the generic flag with the value
// provided by the user for parsing by the flag
func (f GenericFlag) Apply(set *flag.FlagSet) {
	val := f.Value
	if f.EnvVar != "" {
		for _, envVar := range strings.Split(f.EnvVar, ",") {
			envVar = strings.TrimSpace(envVar)
			if envVal := os.Getenv(envVar); envVal != "" {
				val.Set(envVal)
				break
			}
		}
	}

	eachName(f.Name, func(name string) {
		set.Var(f.Value, name, f.Usage)
	})
}

func (f GenericFlag) GetName() string {
	return f.Name
}

// StringSlice is an opaque type for []string to satisfy flag.Value
type StringSlice []string

// Set appends the string value to the list of values
func (f *StringSlice) Set(value string) error {
	*f = append(*f, value)
	return nil
}

// String returns a readable representation of this value (for usage defaults)
func (f *StringSlice) String() string {
	return fmt.Sprintf("%s", *f)
}

// Value returns the slice of strings set by this flag
func (f *StringSlice) Value() []string {
	return *f
}

// StringSlice is a string flag that can be specified multiple times on the
// command-line
type StringSliceFlag struct {
	Name   string
	Value  *StringSlice
	Usage  string
	EnvVar string
	Hidden bool
}

// String returns the usage
func (f StringSliceFlag) String() string {
	firstName := strings.Trim(strings.Split(f.Name, ",")[0], " ")
	pref := prefixFor(firstName)
	placeholder, usage := unquoteUsage(f.Usage)
	return withEnvHint(f.EnvVar, fmt.Sprintf("%s [%v]\t%v", prefixedNames(f.Name, placeholder), pref+firstName+" option "+pref+firstName+" option", usage))
}

// Apply populates the flag given the flag set and environment
func (f StringSliceFlag) Apply(set *flag.FlagSet) {
	if f.EnvVar != "" {
		for _, envVar := range strings.Split(f.EnvVar, ",") {
			envVar = strings.TrimSpace(envVar)
			if envVal := os.Getenv(envVar); envVal != "" {
				newVal := &StringSlice{}
				for _, s := range strings.Split(envVal, ",") {
					s = strings.TrimSpace(s)
					newVal.Set(s)
				}
				f.Value = newVal
				break
			}
		}
	}

	eachName(f.Name, func(name string) {
		if f.Value == nil {
			f.Value = &StringSlice{}
		}
		set.Var(f.Value, name, f.Usage)
	})
}

func (f StringSliceFlag) GetName() string {
	return f.Name
}

// StringSlice is an opaque type for []int to satisfy flag.Value
type IntSlice []int

// Set parses the value into an integer and appends it to the list of values
func (f *IntSlice) Set(value string) error {
	tmp, err := strconv.Atoi(value)
	if err != nil {
		return err
	} else {
		*f = append(*f, tmp)
	}
	return nil
}

// String returns a readable representation of this value (for usage defaults)
func (f *IntSlice) String() string {
	return fmt.Sprintf("%d", *f)
}

// Value returns the slice of ints set by this flag
func (f *IntSlice) Value() []int {
	return *f
}

// IntSliceFlag is an int flag that can be specified multiple times on the
// command-line
type IntSliceFlag struct {
	Name   string
	Value  *IntSlice
	Usage  string
	EnvVar string
	Hidden bool
}

// String returns the usage
func (f IntSliceFlag) String() string {
	firstName := strings.Trim(strings.Split(f.Name, ",")[0], " ")
	pref := prefixFor(firstName)
	placeholder, usage := unquoteUsage(f.Usage)
	return withEnvHint(f.EnvVar, fmt.Sprintf("%s [%v]\t%v", prefixedNames(f.Name, placeholder), pref+firstName+" option "+pref+firstName+" option", usage))
}

// Apply populates the flag given the flag set and environment
func (f IntSliceFlag) Apply(set *flag.FlagSet) {
	if f.EnvVar != "" {
		for _, envVar := range strings.Split(f.EnvVar, ",") {
			envVar = strings.TrimSpace(envVar)
			if envVal := os.Getenv(envVar); envVal != "" {
				newVal := &IntSlice{}
				for _, s := range strings.Split(envVal, ",") {
					s = strings.TrimSpace(s)
					err := newVal.Set(s)
					if err != nil {
						fmt.Fprintf(os.Stderr, err.Error())
					}
				}
				f.Value = newVal
				break
			}
		}
	}

	eachName(f.Name, func(name string) {
		if f.Value == nil {
			f.Value = &IntSlice{}
		}
		set.Var(f.Value, name, f.Usage)
	})
}

func (f IntSliceFlag) GetName() string {
	return f.Name
}

// BoolFlag is a switch that defaults to false
type BoolFlag struct {
	Name        string
	Usage       string
	EnvVar      string
	Destination *bool
	Hidden      bool
}

// String returns a readable representation of this value (for usage defaults)
func (f BoolFlag) String() string {
	placeholder, usage := unquoteUsage(f.Usage)
	return withEnvHint(f.EnvVar, fmt.Sprintf("%s\t%v", prefixedNames(f.Name, placeholder), usage))
}

// Apply populates the flag given the flag set and environment
func (f BoolFlag) Apply(set *flag.FlagSet) {
	val := false
	if f.EnvVar != "" {
		for _, envVar := range strings.Split(f.EnvVar, ",") {
			envVar = strings.TrimSpace(envVar)
			if envVal := os.Getenv(envVar); envVal != "" {
				envValBool, err := strconv.ParseBool(envVal)
				if err == nil {
					val = envValBool
				}
				break
			}
		}
	}

	eachName(f.Name, func(name string) {
		if f.Destination != nil {
			set.BoolVar(f.Destination, name, val, f.Usage)
			return
		}
		set.Bool(name, val, f.Usage)
	})
}

func (f BoolFlag) GetName() string {
	return f.Name
}

// BoolTFlag this represents a boolean flag that is true by default, but can
// still be set to false by --some-flag=false
type BoolTFlag struct {
	Name        string
	Usage       string
	EnvVar      string
	Destination *bool
	Hidden      bool
}

// String returns a readable representation of this value (for usage defaults)
func (f BoolTFlag) String() string {
	placeholder, usage := unquoteUsage(f.Usage)
	return withEnvHint(f.EnvVar, fmt.Sprintf("%s\t%v", prefixedNames(f.Name, placeholder), usage))
}

// Apply populates the flag given the flag set and environment
func (f BoolTFlag) Apply(set *flag.FlagSet) {
	val := true
	if f.EnvVar != "" {
		for _, envVar := range strings.Split(f.EnvVar, ",") {
			envVar = strings.TrimSpace(envVar)
			if envVal := os.Getenv(envVar); envVal != "" {
				envValBool, err := strconv.ParseBool(envVal)
				if err == nil {
					val = envValBool
					break
				}
			}
		}
	}

	eachName(f.Name, func(name string) {
		if f.Destination != nil {
			set.BoolVar(f.Destination, name, val, f.Usage)
			return
		}
		set.Bool(name, val, f.Usage)
	})
}

func (f BoolTFlag) GetName() string {
	return f.Name
}

// StringFlag represents a flag that takes as string value
type StringFlag struct {
	Name        string
	Value       string
	Usage       string
	EnvVar      string
	Destination *string
	Hidden      bool
}

// String returns the usage
func (f StringFlag) String() string {
	placeholder, usage := unquoteUsage(f.Usage)
	return withEnvHint(f.EnvVar, fmt.Sprintf("%s %v\t%v", prefixedNames(f.Name, placeholder), f.FormatValueHelp(), usage))
}

func (f StringFlag) FormatValueHelp() string {
	s := f.Value
	if len(s) == 0 {
		return ""
	}
	return fmt.Sprintf("\"%s\"", s)
}

// Apply populates the flag given the flag set and environment
func (f StringFlag) Apply(set *flag.FlagSet) {
	if f.EnvVar != "" {
		for _, envVar := range strings.Split(f.EnvVar, ",") {
			envVar = strings.TrimSpace(envVar)
			if envVal := os.Getenv(envVar); envVal != "" {
				f.Value = envVal
				break
			}
		}
	}

	eachName(f.Name, func(name string) {
		if f.Destination != nil {
			set.StringVar(f.Destination, name, f.Value, f.Usage)
			return
		}
		set.String(name, f.Value, f.Usage)
	})
}

func (f StringFlag) GetName() string {
	return f.Name
}

// IntFlag is a flag that takes an integer
// Errors if the value provided cannot be parsed
type IntFlag struct {
	Name        string
	Value       int
	Usage       string
	EnvVar      string
	Destination *int
	Hidden      bool
}

// String returns the usage
func (f IntFlag) String() string {
	placeholder, usage := unquoteUsage(f.Usage)
	return withEnvHint(f.EnvVar, fmt.Sprintf("%s \"%v\"\t%v", prefixedNames(f.Name, placeholder), f.Value, usage))
}

// Apply populates the flag given the flag set and environment
func (f IntFlag) Apply(set *flag.FlagSet) {
	if f.EnvVar != "" {
		for _, envVar := range strings.Split(f.EnvVar, ",") {
			envVar = strings.TrimSpace(envVar)
			if envVal := os.Getenv(envVar); envVal != "" {
				envValInt, err := strconv.ParseInt(envVal, 0, 64)
				if err == nil {
					f.Value = int(envValInt)
					break
				}
			}
		}
	}

	eachName(f.Name, func(name string) {
		if f.Destination != nil {
			set.IntVar(f.Destination, name, f.Value, f.Usage)
			return
		}
		set.Int(name, f.Value, f.Usage)
	})
}

func (f IntFlag) GetName() string {
	return f.Name
}

// DurationFlag is a flag that takes a duration specified in Go's duration
// format: https://golang.org/pkg/time/#ParseDuration
type DurationFlag struct {
	Name        string
	Value       time.Duration
	Usage       string
	EnvVar      string
	Destination *time.Duration
	Hidden      bool
}

// String returns a readable representation of this value (for usage defaults)
func (f DurationFlag) String() string {
	placeholder, usage := unquoteUsage(f.Usage)
	return withEnvHint(f.EnvVar, fmt.Sprintf("%s \"%v\"\t%v", prefixedNames(f.Name, placeholder), f.Value, usage))
}

// Apply populates the flag given the flag set and environment
func (f DurationFlag) Apply(set *flag.FlagSet) {
	if f.EnvVar != "" {
		for _, envVar := range strings.Split(f.EnvVar, ",") {
			envVar = strings.TrimSpace(envVar)
			if envVal := os.Getenv(envVar); envVal != "" {
				envValDuration, err := time.ParseDuration(envVal)
				if err == nil {
					f.Value = envValDuration
					break
				}
			}
		}
	}

	eachName(f.Name, func(name string) {
		if f.Destination != nil {
			set.DurationVar(f.Destination, name, f.Value, f.Usage)
			return
		}
		set.Duration(name, f.Value, f.Usage)
	})
}

func (f DurationFlag) GetName() string {
	return f.Name
}

// Float64Flag is a flag that takes an float value
// Errors if the value provided cannot be parsed
type Float64Flag struct {
	Name        string
	Value       float64
	Usage       string
	EnvVar      string
	Destination *float64
	Hidden      bool
}

// String returns the usage
func (f Float64Flag) String() string {
	placeholder, usage := unquoteUsage(f.Usage)
	return withEnvHint(f.EnvVar, fmt.Sprintf("%s \"%v\"\t%v", prefixedNames(f.Name, placeholder), f.Value, usage))
}

// Apply populates the flag given the flag set and environment
func (f Float64Flag) Apply(set *flag.FlagSet) {
	if f.EnvVar != "" {
		for _, envVar := range strings.Split(f.EnvVar, ",") {
			envVar = strings.TrimSpace(envVar)
			if envVal := os.Getenv(envVar); envVal != "" {
				envValFloat, err := strconv.ParseFloat(envVal, 10)
				if err == nil {
					f.Value = float64(envValFloat)
				}
			}
		}
	}

	eachName(f.Name, func(name string) {
		if f.Destination != nil {
			set.Float64Var(f.Destination, name, f.Value, f.Usage)
			return
		}
		set.Float64(name, f.Value, f.Usage)
	})
}

func (f Float64Flag) GetName() string {
	return f.Name
}

func visibleFlags(fl []Flag) []Flag {
	visible := []Flag{}
	for _, flag := range fl {
		if !reflect.ValueOf(flag).FieldByName("Hidden").Bool() {
			visible = append(visible, flag)
		}
	}
	return visible
}

func prefixFor(name string) (prefix string) {
	if len(name) == 1 {
		prefix = "-"
	} else {
		prefix = "--"
	}

	return
}

// Returns the placeholder, if any, and the unquoted usage string.
func unquoteUsage(usage string) (string, string) {
	for i := 0; i < len(usage); i++ {
		if usage[i] == '`' {
			for j := i + 1; j < len(usage); j++ {
				if usage[j] == '`' {
					name := usage[i+1 : j]
					usage = usage[:i] + name + usage[j+1:]
					return name, usage
				}
			}
			break
		}
	}
	return "", usage
}

func prefixedNames(fullName, placeholder string) string {
	var prefixed string
	parts := strings.Split(fullName, ",")
	for i, name := range parts {
		name = strings.Trim(name, " ")
		prefixed += prefixFor(name) + name
		if placeholder != "" {
			prefixed += " " + placeholder
		}
		if i < len(parts)-1 {
			prefixed += ", "
		}
	}
	return prefixed
}

func withEnvHint(envVar, str string) string {
	envText := ""
	if envVar != "" {
		prefix := "$"
		suffix := ""
		sep := ", $"
		if runtime.GOOS == "windows" {
			prefix = "%"
			suffix = "%"
			sep = "%, %"
		}
		envText = fmt.Sprintf(" [%s%s%s]", prefix, strings.Join(strings.Split(envVar, ","), sep), suffix)
	}
	return str + envText
}
