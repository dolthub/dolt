package commands

type ArgumentType int

const (
	ArgString ArgumentType = iota
	ArgFile
)

type Argument struct {
	Name          string
	Type          ArgumentType
	Required      bool // error if no value is specified
	Variadic      bool // unlimited values can be specfied
	SupportsStdin bool // can accept stdin as a value
	Recursive     bool // supports recursive file adding (with '-r' flag)
	Description   string
}

func StringArg(name string, required, variadic bool, description string) Argument {
	return Argument{
		Name:        name,
		Type:        ArgString,
		Required:    required,
		Variadic:    variadic,
		Description: description,
	}
}

func FileArg(name string, required, variadic bool, description string) Argument {
	return Argument{
		Name:        name,
		Type:        ArgFile,
		Required:    required,
		Variadic:    variadic,
		Description: description,
	}
}

// TODO: modifiers might need a different API?
//       e.g. passing enum values into arg constructors variadically
//       (`FileArg("file", ArgRequired, ArgStdin, ArgRecursive)`)

func (a Argument) EnableStdin() Argument {
	a.SupportsStdin = true
	return a
}

func (a Argument) EnableRecursive() Argument {
	if a.Type != ArgFile {
		panic("Only FileArgs can enable recursive")
	}

	a.Recursive = true
	return a
}
