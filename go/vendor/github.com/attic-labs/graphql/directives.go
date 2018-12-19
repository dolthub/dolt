package graphql

const (
	DirectiveLocationQuery              = "QUERY"
	DirectiveLocationMutation           = "MUTATION"
	DirectiveLocationSubscription       = "SUBSCRIPTION"
	DirectiveLocationField              = "FIELD"
	DirectiveLocationFragmentDefinition = "FRAGMENT_DEFINITION"
	DirectiveLocationFragmentSpread     = "FRAGMENT_SPREAD"
	DirectiveLocationInlineFragment     = "INLINE_FRAGMENT"
)

// Directive structs are used by the GraphQL runtime as a way of modifying execution
// behavior. Type system creators will usually not create these directly.
type Directive struct {
	Name        string      `json:"name"`
	Description string      `json:"description"`
	Locations   []string    `json:"locations"`
	Args        []*Argument `json:"args"`

	err error
}

// DirectiveConfig options for creating a new GraphQLDirective
type DirectiveConfig struct {
	Name        string              `json:"name"`
	Description string              `json:"description"`
	Locations   []string            `json:"locations"`
	Args        FieldConfigArgument `json:"args"`
}

func NewDirective(config DirectiveConfig) *Directive {
	dir := &Directive{}

	// Ensure directive is named
	err := invariant(config.Name != "", "Directive must be named.")
	if err != nil {
		dir.err = err
		return dir
	}

	// Ensure directive name is valid
	err = assertValidName(config.Name)
	if err != nil {
		dir.err = err
		return dir
	}

	// Ensure locations are provided for directive
	err = invariant(len(config.Locations) > 0, "Must provide locations for directive.")
	if err != nil {
		dir.err = err
		return dir
	}

	args := []*Argument{}

	for argName, argConfig := range config.Args {
		err := assertValidName(argName)
		if err != nil {
			dir.err = err
			return dir
		}
		args = append(args, &Argument{
			PrivateName:        argName,
			PrivateDescription: argConfig.Description,
			Type:               argConfig.Type,
			DefaultValue:       argConfig.DefaultValue,
		})
	}

	dir.Name = config.Name
	dir.Description = config.Description
	dir.Locations = config.Locations
	dir.Args = args
	return dir
}

// IncludeDirective is used to conditionally include fields or fragments
var IncludeDirective = NewDirective(DirectiveConfig{
	Name: "include",
	Description: "Directs the executor to include this field or fragment only when " +
		"the `if` argument is true.",
	Locations: []string{
		DirectiveLocationField,
		DirectiveLocationFragmentSpread,
		DirectiveLocationInlineFragment,
	},
	Args: FieldConfigArgument{
		"if": &ArgumentConfig{
			Type:        NewNonNull(Boolean),
			Description: "Included when true.",
		},
	},
})

// SkipDirective Used to conditionally skip (exclude) fields or fragments
var SkipDirective = NewDirective(DirectiveConfig{
	Name: "skip",
	Description: "Directs the executor to skip this field or fragment when the `if` " +
		"argument is true.",
	Args: FieldConfigArgument{
		"if": &ArgumentConfig{
			Type:        NewNonNull(Boolean),
			Description: "Skipped when true.",
		},
	},
	Locations: []string{
		DirectiveLocationField,
		DirectiveLocationFragmentSpread,
		DirectiveLocationInlineFragment,
	},
})
