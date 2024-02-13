package sysbench_runner

type sysbenchTestParamsImpl []string

func (s *sysbenchTestParamsImpl) ToSlice() []string {
	if s != nil {
		return *s
	}
	return []string{}
}

var _ TestParams = &sysbenchTestParamsImpl{}

// sysbenchTestImpl is a single sysbench test
type sysbenchTestImpl struct {
	id string

	// Name is the test name
	Name string

	// Params are the parameters passed to sysbench
	Params TestParams

	// FromScript indicates if this test is from a lua script
	FromScript bool
}

var _ Test = &sysbenchTestImpl{}

func NewSysbenchTest(id, name string, params TestParams, fromScript bool) *sysbenchTestImpl {
	return &sysbenchTestImpl{
		id:         id,
		Name:       name,
		Params:     params,
		FromScript: fromScript,
	}
}

func (t *sysbenchTestImpl) GetId() string {
	return t.id
}

func (t *sysbenchTestImpl) GetName() string {
	return t.Name
}

func (t *sysbenchTestImpl) GetParamsToSlice() []string {
	return t.Params.ToSlice()
}

// PrepareArgs returns a test's args for sysbench's prepare step
func (t *sysbenchTestImpl) GetPrepareArgs(serverConfig ServerConfig) []string {
	return withCommand(t.Params, sysbenchPrepareCommand)
}

// Run returns a test's args for sysbench's run step
func (t *sysbenchTestImpl) GetRunArgs(serverConfig ServerConfig) []string {
	return withCommand(t.Params, sysbenchRunCommand)
}

// Cleanup returns a test's args for sysbench's cleanup step
func (t *sysbenchTestImpl) GetCleanupArgs(serverConfig ServerConfig) []string {
	return withCommand(t.Params, sysbenchCleanupCommand)
}

func withCommand(params TestParams, command string) []string {
	c := make([]string, 0)
	c = append(c, params.ToSlice()...)
	return append(c, command)
}
