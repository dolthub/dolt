package sysbench_runner

// sysbenchTestImpl is a single sysbench test
type sysbenchTestImpl struct {
	id string

	// Name is the test name
	Name string

	// Params are the parameters passed to sysbench
	Params []string

	// FromScript indicates if this test is from a lua script
	FromScript bool
}

var _ Test = &sysbenchTestImpl{}

func NewSysbenchTest(id, name string, params []string, fromScript bool) *sysbenchTestImpl {
	return &sysbenchTestImpl{
		id:         id,
		Name:       name,
		Params:     params,
		FromScript: fromScript,
	}
}

// PrepareArgs returns a test's args for sysbench's prepare step
func (t *sysbenchTestImpl) GetPrepareArgs(serverConfig ServerConfig) []string {
	return withCommand(t.Params, "prepare")
}

// Run returns a test's args for sysbench's run step
func (t *sysbenchTestImpl) GetRunArgs(serverConfig ServerConfig) []string {
	return withCommand(t.Params, "run")
}

// Cleanup returns a test's args for sysbench's cleanup step
func (t *sysbenchTestImpl) GetCleanupArgs(serverConfig ServerConfig) []string {
	return withCommand(t.Params, "cleanup")
}

func withCommand(params []string, command string) []string {
	c := make([]string, 0)
	c = append(c, params...)
	return append(c, command)
}
