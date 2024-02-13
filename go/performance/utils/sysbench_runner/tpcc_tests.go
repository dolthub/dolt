package sysbench_runner

import "fmt"

type tpccTestParamsImpl struct {
	// NumThreads represents the number of threads running queries concurrently.
	NumThreads int

	// ScaleFactor represents the number of warehouse to test this at scale.
	ScaleFactor int

	// Tables represents the number of tables created per warehouse.
	Tables int

	// TrxLevel represents what transaction level to use
	TrxLevel string

	// ReportCSV determines whether to report output as a csv.
	ReportCSV bool

	// ReportInterval defines how often the tpcc benchmark outputs performance stats.
	ReportInterval int

	// Time represents how long
	Time int
}

var _ TestParams = &tpccTestParamsImpl{}

// NewDefaultTpccParams returns default TpccTestParams.
func NewDefaultTpccParams() *tpccTestParamsImpl {
	return &tpccTestParamsImpl{
		NumThreads:     2, // TODO: When ready, expose as command line argument.
		ScaleFactor:    1,
		Tables:         1,
		TrxLevel:       "RR", // todo: move to constants
		ReportCSV:      true,
		ReportInterval: 1,
		Time:           30,
	}
}

// todo: fix all these to match the real flags
func (t *tpccTestParamsImpl) ToSlice() []string {
	params := make([]string, 0)
	params = append(params, fmt.Sprintf("numThreads=%d", t.NumThreads))
	params = append(params, fmt.Sprintf("scaleFactor=%d", t.ScaleFactor))
	params = append(params, fmt.Sprintf("tables=%d", t.Tables))
	params = append(params, fmt.Sprintf("trxLevel=%s", t.TrxLevel))
	params = append(params, fmt.Sprintf("reportCsv=%t", t.ReportCSV))
	params = append(params, fmt.Sprintf("reportInterval=%d", t.ReportInterval))
	params = append(params, fmt.Sprintf("time=%d", t.Time))
	return params
}

// tpccTestImpl encapsulates an End to End prepare, run, cleanup test case.
type tpccTestImpl struct {
	// Id represents a unique test id
	Id string

	// Name represents the name of the test case
	Name string

	// Params are associated parameters this test runs with
	Params TestParams
}

var _ Test = &tpccTestImpl{}

func (t *tpccTestImpl) GetId() string {
	return t.Id
}

func (t *tpccTestImpl) GetName() string {
	return t.Name
}

func (t *tpccTestImpl) GetParamsToSlice() []string {
	return t.Params.ToSlice()
}

func (t tpccTestImpl) GetPrepareArgs() []string {
	//TODO implement me
	panic("implement me")
}

func (t tpccTestImpl) GetRunArgs() []string {
	//TODO implement me
	panic("implement me")
}

func (t tpccTestImpl) GetCleanupArgs() []string {
	//TODO implement me
	panic("implement me")
}
