package logictest

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestParseFile(t *testing.T) {
	f :=  "C:\\Users\\zachmu\\liquidata\\go-workspace\\src\\github.com\\gregrahn\\sqllogictest\\test\\select1.test"
	records, err := ParseTestFile(f)
	require.NoError(t, err)

	fmt.Print(records)
}
