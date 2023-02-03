package schema

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCompareCollatedStrings(t *testing.T) {
	tests := []struct {
		name  string
		left  []byte
		right []byte
		exp   int
	}{
		{
			left:  []byte("Hello, 人"),
			right: []byte("Hello, 亻"),
			exp:   -1,
		},
		{
			left:  []byte("woÒ"),
			right: []byte("woÓ"),
			exp:   0,
		},
		{
			left:  []byte("\u07FB"),
			right: []byte("\u07FC"),
			exp:   -1,
		},
		{
			left:  []byte("˧"),
			right: []byte("˦"),
			exp:   1,
		},
		{
			left:  []byte("ƵƶzƸ"),
			right: []byte("ƵƶzƷ"),
			exp:   1,
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s vs %s", tt.left, tt.right), func(t *testing.T) {
			cmp := compareCollatedStrings(sql.Collation_utf8mb4_0900_ai_ci, tt.left, tt.right)
			require.Equal(t, tt.exp, cmp)
		})
	}
}
