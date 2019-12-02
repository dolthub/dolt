package commands

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestParseDate(t *testing.T) {
	tests := []struct {
		dateStr string
		expTime time.Time
		expErr  bool
	}{
		{"1901/09/30", time.Date(1901, 9, 30, 0, 0, 0, 0, time.UTC), false},
		//{"2019/01/20", time.Date(2019, 1, 20, 0, 0, 0, 0, time.UTC), false},
		//{"2019-1-20", time.Date(2019, 1, 20, 0, 0, 0, 0, time.UTC), true},
		//{"2019.01.20", time.Date(2019, 1, 20, 0, 0, 0, 0, time.UTC), false},
		//{"2019/01/20T13:49:59", time.Date(2019, 1, 20, 13, 49, 59, 0, time.UTC), false},
		//{"2019-01-20T13:49:59", time.Date(2019, 1, 20, 13, 49, 59, 0, time.UTC), false},
		//{"2019.01.20T13:49:59", time.Date(2019, 1, 20, 13, 49, 59, 0, time.UTC), false},
		//{"2019.01.20T13:49", time.Date(2019, 1, 20, 13, 49, 59, 0, time.UTC), true},
		//{"2019.01.20T13", time.Date(2019, 1, 20, 13, 49, 59, 0, time.UTC), true},
		//{"2019.01", time.Date(2019, 1, 20, 13, 49, 59, 0, time.UTC), true},
	}

	for _, test := range tests {
		t.Run(test.dateStr, func(t *testing.T) {
			result, err := parseDate(test.dateStr)

			if test.expErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, result, test.expTime)
			}
		})
	}
}
