package enginetest

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_pmd(t *testing.T) {
	tests := []struct {
		name  string
		str   string
		want  []string
		panic bool
	}{
		{
			name: "missing ancestor",
			str: `
right:
	blah
left:
	blah
`,
			panic: true,
		},
		{
			name: "out-of-order left/right",
			str: `
ancestor:
	blah
left:
	blah
right:
	blah
`,
			panic: true,
		},
		{
			name: "missing left",
			str: `
ancestor:
	blah
right:
	blah
`,
			panic: true,
		},
		{
			name: "missing right",
			str: `
ancestor:
	blah
left:
	blah
`,
			panic: true,
		},
		{
			name: "base case",
			str: `
ancestor:
	create table t (pk int primary key);

right:
	insert into t values (1);

left:
	insert into t values (2);
`,
			want: []string{
				"create table t (pk int primary key);",
				"CALL DOLT_COMMIT('-Am', 'ancestor commit');",
				"CALL DOLT_CHECKOUT('-b', 'right');",
				"insert into t values (1);",
				"CALL DOLT_COMMIT('-Am', 'right commit');",
				"CALL DOLT_CHECKOUT('main');",
				"insert into t values (2);",
				"CALL DOLT_COMMIT('-Am', 'left commit');",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.panic {
				assertPanic(t, func() {
					pmd(tt.str)
				})
			} else {
				assert.Equalf(t, tt.want, pmd(tt.str), "pmd(%v)", tt.str)
			}

		})
	}
}

func Benchmark_pwd(b *testing.B) {
	for n := 0; n < b.N; n++ {
		pmd(`
ancestor:
	create table t (pk int primary key);
	insert into t values (1), (2);

right:
	alter table t add column col2 int;
	insert into t values (3, 300), (4, 400);

left:
	alter table t add column col1 int;
	insert into t values (5, 50), (6, 60);
`)
	}
}

func assertPanic(t *testing.T, f func()) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic")
		}
	}()
	f()
}
