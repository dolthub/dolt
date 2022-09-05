package blobstore

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_normalizePrefix(t *testing.T) {
	type args struct {
		prefix string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "no_leading_slash",
			args: args{
				prefix: "root",
			},
			want: "root",
		},
		{
			name: "with_leading_slash",
			args: args{
				prefix: "/root",
			},
			want: "root",
		},
		{
			name: "with_multi_leading_slash",
			args: args{
				prefix: "//root",
			},
			want: "root",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, normalizePrefix(tt.args.prefix), "normalizePrefix(%v)", tt.args.prefix)
		})
	}
}
