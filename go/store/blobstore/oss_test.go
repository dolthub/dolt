package blobstore

import (
	"context"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestOSSBlobstore_Put(t *testing.T) {
	c, _ := oss.New("oss-cn-hangzhou.aliyuncs.com", "", "")
	b, _ := c.Bucket("seanxu-version")
	cfg, err := c.GetBucketVersioning("seanxu-version")
	assert.Nil(t, err)
	assert.Equal(t, "Enabled", cfg.Status)
	f, err := os.Open("/Users/sean/code/github.com/dolthub/dolt/go/go.sum")
	assert.Nil(t, err)
	defer f.Close()
	err = b.PutObject("testversion/go.mod", f)
	assert.Nil(t, err)
}

func TestOSSBlobstore_Get(t *testing.T) {
	c, _ := oss.New("oss-cn-hangzhou.aliyuncs.com", "", "")
	b, _ := c.Bucket("seanxu-version")
	meta, _ := b.GetObjectMeta("testversion/go.mod")
	versionID := oss.GetVersionId(meta)
	// "CAEQDhiBgMDd9_CYmBgiIGY0YjE2YjY0ZTJiMzQ0NDk4YzNhZWYzNTUwMzFjYTgy"
	// "CAEQDhiBgID27JiZmBgiIDJjYTMwN2U5MDkyODRjYjg5ZWUzN2FkYTk0ZWQ3MjY5"
	assert.Equal(t, "test", versionID)
}

func TestOSSBlobstore_Put1(t *testing.T) {
	c, err := oss.New("oss-cn-hangzhou.aliyuncs.com", "", "")
	assert.Nil(t, err)
	bs, _ := NewOSSBlobstore(c, "seanxu-version", "")
	f, err := os.Open("/Users/sean/code/github.com/dolthub/dolt/go/go.sum")
	assert.Nil(t, err)
	defer f.Close()
	version, err := bs.Put(context.Background(), "dolt/TestOSSBlobstore_Put1", f)
	assert.Nil(t, err)
	assert.Equal(t, "aaa", version)
}

func TestNewOSSBlobstore(t *testing.T) {
	c, err := oss.New("oss-cn-hangzhou.aliyuncs.com", "", "")
	assert.Nil(t, err)
	bs, err := NewOSSBlobstore(c, "seanxu-version", "dolt")
	assert.Nil(t, err)
	assert.True(t, bs.enableVersion)

	bs, err = NewOSSBlobstore(c, "seanxu", "dolt")
	assert.Nil(t, err)
	assert.False(t, bs.enableVersion)
}

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
