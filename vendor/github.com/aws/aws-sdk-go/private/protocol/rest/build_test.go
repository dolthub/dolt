package rest

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUpdatePathWithRaw(t *testing.T) {
	uri := &url.URL{
		Scheme: "https",
		Host:   "host",
	}
	updatePath(uri, "//foo//bar", true)

	expected := "https://host//foo//bar"
	assert.Equal(t, expected, uri.String())
}

func TestUpdatePathNoRaw(t *testing.T) {
	uri := &url.URL{
		Scheme: "https",
		Host:   "host",
	}
	updatePath(uri, "//foo//bar", false)

	expected := "https://host/foo/bar"
	assert.Equal(t, expected, uri.String())
}
