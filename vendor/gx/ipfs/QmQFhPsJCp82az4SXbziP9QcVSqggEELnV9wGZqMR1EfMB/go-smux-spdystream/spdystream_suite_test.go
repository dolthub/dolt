package peerstream_spdystream

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestGoSmuxSpdystream(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "GoSmuxSpdystream Suite")
}
