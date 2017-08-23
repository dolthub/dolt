package jenkins

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestJenkins(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Jenkins Suite")
}
