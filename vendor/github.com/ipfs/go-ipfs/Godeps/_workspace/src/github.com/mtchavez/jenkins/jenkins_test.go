package jenkins

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"hash"
)

var _ = Describe("Jenkins", func() {

	var jhash hash.Hash32
	var key []byte

	BeforeEach(func() {
		jhash = New()
		key = []byte("Apple")
	})

	Describe("New", func() {

		It("returns jenkhash", func() {
			var h *jenkhash
			Expect(jhash).To(BeAssignableToTypeOf(h))
		})

		It("initializes offset to 0", func() {
			Expect(jhash.Sum32()).To(Equal(uint32(0)))
		})
	})

	Describe("Write", func() {

		It("returns key length", func() {
			length, _ := jhash.Write(key)
			Expect(length).To(Equal(5))
		})

		It("has no error", func() {
			_, err := jhash.Write(key)
			Expect(err).To(BeNil())
		})

	})

	Describe("Reset", func() {

		It("sets back to 0", func() {
			Expect(jhash.Sum32()).To(Equal(uint32(0)))
			jhash.Write(key)
			Expect(jhash.Sum32()).NotTo(Equal(uint32(0)))
			jhash.Reset()
			Expect(jhash.Sum32()).To(Equal(uint32(0)))
		})

	})

	Describe("Size", func() {

		It("is 4", func() {
			Expect(jhash.Size()).To(Equal(4))
		})

	})

	Describe("BlockSize", func() {

		It("is 1", func() {
			Expect(jhash.BlockSize()).To(Equal(1))
		})

	})

	Describe("Sum32", func() {

		It("defaults to 0", func() {
			Expect(jhash.Sum32()).To(Equal(uint32(0)))
		})

		It("sums hash", func() {
			jhash.Write(key)
			Expect(jhash.Sum32()).To(Equal(uint32(884782484)))
		})

	})

	Describe("Sum", func() {

		It("default 0 hash byte returned", func() {
			expected := []byte{0x41, 0x70, 0x70, 0x6c, 0x65, 0x0, 0x0, 0x0, 0x0}
			Expect(jhash.Sum(key)).To(Equal(expected))
		})

		It("returns sum byte array", func() {
			jhash.Write(key)
			expected := []byte{0x41, 0x70, 0x70, 0x6c, 0x65, 0x34, 0xbc, 0xb5, 0x94}
			Expect(jhash.Sum(key)).To(Equal(expected))
		})

	})

})
