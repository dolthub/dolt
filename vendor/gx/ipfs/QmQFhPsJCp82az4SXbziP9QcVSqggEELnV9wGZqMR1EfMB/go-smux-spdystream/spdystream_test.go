package peerstream_spdystream

import (
	"io"
	"net"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	smux "gx/ipfs/QmY9JXR3FupnYAYJWK9aMr9bCpqWKcToQ1tz8DVGTrHpHw/go-stream-muxer"
)

var _ = Describe("SPDY", func() {
	var server, client smux.Conn

	Context("accepting streams", func() {
		BeforeEach(func() {
			// start the server
			serverReady := make(chan struct{})
			serverAddr := make(chan net.Addr)
			go func() {
				defer GinkgoRecover()
				l, err := net.Listen("tcp", "localhost:0")
				Expect(err).ToNot(HaveOccurred())
				serverAddr <- l.Addr()
				c, err := l.Accept()
				Expect(err).ToNot(HaveOccurred())
				server, err = Transport.NewConn(c, true)
				Expect(err).ToNot(HaveOccurred())
				close(serverReady)
			}()

			// start the client
			addr := <-serverAddr
			nconn, err := net.Dial("tcp", addr.String())
			Expect(err).ToNot(HaveOccurred())
			client, err = Transport.NewConn(nconn, false)
			Expect(err).ToNot(HaveOccurred())
			<-serverReady
		})

		AfterEach(func() {
			server.Close()
			client.Close()
		})

		It("returns an error when the connection is closed", func() {
			done := make(chan struct{})
			go func() {
				defer GinkgoRecover()
				_, err := server.AcceptStream()
				Expect(err).To(MatchError(errClosed))
				close(done)
			}()
			server.Close()
			Eventually(done).Should(BeClosed())
		})

		It("returns an error when the connection is closed, even if there are streams in the queue", func() {
			_, err := client.OpenStream()
			Expect(err).ToNot(HaveOccurred())
			server.Close()
			_, err = server.AcceptStream()
			Expect(err).To(MatchError(errClosed))
		})

		It("waits for new streams", func() {
			done := make(chan struct{})
			go func() {
				defer GinkgoRecover()
				server.AcceptStream()
				close(done)
			}()
			Consistently(done).ShouldNot(BeClosed())
			// kill the goroutine, so that the race detector is happy
			server.Close()
			Eventually(done).Should(BeClosed())
		})

		It("accepts a new stream", func() {
			done := make(chan struct{})
			go func() {
				defer GinkgoRecover()
				str, err := server.AcceptStream()
				Expect(err).ToNot(HaveOccurred())
				_, err = io.Copy(str, str)
				Expect(err).ToNot(HaveOccurred())
				close(done)
			}()
			str, err := client.OpenStream()
			Expect(err).ToNot(HaveOccurred())
			_, err = str.Write([]byte("foobar"))
			Expect(err).ToNot(HaveOccurred())
			b := make([]byte, 6)
			_, err = str.Read(b)
			Expect(err).ToNot(HaveOccurred())
			Expect(b).To(Equal([]byte("foobar")))
			str.Close()
			Eventually(done).Should(BeClosed())
		})

		It("accepts multiple streams, that were opened before AcceptStream was called", func() {
			n := 5
			for i := 0; i < n; i++ {
				_, err := client.OpenStream()
				Expect(err).ToNot(HaveOccurred())
			}
			time.Sleep(50 * time.Millisecond)
			for i := 0; i < n; i++ {
				_, err := server.AcceptStream()
				Expect(err).ToNot(HaveOccurred())
			}
		})
	})
})
