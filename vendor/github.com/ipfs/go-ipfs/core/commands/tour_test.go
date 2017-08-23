package commands

import (
	"bytes"
	"testing"

	"github.com/ipfs/go-ipfs/tour"
)

func TestParseTourTemplate(t *testing.T) {
	topic := &tour.Topic{
		ID: "42",
		Content: tour.Content{
			Title: "ipfs CLI test files",
			Text: `
Welcome to the ipfs test files
This is where we test our beautiful command line interfaces
		`,
		},
	}
	buf := new(bytes.Buffer)
	err := fprintTourShow(buf, topic)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(buf.String())
}
