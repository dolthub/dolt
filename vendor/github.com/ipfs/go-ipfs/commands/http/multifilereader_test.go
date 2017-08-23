package http

import (
	"io"
	"io/ioutil"
	"mime/multipart"
	"strings"
	"testing"

	files "github.com/ipfs/go-ipfs/commands/files"
)

func TestOutput(t *testing.T) {
	text := "Some text! :)"
	fileset := []files.File{
		files.NewReaderFile("file.txt", "file.txt", ioutil.NopCloser(strings.NewReader(text)), nil),
		files.NewSliceFile("boop", "boop", []files.File{
			files.NewReaderFile("boop/a.txt", "boop/a.txt", ioutil.NopCloser(strings.NewReader("bleep")), nil),
			files.NewReaderFile("boop/b.txt", "boop/b.txt", ioutil.NopCloser(strings.NewReader("bloop")), nil),
		}),
		files.NewReaderFile("beep.txt", "beep.txt", ioutil.NopCloser(strings.NewReader("beep")), nil),
	}
	sf := files.NewSliceFile("", "", fileset)
	buf := make([]byte, 20)

	// testing output by reading it with the go stdlib "mime/multipart" Reader
	mfr := NewMultiFileReader(sf, true)
	mpReader := multipart.NewReader(mfr, mfr.Boundary())

	part, err := mpReader.NextPart()
	if part == nil || err != nil {
		t.Fatal("Expected non-nil part, nil error")
	}
	mpf, err := files.NewFileFromPart(part)
	if mpf == nil || err != nil {
		t.Fatal("Expected non-nil MultipartFile, nil error")
	}
	if mpf.IsDirectory() {
		t.Fatal("Expected file to not be a directory")
	}
	if mpf.FileName() != "file.txt" {
		t.Fatal("Expected filename to be \"file.txt\"")
	}
	if n, err := mpf.Read(buf); n != len(text) || err != nil {
		t.Fatal("Expected to read from file", n, err)
	}
	if string(buf[:len(text)]) != text {
		t.Fatal("Data read was different than expected")
	}

	part, err = mpReader.NextPart()
	if part == nil || err != nil {
		t.Fatal("Expected non-nil part, nil error")
	}
	mpf, err = files.NewFileFromPart(part)
	if mpf == nil || err != nil {
		t.Fatal("Expected non-nil MultipartFile, nil error")
	}
	if !mpf.IsDirectory() {
		t.Fatal("Expected file to be a directory")
	}
	if mpf.FileName() != "boop" {
		t.Fatal("Expected filename to be \"boop\"")
	}

	part, err = mpReader.NextPart()
	if part == nil || err != nil {
		t.Fatal("Expected non-nil part, nil error")
	}
	child, err := files.NewFileFromPart(part)
	if child == nil || err != nil {
		t.Fatal("Expected to be able to read a child file")
	}
	if child.IsDirectory() {
		t.Fatal("Expected file to not be a directory")
	}
	if child.FileName() != "boop/a.txt" {
		t.Fatal("Expected filename to be \"some/file/path\"")
	}

	part, err = mpReader.NextPart()
	if part == nil || err != nil {
		t.Fatal("Expected non-nil part, nil error")
	}
	child, err = files.NewFileFromPart(part)
	if child == nil || err != nil {
		t.Fatal("Expected to be able to read a child file")
	}
	if child.IsDirectory() {
		t.Fatal("Expected file to not be a directory")
	}
	if child.FileName() != "boop/b.txt" {
		t.Fatal("Expected filename to be \"some/file/path\"")
	}

	child, err = mpf.NextFile()
	if child != nil || err != io.EOF {
		t.Fatal("Expected to get (nil, io.EOF)")
	}

	part, err = mpReader.NextPart()
	if part == nil || err != nil {
		t.Fatal("Expected non-nil part, nil error")
	}
	mpf, err = files.NewFileFromPart(part)
	if mpf == nil || err != nil {
		t.Fatal("Expected non-nil MultipartFile, nil error")
	}

	part, err = mpReader.NextPart()
	if part != nil || err != io.EOF {
		t.Fatal("Expected to get (nil, io.EOF)")
	}
}
