package files

import (
	"io"
	"io/ioutil"
	"mime/multipart"
	"strings"
	"testing"
)

func TestSliceFiles(t *testing.T) {
	name := "testname"
	files := []File{
		NewReaderFile("file.txt", "file.txt", ioutil.NopCloser(strings.NewReader("Some text!\n")), nil),
		NewReaderFile("beep.txt", "beep.txt", ioutil.NopCloser(strings.NewReader("beep")), nil),
		NewReaderFile("boop.txt", "boop.txt", ioutil.NopCloser(strings.NewReader("boop")), nil),
	}
	buf := make([]byte, 20)

	sf := NewSliceFile(name, name, files)

	if !sf.IsDirectory() {
		t.Fatal("SliceFile should always be a directory")
	}

	if n, err := sf.Read(buf); n > 0 || err != io.EOF {
		t.Fatal("Shouldn't be able to read data from a SliceFile")
	}

	if err := sf.Close(); err != ErrNotReader {
		t.Fatal("Shouldn't be able to call `Close` on a SliceFile")
	}

	file, err := sf.NextFile()
	if file == nil || err != nil {
		t.Fatal("Expected a file and nil error")
	}
	read, err := file.Read(buf)
	if read != 11 || err != nil {
		t.Fatal("NextFile got a file in the wrong order")
	}

	file, err = sf.NextFile()
	if file == nil || err != nil {
		t.Fatal("Expected a file and nil error")
	}
	file, err = sf.NextFile()
	if file == nil || err != nil {
		t.Fatal("Expected a file and nil error")
	}

	file, err = sf.NextFile()
	if file != nil || err != io.EOF {
		t.Fatal("Expected a nil file and io.EOF")
	}
}

func TestReaderFiles(t *testing.T) {
	message := "beep boop"
	rf := NewReaderFile("file.txt", "file.txt", ioutil.NopCloser(strings.NewReader(message)), nil)
	buf := make([]byte, len(message))

	if rf.IsDirectory() {
		t.Fatal("ReaderFile should never be a directory")
	}
	file, err := rf.NextFile()
	if file != nil || err != ErrNotDirectory {
		t.Fatal("Expected a nil file and ErrNotDirectory")
	}

	if n, err := rf.Read(buf); n == 0 || err != nil {
		t.Fatal("Expected to be able to read")
	}
	if err := rf.Close(); err != nil {
		t.Fatal("Should be able to close")
	}
	if n, err := rf.Read(buf); n != 0 || err != io.EOF {
		t.Fatal("Expected EOF when reading after close")
	}
}

func TestMultipartFiles(t *testing.T) {
	data := `
--Boundary!
Content-Type: text/plain
Content-Disposition: file; filename="name"
Some-Header: beep

beep
--Boundary!
Content-Type: application/x-directory
Content-Disposition: file; filename="dir"

--Boundary!
Content-Type: text/plain
Content-Disposition: file; filename="dir/nested"

some content
--Boundary!
Content-Type: application/symlink
Content-Disposition: file; filename="dir/simlynk"

anotherfile
--Boundary!--

`

	reader := strings.NewReader(data)
	mpReader := multipart.NewReader(reader, "Boundary!")
	buf := make([]byte, 20)

	// test properties of a file created from the first part
	part, err := mpReader.NextPart()
	if part == nil || err != nil {
		t.Fatal("Expected non-nil part, nil error")
	}
	mpf, err := NewFileFromPart(part)
	if mpf == nil || err != nil {
		t.Fatal("Expected non-nil MultipartFile, nil error")
	}
	if mpf.IsDirectory() {
		t.Fatal("Expected file to not be a directory")
	}
	if mpf.FileName() != "name" {
		t.Fatal("Expected filename to be \"name\"")
	}
	if file, err := mpf.NextFile(); file != nil || err != ErrNotDirectory {
		t.Fatal("Expected a nil file and ErrNotDirectory")
	}
	if n, err := mpf.Read(buf); n != 4 || err != io.EOF && err != nil {
		t.Fatal("Expected to be able to read 4 bytes: ", n, err)
	}
	if err := mpf.Close(); err != nil {
		t.Fatal("Expected to be able to close file")
	}

	// test properties of file created from second part (directory)
	part, err = mpReader.NextPart()
	if part == nil || err != nil {
		t.Fatal("Expected non-nil part, nil error")
	}
	mpf, err = NewFileFromPart(part)
	if mpf == nil || err != nil {
		t.Fatal("Expected non-nil MultipartFile, nil error")
	}
	if !mpf.IsDirectory() {
		t.Fatal("Expected file to be a directory")
	}
	if mpf.FileName() != "dir" {
		t.Fatal("Expected filename to be \"dir\"")
	}
	if n, err := mpf.Read(buf); n > 0 || err != ErrNotReader {
		t.Fatal("Shouldn't be able to call `Read` on a directory")
	}
	if err := mpf.Close(); err != ErrNotReader {
		t.Fatal("Shouldn't be able to call `Close` on a directory")
	}

	// test properties of file created from third part (nested file)
	part, err = mpReader.NextPart()
	if part == nil || err != nil {
		t.Fatal("Expected non-nil part, nil error")
	}
	mpf, err = NewFileFromPart(part)
	if mpf == nil || err != nil {
		t.Fatal("Expected non-nil MultipartFile, nil error")
	}
	if mpf.IsDirectory() {
		t.Fatal("Expected file, got directory")
	}
	if mpf.FileName() != "dir/nested" {
		t.Fatalf("Expected filename to be \"nested\", got %s", mpf.FileName())
	}
	if n, err := mpf.Read(buf); n != 12 || err != io.EOF && err != nil {
		t.Fatalf("expected to be able to read 12 bytes from file: %s (got %d)", err, n)
	}
	if err := mpf.Close(); err != nil {
		t.Fatalf("should be able to close file: %s", err)
	}

	// test properties of symlink created from fourth part (symlink)
	part, err = mpReader.NextPart()
	if part == nil || err != nil {
		t.Fatal("Expected non-nil part, nil error")
	}
	mpf, err = NewFileFromPart(part)
	if mpf == nil || err != nil {
		t.Fatal("Expected non-nil MultipartFile, nil error")
	}
	if mpf.IsDirectory() {
		t.Fatal("Expected file to be a symlink")
	}
	if mpf.FileName() != "dir/simlynk" {
		t.Fatal("Expected filename to be \"dir/simlynk\"")
	}
	slink, ok := mpf.(*Symlink)
	if !ok {
		t.Fatalf("expected file to be a symlink")
	}
	if slink.Target != "anotherfile" {
		t.Fatal("expected link to point to anotherfile")
	}
}
