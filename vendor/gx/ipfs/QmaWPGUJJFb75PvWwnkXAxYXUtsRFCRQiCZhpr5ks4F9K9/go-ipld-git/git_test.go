package ipldgit

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	node "gx/ipfs/QmPN7cwmpcc4DWXb4KTB9dNAJgjuPY69h3npsMfhRrQL9c/go-ipld-format"
)

type GitObj interface {
	GitSha() []byte
}

func TestObjectParse(t *testing.T) {
	i := 0
	err := filepath.Walk(".git/objects", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		parts := strings.Split(path, "/")
		dir := parts[len(parts)-2]
		if dir == "info" || dir == "pack" {
			return nil
		}

		fi, err := os.Open(path)
		if err != nil {
			return err
		}
		defer fi.Close()

		thing, err := ParseCompressedObject(fi)
		if err != nil {
			fmt.Println("ERROR: ", path, err)
			return err
		}

		if i%64 == 0 {
			fmt.Printf("%d %s\r", i, path)
		}

		sha := thing.(GitObj).GitSha()
		if fmt.Sprintf("%x", sha) != parts[len(parts)-2]+parts[len(parts)-1] {
			fmt.Printf("\nsha: %x\n", sha)
			fmt.Printf("path: %s\n", path)
			fmt.Printf("mismatch on: %T\n", thing)
			fmt.Printf("%#v\n", thing)
			fmt.Println("vvvvvv")
			fmt.Println(string(thing.RawData()))
			fmt.Println("^^^^^^")
			t.Fatal("mismatch!")
		}

		err = testNode(t, thing)
		if err != nil {
			t.Fatalf("error: %s, %s", path, err)
		}
		i++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestArchiveObjectParse(t *testing.T) {
	archive, err := os.Open("testdata.tar.gz")
	if err != nil {
		fmt.Println("ERROR: ", err)
		return
	}

	defer archive.Close()

	gz, err := gzip.NewReader(archive)
	if err != nil {
		fmt.Println("ERROR: ", err)
		return
	}

	tarReader := tar.NewReader(gz)

	for {
		header, err := tarReader.Next()

		if err == io.EOF {
			break
		}

		if err != nil {
			fmt.Println("ERROR: ", err)
			return
		}

		name := header.Name

		switch header.Typeflag {
		case tar.TypeDir:
			continue
		case tar.TypeReg:
			if !strings.HasPrefix(name, ".git/objects/") {
				continue
			}

			parts := strings.Split(name, "/")
			dir := parts[2]
			if dir == "info" || dir == "pack" {
				continue
			}

			thing, err := ParseCompressedObject(tarReader)
			if err != nil {
				fmt.Println("ERROR: ", name, err)
				return
			}

			fmt.Printf("%s\r", name)

			sha := thing.(GitObj).GitSha()
			if fmt.Sprintf("%x", sha) != parts[len(parts)-2]+parts[len(parts)-1] {
				fmt.Printf("\nsha: %x\n", sha)
				fmt.Printf("path: %s\n", name)
				fmt.Printf("mismatch on: %T\n", thing)
				fmt.Printf("%#v\n", thing)
				fmt.Println("vvvvvv")
				fmt.Println(string(thing.RawData()))
				fmt.Println("^^^^^^")
				t.Fatal("mismatch!")
			}

			err = testNode(t, thing)
			if err != nil {
				t.Fatalf("error: %s, %s", name, err)
			}
		default:

		}
	}

}

func testNode(t *testing.T, nd node.Node) error {
	switch nd.String() {
	case "[git blob]":
		blob, ok := nd.(Blob)
		if !ok {
			t.Fatalf("Blob is not a blob")
		}

		assert(t, blob.Links() == nil)
		assert(t, blob.Tree("", 0) == nil)
		assert(t, blob.Loggable()["type"] == "git_blob")

		s, _ := blob.Size()
		assert(t, len(blob.RawData()) == int(s))
	case "[git commit object]":
		commit, ok := nd.(*Commit)
		if !ok {
			t.Fatalf("Commit is not a commit")
		}

		/*s, _ := commit.Size()
		assert.Equal(t, len(commit.RawData()), int(s))*/ //TODO: Known breakage
		assert(t, commit.GitTree != nil)
		assert(t, commit.Links() != nil)
		assert(t, commit.Loggable()["type"] == "git_commit")

		assert(t, commit.Tree("", -1) != nil)
		lnk, rest, err := commit.ResolveLink([]string{"tree", "aoeu"})
		assert(t, err == nil)
		assert(t, lnk != nil)
		assert(t, rest != nil)
		assert(t, len(rest) == 1)
		assert(t, rest[0] == "aoeu")
	case "[git tag object]":
		tag, ok := nd.(*Tag)
		if !ok {
			t.Fatalf("Tag is not a tag")
		}

		assert(t, tag.Type == "commit" || tag.Type == "tree" || tag.Type == "blob" || tag.Type == "tag")
		assert(t, tag.Object != nil)
		assert(t, tag.Loggable()["type"] == "git_tag")
		assert(t, tag.Tree("", -1) != nil)
		obj, rest, err := tag.ResolveLink([]string{"object", "aoeu"})
		assert(t, err == nil)
		assert(t, obj != nil)
		assert(t, rest != nil)
		assert(t, len(rest) == 1)
		assert(t, rest[0] == "aoeu")
	case "[git tree object]":
		tree, ok := nd.(*Tree)
		if !ok {
			t.Fatalf("Tree is not a tree")
		}

		assert(t, tree.entries != nil)
		assert(t, tree.Tree("", 0) == nil)
	}
	return nil
}

func TestParsePersonInfo(t *testing.T) {
	pi, err := parsePersonInfo([]byte("prefix Someone <some@one.somewhere> 123456 +0123"))
	if err != nil {
		t.Fatal(err)
	}

	if pi.Date != "123456" {
		t.Fatalf("invalid date, got %s\n", pi.Date)
	}

	if pi.Timezone != "+0123" {
		t.Fatalf("invalid timezone, got %s\n", pi.Timezone)
	}

	if pi.Email != "some@one.somewhere" {
		t.Fatalf("invalid email, got %s\n", pi.Email)
	}

	if pi.Name != "Someone" {
		t.Fatalf("invalid name, got %s\n", pi.Name)
	}

	pi, err = parsePersonInfo([]byte("prefix So Me One <some@one.somewhere> 123456 +0123"))
	if err != nil {
		t.Fatal(err)
	}

	if pi.Name != "So Me One" {
		t.Fatalf("invalid name, got %s\n", pi.Name)
	}

	pi, err = parsePersonInfo([]byte("prefix Some One & Other One <some@one.somewhere, other@one.elsewhere> 987654 +4321"))
	if err != nil {
		t.Fatal(err)
	}

	if pi.Date != "987654" {
		t.Fatalf("invalid date, got %s\n", pi.Date)
	}

	if pi.Timezone != "+4321" {
		t.Fatalf("invalid timezone, got %s\n", pi.Timezone)
	}

	if pi.Email != "some@one.somewhere, other@one.elsewhere" {
		t.Fatalf("invalid email, got %s\n", pi.Email)
	}

	if pi.Name != "Some One & Other One" {
		t.Fatalf("invalid name, got %s\n", pi.Name)
	}

	pi, err = parsePersonInfo([]byte("prefix  <some@one.somewhere> 987654 +4321"))
	if err != nil {
		t.Fatal(err)
	}

	if pi.Name != "" {
		t.Fatalf("invalid name, got %s\n", pi.Name)
	}

	if pi.Email != "some@one.somewhere" {
		t.Fatalf("invalid email, got %s\n", pi.Email)
	}

	if pi.Date != "987654" {
		t.Fatalf("invalid date, got %s\n", pi.Date)
	}

	if pi.Timezone != "+4321" {
		t.Fatalf("invalid timezone, got %s\n", pi.Timezone)
	}

	pi, err = parsePersonInfo([]byte("prefix Someone  <some@one.somewhere> 987654 +4321"))
	if err != nil {
		t.Fatal(err)
	}

	if pi.Name != "Someone " {
		t.Fatalf("invalid name, got %s\n", pi.Name)
	}

	pi, err = parsePersonInfo([]byte("prefix Someone <some.one@some.where>"))
	if err != nil {
		t.Fatal(err)
	}

	assert(t, pi.String() == "Someone <some.one@some.where>")
}

func assert(t *testing.T, ok bool) {
	if !ok {
		fmt.Printf("\n")
		panic("Assernion failed")
	}
}
