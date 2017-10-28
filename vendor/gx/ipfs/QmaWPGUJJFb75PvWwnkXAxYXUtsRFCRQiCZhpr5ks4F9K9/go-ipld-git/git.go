package ipldgit

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"strings"

	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
	node "gx/ipfs/QmPN7cwmpcc4DWXb4KTB9dNAJgjuPY69h3npsMfhRrQL9c/go-ipld-format"
	blocks "gx/ipfs/QmSn9Td7xgxm9EV7iEjTckpUWmWApggzPxu7eFGWkkpwin/go-block-format"
	mh "gx/ipfs/QmU9a9NV9RdPNwZQDYd5uKsm6N6LJLSvLbywDDYFbaaC6P/go-multihash"
)

func DecodeBlock(block blocks.Block) (node.Node, error) {
	prefix := block.Cid().Prefix()

	if prefix.Codec != cid.GitRaw || prefix.MhType != mh.SHA1 || prefix.MhLength != mh.DefaultLengths[mh.SHA1] {
		return nil, errors.New("invalid CID prefix")
	}

	return ParseObjectFromBuffer(block.RawData())
}

var _ node.DecodeBlockFunc = DecodeBlock

func ParseObjectFromBuffer(b []byte) (node.Node, error) {
	return ParseObject(bytes.NewReader(b))
}

func ParseCompressedObject(r io.Reader) (node.Node, error) {
	rc, err := zlib.NewReader(r)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	return ParseObject(rc)
}

func ParseObject(r io.Reader) (node.Node, error) {
	rd := bufio.NewReader(r)

	typ, err := rd.ReadString(' ')
	if err != nil {
		return nil, err
	}
	typ = typ[:len(typ)-1]

	switch typ {
	case "tree":
		return ReadTree(rd)
	case "commit":
		return ReadCommit(rd)
	case "blob":
		return ReadBlob(rd)
	case "tag":
		return ReadTag(rd)
	default:
		return nil, fmt.Errorf("unrecognized object type: %s", typ)
	}
}

func ReadBlob(rd *bufio.Reader) (Blob, error) {
	size, err := rd.ReadString(0)
	if err != nil {
		return nil, err
	}

	sizen, err := strconv.Atoi(size[:len(size)-1])
	if err != nil {
		return nil, err
	}

	buf := new(bytes.Buffer)
	fmt.Fprintf(buf, "blob %d\x00", sizen)

	n, err := io.Copy(buf, rd)
	if err != nil {
		return nil, err
	}

	if n != int64(sizen) {
		return nil, fmt.Errorf("blob size was not accurate")
	}

	return Blob(buf.Bytes()), nil
}

func ReadCommit(rd *bufio.Reader) (*Commit, error) {
	size, err := rd.ReadString(0)
	if err != nil {
		return nil, err
	}

	out := &Commit{
		DataSize: size[:len(size)-1],
	}

	for {
		line, _, err := rd.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		err = parseCommitLine(out, line, rd)
		if err != nil {
			return nil, err
		}
	}

	out.cid = hashObject(out.RawData())

	return out, nil
}

func parseCommitLine(out *Commit, line []byte, rd *bufio.Reader) error {
	switch {
	case bytes.HasPrefix(line, []byte("tree ")):
		sha, err := hex.DecodeString(string(line[5:]))
		if err != nil {
			return err
		}

		out.GitTree = shaToCid(sha)
	case bytes.HasPrefix(line, []byte("parent ")):
		psha, err := hex.DecodeString(string(line[7:]))
		if err != nil {
			return err
		}

		out.Parents = append(out.Parents, shaToCid(psha))
	case bytes.HasPrefix(line, []byte("author ")):
		a, err := parsePersonInfo(line)
		if err != nil {
			return err
		}

		out.Author = a
	case bytes.HasPrefix(line, []byte("committer ")):
		c, err := parsePersonInfo(line)
		if err != nil {
			return err
		}

		out.Committer = c
	case bytes.HasPrefix(line, []byte("encoding ")):
		out.Encoding = string(line[9:])
	case bytes.HasPrefix(line, []byte("mergetag object ")):
		sha, err := hex.DecodeString(string(line)[16:])
		if err != nil {
			return err
		}

		mt, rest, err := ReadMergeTag(sha, rd)
		if err != nil {
			return err
		}

		out.MergeTag = append(out.MergeTag, mt)

		if rest != nil {
			err = parseCommitLine(out, rest, rd)
			if err != nil {
				return err
			}
		}
	case bytes.HasPrefix(line, []byte("gpgsig ")):
		sig, err := ReadGpgSig(rd)
		if err != nil {
			return err
		}
		out.Sig = sig
	case len(line) == 0:
		rest, err := ioutil.ReadAll(rd)
		if err != nil {
			return err
		}

		out.Message = string(rest)
	default:
		out.Other = append(out.Other, string(line))
	}
	return nil
}

func ReadTag(rd *bufio.Reader) (*Tag, error) {
	size, err := rd.ReadString(0)
	if err != nil {
		return nil, err
	}

	out := &Tag{
		dataSize: size[:len(size)-1],
	}

	for {
		line, _, err := rd.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		switch {
		case bytes.HasPrefix(line, []byte("object ")):
			sha, err := hex.DecodeString(string(line[7:]))
			if err != nil {
				return nil, err
			}

			out.Object = shaToCid(sha)
		case bytes.HasPrefix(line, []byte("tag ")):
			out.Tag = string(line[4:])
		case bytes.HasPrefix(line, []byte("tagger ")):
			c, err := parsePersonInfo(line)
			if err != nil {
				return nil, err
			}

			out.Tagger = c
		case bytes.HasPrefix(line, []byte("type ")):
			out.Type = string(line[5:])
		case len(line) == 0:
			rest, err := ioutil.ReadAll(rd)
			if err != nil {
				return nil, err
			}

			out.Message = string(rest)
		default:
			fmt.Println("unhandled line: ", string(line))
		}
	}

	out.cid = hashObject(out.RawData())

	return out, nil
}

func hashObject(data []byte) *cid.Cid {
	c, err := cid.Prefix{
		MhType:   mh.SHA1,
		MhLength: -1,
		Codec:    cid.GitRaw,
		Version:  1,
	}.Sum(data)
	if err != nil {
		panic(err)
	}
	return c
}

func ReadMergeTag(hash []byte, rd *bufio.Reader) (*MergeTag, []byte, error) {
	out := new(MergeTag)

	out.Object = shaToCid(hash)
	for {
		line, _, err := rd.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, nil, err
		}

		switch {
		case bytes.HasPrefix(line, []byte(" type ")):
			out.Type = string(line[6:])
		case bytes.HasPrefix(line, []byte(" tag ")):
			out.Tag = string(line[5:])
		case bytes.HasPrefix(line, []byte(" tagger ")):
			tagger, err := parsePersonInfo(line[1:])
			if err != nil {
				return nil, nil, err
			}
			out.Tagger = tagger
		case string(line) == " ":
			for {
				line, _, err := rd.ReadLine()
				if err != nil {
					return nil, nil, err
				}

				if !bytes.HasPrefix(line, []byte(" ")) {
					return out, line, nil
				}

				out.Text += string(line) + "\n"
			}
		}
	}
	return out, nil, nil
}

func ReadGpgSig(rd *bufio.Reader) (*GpgSig, error) {
	line, _, err := rd.ReadLine()
	if err != nil {
		return nil, err
	}

	out := new(GpgSig)

	if string(line) != " " {
		if strings.HasPrefix(string(line), " Version: ") || strings.HasPrefix(string(line), " Comment: ") {
			out.Text += string(line) + "\n"
		} else {
			return nil, fmt.Errorf("expected first line of sig to be a single space or version")
		}
	} else {
		out.Text += " \n"
	}

	for {
		line, _, err := rd.ReadLine()
		if err != nil {
			return nil, err
		}

		if bytes.Equal(line, []byte(" -----END PGP SIGNATURE-----")) {
			break
		}

		out.Text += string(line) + "\n"
	}

	return out, nil
}

func parsePersonInfo(line []byte) (*PersonInfo, error) {
	parts := bytes.Split(line, []byte{' '})
	if len(parts) < 3 {
		fmt.Println(string(line))
		return nil, fmt.Errorf("incorrectly formatted person info line")
	}

	//TODO: just use regex?
	//skip prefix
	at := 1

	var pi PersonInfo
	var name string

	for {
		if at == len(parts) {
			return nil, fmt.Errorf("invalid personInfo: %s\n", line)
		}
		part := parts[at]
		if len(part) != 0 {
			if part[0] == '<' {
				break
			}
			name += string(part) + " "
		} else if len(name) > 0 {
			name += " "
		}
		at++
	}
	if len(name) != 0 {
		pi.Name = name[:len(name)-1]
	}

	var email string
	for {
		if at == len(parts) {
			return nil, fmt.Errorf("invalid personInfo: %s\n", line)
		}
		part := parts[at]
		if part[0] == '<' {
			part = part[1:]
		}

		at++
		if part[len(part)-1] == '>' {
			email += string(part[:len(part)-1])
			break
		}
		email += string(part) + " "
	}
	pi.Email = email

	if at == len(parts) {
		return &pi, nil
	}
	pi.Date = string(parts[at])

	at++
	if at == len(parts) {
		return &pi, nil
	}
	pi.Timezone = string(parts[at])
	return &pi, nil
}

func ReadTree(rd *bufio.Reader) (*Tree, error) {
	lstr, err := rd.ReadString(0)
	if err != nil {
		return nil, err
	}
	lstr = lstr[:len(lstr)-1]

	n, err := strconv.Atoi(lstr)
	if err != nil {
		return nil, err
	}

	t := &Tree{
		entries: make(map[string]*TreeEntry),
		size:    n,
	}
	var order []string
	for {
		e, err := ReadEntry(rd)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		order = append(order, e.name)
		t.entries[e.name] = e
	}
	t.order = order
	t.cid = hashObject(t.RawData())

	return t, nil
}

func cidToSha(c *cid.Cid) []byte {
	h := c.Hash()
	return h[len(h)-20:]
}

func shaToCid(sha []byte) *cid.Cid {
	h, _ := mh.Encode(sha, mh.SHA1)
	return cid.NewCidV1(cid.GitRaw, h)
}

func ReadEntry(r *bufio.Reader) (*TreeEntry, error) {
	data, err := r.ReadString(' ')
	if err != nil {
		return nil, err
	}
	data = data[:len(data)-1]

	name, err := r.ReadString(0)
	if err != nil {
		return nil, err
	}
	name = name[:len(name)-1]

	sha := make([]byte, 20)
	_, err = io.ReadFull(r, sha)
	if err != nil {
		return nil, err
	}

	return &TreeEntry{
		name: name,
		Mode: data,
		Hash: shaToCid(sha),
	}, nil
}
