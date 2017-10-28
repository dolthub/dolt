package main

import (
	"bytes"
	"fmt"
	"os"
	"strings"

	c "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"

	mh "gx/ipfs/QmU9a9NV9RdPNwZQDYd5uKsm6N6LJLSvLbywDDYFbaaC6P/go-multihash"
	mb "gx/ipfs/QmafgXF3u3QSWErQoZ2URmQp5PFG384htoE7J338nS2H7T/go-multibase"
)

func usage() {
	fmt.Fprintf(os.Stderr, "usage: %s [-b multibase-code] [-v cid-version] <fmt-str> <cid> ...\n\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "<fmt-str> is either 'prefix' or a printf style format string:\n%s", fmtRef)
	os.Exit(2)
}

const fmtRef = `
   %% literal %
   %b multibase name 
   %B multibase code
   %v version string
   %V version number
   %c codec name
   %C codec code
   %h multihash name
   %H multihash code
   %L hash digest length
   %m multihash encoded in base %b (with multibase prefix)
   %M multihash encoded in base %b without multibase prefix
   %d hash digest encoded in base %b (with multibase prefix)
   %D hash digest encoded in base %b without multibase prefix
   %s cid string encoded in base %b (1)
   %s cid string encoded in base %b without multibase prefix
   %P cid prefix: %v-%c-%h-%L

(1) For CID version 0 the multibase must be base58btc and no prefix is
used.  For Cid version 1 the multibase prefix is included.
`

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	newBase := mb.Encoding(-1)
	var verConv func(cid *c.Cid) (*c.Cid, error)
	args := os.Args[1:]
outer:
	for {
		switch args[0] {
		case "-b":
			if len(args) < 2 {
				usage()
			}
			if len(args[1]) != 1 {
				fmt.Fprintf(os.Stderr, "Error: Invalid multibase code: %s\n", args[1])
				os.Exit(2)
			}
			newBase = mb.Encoding(args[1][0])
			args = args[2:]
		case "-v":
			if len(args) < 2 {
				usage()
			}
			switch args[1] {
			case "0":
				verConv = toCidV0
			case "1":
				verConv = toCidV1
			default:
				fmt.Fprintf(os.Stderr, "Error: Invalid cid version: %s\n", args[1])
				os.Exit(2)
			}
			args = args[2:]
		default:
			break outer
		}
	}
	if len(args) < 2 {
		usage()
	}
	fmtStr := args[0]
	switch fmtStr {
	case "prefix":
		fmtStr = "%P"
	default:
		if strings.IndexByte(fmtStr, '%') == -1 {
			fmt.Fprintf(os.Stderr, "Error: Invalid format string: %s\n", fmtStr)
			os.Exit(2)
		}
	}
	for _, cidStr := range args[1:] {
		base, cid, err := decode(cidStr)
		if err != nil {
			fmt.Fprintf(os.Stdout, "!INVALID_CID!\n")
			errorMsg("%s: %v", cidStr, err)
			// Don't abort on a bad cid
			continue
		}
		if newBase != -1 {
			base = newBase
		}
		if verConv != nil {
			cid, err = verConv(cid)
			if err != nil {
				fmt.Fprintf(os.Stdout, "!ERROR!\n")
				errorMsg("%s: %v", cidStr, err)
				// Don't abort on a bad conversion
				continue
			}
		}
		str, err := fmtCid(fmtStr, base, cid)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			// An error here means a bad format string, no point in continuing
			os.Exit(2)
		}
		fmt.Fprintf(os.Stdout, "%s\n", str)
	}
	os.Exit(exitCode)
}

var exitCode = 0

func errorMsg(fmtStr string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, "Error: ")
	fmt.Fprintf(os.Stderr, fmtStr, a...)
	fmt.Fprintf(os.Stderr, "\n")
	exitCode = 1
}

func decode(v string) (mb.Encoding, *c.Cid, error) {
	if len(v) < 2 {
		return 0, nil, c.ErrCidTooShort
	}

	if len(v) == 46 && v[:2] == "Qm" {
		hash, err := mh.FromB58String(v)
		if err != nil {
			return 0, nil, err
		}

		return mb.Base58BTC, c.NewCidV0(hash), nil
	}

	base, data, err := mb.Decode(v)
	if err != nil {
		return 0, nil, err
	}

	cid, err := c.Cast(data)

	return base, cid, err
}

const ERR_STR = "!ERROR!"

func fmtCid(fmtStr string, base mb.Encoding, cid *c.Cid) (string, error) {
	p := cid.Prefix()
	out := new(bytes.Buffer)
	var err error
	for i := 0; i < len(fmtStr); i++ {
		if fmtStr[i] != '%' {
			out.WriteByte(fmtStr[i])
			continue
		}
		i++
		if i >= len(fmtStr) {
			return "", fmt.Errorf("premature end of format string")
		}
		switch fmtStr[i] {
		case '%':
			out.WriteByte('%')
		case 'b': // base name
			out.WriteString(baseToString(base))
		case 'B': // base code
			out.WriteByte(byte(base))
		case 'v': // version string
			fmt.Fprintf(out, "cidv%d", p.Version)
		case 'V': // version num
			fmt.Fprintf(out, "%d", p.Version)
		case 'c': // codec name
			out.WriteString(codecToString(p.Codec))
		case 'C': // codec code
			fmt.Fprintf(out, "%d", p.Codec)
		case 'h': // hash fun name
			out.WriteString(hashToString(p.MhType))
		case 'H': // hash fun code
			fmt.Fprintf(out, "%d", p.MhType)
		case 'L': // hash length
			fmt.Fprintf(out, "%d", p.MhLength)
		case 'm', 'M': // multihash encoded in base %b
			out.WriteString(encode(base, cid.Hash(), fmtStr[i] == 'M'))
		case 'd', 'D': // hash digest encoded in base %b
			dec, err := mh.Decode(cid.Hash())
			if err != nil {
				out.WriteString(ERR_STR)
				errorMsg("%v", err)
				continue
			}
			out.WriteString(encode(base, dec.Digest, fmtStr[i] == 'D'))
		case 's': // cid string encoded in base %b
			str, err := cid.StringOfBase(base)
			if err != nil {
				out.WriteString(ERR_STR)
				errorMsg("%v", err)
				continue
			}
			out.WriteString(str)
		case 'S': // cid string without base prefix
			out.WriteString(encode(base, cid.Bytes(), true))
		case 'P': // prefix
			fmt.Fprintf(out, "cidv%d-%s-%s-%d",
				p.Version,
				codecToString(p.Codec),
				hashToString(p.MhType),
				p.MhLength,
			)
		default:
			return "", fmt.Errorf("unrecognized specifier in format string: %c", fmtStr[i])
		}

	}
	return out.String(), err
}

func baseToString(base mb.Encoding) string {
	// FIXME: Use lookup tables when they are added to go-multibase
	switch base {
	case mb.Base58BTC:
		return "base58btc"
	default:
		return fmt.Sprintf("base?%c", base)
	}
}

func codecToString(num uint64) string {
	name, ok := c.CodecToStr[num]
	if !ok {
		return fmt.Sprintf("codec?%d", num)
	}
	return name
}

func hashToString(num uint64) string {
	name, ok := mh.Codes[num]
	if !ok {
		return fmt.Sprintf("hash?%d", num)
	}
	return name
}

func encode(base mb.Encoding, data []byte, strip bool) string {
	str, err := mb.Encode(base, data)
	if err != nil {
		errorMsg("%v", err)
		return ERR_STR
	}
	if strip {
		return str[1:]
	}
	return str
}

func toCidV0(cid *c.Cid) (*c.Cid, error) {
	if cid.Type() != c.DagProtobuf {
		return nil, fmt.Errorf("can't convert non-protobuf nodes to cidv0")
	}
	return c.NewCidV0(cid.Hash()), nil
}

func toCidV1(cid *c.Cid) (*c.Cid, error) {
	return c.NewCidV1(cid.Type(), cid.Hash()), nil
}
