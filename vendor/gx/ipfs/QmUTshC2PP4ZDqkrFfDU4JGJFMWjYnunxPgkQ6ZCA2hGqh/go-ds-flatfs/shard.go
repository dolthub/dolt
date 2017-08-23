package flatfs

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

var IPFS_DEF_SHARD = NextToLast(2)
var IPFS_DEF_SHARD_STR = IPFS_DEF_SHARD.String()

const PREFIX = "/repo/flatfs/shard/"

const SHARDING_FN = "SHARDING"
const README_FN = "_README"

type ShardIdV1 struct {
	funName string
	param   int
	fun     ShardFunc
}

func (f *ShardIdV1) String() string {
	return fmt.Sprintf("%sv1/%s/%d", PREFIX, f.funName, f.param)
}

func (f *ShardIdV1) Func() ShardFunc {
	return f.fun
}

func Prefix(prefixLen int) *ShardIdV1 {
	padding := strings.Repeat("_", prefixLen)
	return &ShardIdV1{
		funName: "prefix",
		param:   prefixLen,
		fun: func(noslash string) string {
			return (noslash + padding)[:prefixLen]
		},
	}
}

func Suffix(suffixLen int) *ShardIdV1 {
	padding := strings.Repeat("_", suffixLen)
	return &ShardIdV1{
		funName: "suffix",
		param:   suffixLen,
		fun: func(noslash string) string {
			str := padding + noslash
			return str[len(str)-suffixLen:]
		},
	}
}

func NextToLast(suffixLen int) *ShardIdV1 {
	padding := strings.Repeat("_", suffixLen+1)
	return &ShardIdV1{
		funName: "next-to-last",
		param:   suffixLen,
		fun: func(noslash string) string {
			str := padding + noslash
			offset := len(str) - suffixLen - 1
			return str[offset : offset+suffixLen]
		},
	}
}

func ParseShardFunc(str string) (*ShardIdV1, error) {
	str = strings.TrimSpace(str)

	if len(str) == 0 {
		return nil, fmt.Errorf("empty shard identifier")
	}

	trimmed := strings.TrimPrefix(str, PREFIX)
	if str == trimmed { // nothing trimmed
		return nil, fmt.Errorf("invalid or no prefix in shard identifier: %s", str)
	}
	str = trimmed

	parts := strings.Split(str, "/")
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid shard identifier: %s", str)
	}

	version := parts[0]
	if version != "v1" {
		return nil, fmt.Errorf("expected 'v1' for version string got: %s\n", version)
	}

	funName := parts[1]

	param, err := strconv.Atoi(parts[2])
	if err != nil {
		return nil, fmt.Errorf("invalid parameter: %v", err)
	}

	switch funName {
	case "prefix":
		return Prefix(param), nil
	case "suffix":
		return Suffix(param), nil
	case "next-to-last":
		return NextToLast(param), nil
	default:
		return nil, fmt.Errorf("expected 'prefix', 'suffix' or 'next-to-last' got: %s", funName)
	}

}

func ReadShardFunc(dir string) (*ShardIdV1, error) {
	buf, err := ioutil.ReadFile(filepath.Join(dir, SHARDING_FN))
	if os.IsNotExist(err) {
		return nil, ErrShardingFileMissing
	} else if err != nil {
		return nil, err
	}
	return ParseShardFunc(string(buf))
}

func WriteShardFunc(dir string, id *ShardIdV1) error {
	file, err := os.OpenFile(filepath.Join(dir, SHARDING_FN), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.WriteString(id.String())
	if err != nil {
		return err
	}
	_, err = file.WriteString("\n")
	return err
}

func WriteReadme(dir string, id *ShardIdV1) error {
	if id.String() == IPFS_DEF_SHARD.String() {
		err := ioutil.WriteFile(filepath.Join(dir, README_FN), []byte(README_IPFS_DEF_SHARD), 0444)
		if err != nil {
			return err
		}
	}
	return nil
}
