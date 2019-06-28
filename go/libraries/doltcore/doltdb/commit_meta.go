package doltdb

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

const (
	commitMetaNameKey      = "name"
	commitMetaEmailKey     = "email"
	commitMetaDescKey      = "desc"
	commitMetaTimestampKey = "timestamp"
	commitMetaVersionKey   = "metaversion"

	metaVersion = "1.0"
)

// CommitMeta contains all the metadata that is associated with a commit within an data repo.
type CommitMeta struct {
	Name        string
	Email       string
	Timestamp   uint64
	Description string
}

var milliToNano = uint64(time.Millisecond / time.Nanosecond)
var secToMilli = uint64(time.Second / time.Millisecond)

// NewCommitMeta creates a CommitMeta instance from a name, email, and description and uses the current time for the
// timestamp
func NewCommitMeta(name, email, desc string) (*CommitMeta, error) {
	n := strings.TrimSpace(name)
	e := strings.TrimSpace(email)
	d := strings.TrimSpace(desc)

	if n == "" || e == "" || d == "" {
		return nil, errors.New("Aborting commit due to empty commit message.")
	}

	ns := uint64(time.Now().UnixNano())
	ms := ns / milliToNano

	return &CommitMeta{n, e, ms, d}, nil
}

func getRequiredFromSt(st types.Struct, k string) (types.Value, error) {
	if v, ok := st.MaybeGet(k); ok {
		return v, nil
	}

	return nil, errors.New("Missing required field \"" + k + "\".")
}

func commitMetaFromNomsSt(st types.Struct) (*CommitMeta, error) {
	e, err := getRequiredFromSt(st, commitMetaEmailKey)

	if err != nil {
		return nil, err
	}

	n, err := getRequiredFromSt(st, commitMetaNameKey)

	if err != nil {
		return nil, err
	}

	d, err := getRequiredFromSt(st, commitMetaDescKey)

	if err != nil {
		return nil, err
	}

	ts, err := getRequiredFromSt(st, commitMetaTimestampKey)

	if err != nil {
		return nil, err
	}

	return &CommitMeta{
		string(n.(types.String)),
		string(e.(types.String)),
		uint64(ts.(types.Uint)),
		string(d.(types.String)),
	}, nil
}

func (cm *CommitMeta) toNomsStruct() types.Struct {
	metadata := types.StructData{
		commitMetaNameKey:      types.String(cm.Name),
		commitMetaEmailKey:     types.String(cm.Email),
		commitMetaDescKey:      types.String(cm.Description),
		commitMetaTimestampKey: types.Uint(cm.Timestamp),
		commitMetaVersionKey:   types.String(metaVersion),
	}

	return types.NewStruct(types.Format_7_18, "metadata", metadata)
}

// FormatTS takes the internal timestamp and turns it into a human readable string in the time.RubyDate format
// which looks like: "Mon Jan 02 15:04:05 -0700 2006"
func (cm *CommitMeta) FormatTS() string {
	seconds := cm.Timestamp / secToMilli
	nanos := (cm.Timestamp % secToMilli) * milliToNano
	now := time.Unix(int64(seconds), int64(nanos))

	return now.Format(time.RubyDate)
}

// String returns the human readable string representation of the commit data
func (cm *CommitMeta) String() string {
	return fmt.Sprintf("name: %s, email: %s, timestamp: %s, description: %s", cm.Name, cm.Email, cm.FormatTS(), cm.Description)
}
