package doltdb

import (
	"strings"
)

// NewTagMeta returns CommitMeta that can be used to create a tag.
func NewTagMeta(name, email, desc string) *CommitMeta {
	n := strings.TrimSpace(name)
	e := strings.TrimSpace(email)
	d := strings.TrimSpace(desc)

	ns := uint64(CommitNowFunc().UnixNano())
	ms := ns / uMilliToNano

	userMS := int64(ns) / milliToNano

	return &CommitMeta{n, e, ms, d, userMS}
}
