// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package spec

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/types"
	flag "github.com/juju/gnuflag"
)

const CommitMetaDateFormat = time.RFC3339

var (
	commitMetaDate            string
	commitMetaMessage         string
	commitMetaKeyValueStrings string
	commitMetaKeyValuePaths   string
)

// RegisterCommitMetaFlags registers command line flags used for creating commit meta structs.
func RegisterCommitMetaFlags(flags *flag.FlagSet) {
	flags.StringVar(&commitMetaDate, "date", "", "alias for -meta 'date=<date>'. '<date>' must be iso8601-formatted. If '<date>' is empty, it defaults to the current date.")
	flags.StringVar(&commitMetaMessage, "message", "", "alias for -meta 'message=<message>'")
	flags.StringVar(&commitMetaKeyValueStrings, "meta", "", "'<key>=<value>' - creates a metadata field called 'key' set to 'value'. Value should be human-readable encoded.")
	flags.StringVar(&commitMetaKeyValuePaths, "meta-p", "", "'<key>=<path>' - creates a metadata field called 'key' set to the value at <path>")
}

// CreateCommitMetaStruct creates and returns a Noms struct suitable for use in CommitOptions.Meta.
// It returns types.EmptyStruct and an error if any issues are encountered.
// Database is used only if commitMetaKeyValuePaths are provided on the command line and values need to be resolved.
// Date should be ISO 8601 format (see CommitMetaDateFormat), if empty the current date is used.
// The values passed as command line arguments (if any) are merged with the values provided as function arguments.
func CreateCommitMetaStruct(db datas.Database, date, message string, keyValueStrings map[string]string, keyValuePaths map[string]types.Value) (types.Struct, error) {
	metaValues := types.StructData{}

	resolvePathFunc := func(path string) (types.Value, error) {
		absPath, err := NewAbsolutePath(path)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Bad path for meta-p: %s", path))
		}
		return absPath.Resolve(db), nil
	}
	parseMetaStrings := func(param string, resolveAsPaths bool) error {
		if param == "" {
			return nil
		}
		ms := strings.Split(param, ",")
		for _, m := range ms {
			kv := strings.Split(m, "=")
			if len(kv) != 2 {
				return errors.New(fmt.Sprintf("Unable to parse meta value: %s", m))
			}
			if !types.IsValidStructFieldName(kv[0]) {
				return errors.New(fmt.Sprintf("Invalid meta key: %s", kv[0]))
			}
			if resolveAsPaths {
				v, err := resolvePathFunc(kv[1])
				if err != nil {
					return err
				}
				metaValues[kv[0]] = v
			} else {
				metaValues[kv[0]] = types.String(kv[1])
			}
		}
		return nil
	}

	if err := parseMetaStrings(commitMetaKeyValueStrings, false); err != nil {
		return types.EmptyStruct, err
	}
	if err := parseMetaStrings(commitMetaKeyValuePaths, true); err != nil {
		return types.EmptyStruct, err
	}

	for k, v := range keyValueStrings {
		if !types.IsValidStructFieldName(k) {
			return types.EmptyStruct, errors.New(fmt.Sprintf("Invalid meta key: %s", k))
		}
		metaValues[k] = types.String(v)
	}
	for k, v := range keyValuePaths {
		if !types.IsValidStructFieldName(k) {
			return types.EmptyStruct, errors.New(fmt.Sprintf("Invalid meta key: %s", k))
		}
		metaValues[k] = v
	}

	if date == "" {
		date = commitMetaDate
	}
	if date == "" {
		date = time.Now().UTC().Format(CommitMetaDateFormat)
	} else {
		_, err := time.Parse(CommitMetaDateFormat, date)
		if err != nil {
			return types.EmptyStruct, errors.New(fmt.Sprintf("Unable to parse date: %s, error: %s", date, err))
		}
	}
	metaValues["date"] = types.String(date)

	if message != "" {
		metaValues["message"] = types.String(message)
	} else if commitMetaMessage != "" {
		metaValues["message"] = types.String(commitMetaMessage)
	}
	return types.NewStruct("Meta", metaValues), nil
}
