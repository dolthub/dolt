// Copyright 2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlserver

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"
)

type LogFormat struct{}

func (l LogFormat) Format(entry *logrus.Entry) ([]byte, error) {
	lvl := ""
	switch entry.Level {
	case logrus.PanicLevel:
		lvl = "PANIC"
	case logrus.FatalLevel:
		lvl = "FATAL"
	case logrus.ErrorLevel:
		lvl = "ERROR"
	case logrus.WarnLevel:
		lvl = "WARN"
	case logrus.InfoLevel:
		lvl = "INFO"
	case logrus.DebugLevel:
		lvl = "DEBUG"
	case logrus.TraceLevel:
		lvl = "TRACE"
	}

	connectionId := entry.Data[sql.ConnectionIdLogField]
	delete(entry.Data, sql.ConnectionIdLogField)

	var dataFormat strings.Builder
	var i int
	for _, k := range sortedKeys(entry.Data) {
		if i > 0 {
			dataFormat.WriteString(", ")
		}
		i++

		dataFormat.WriteString(k)
		dataFormat.WriteString("=")
		v := entry.Data[k]
		switch v := v.(type) {
		case time.Time:
			dataFormat.WriteString(v.Format(time.RFC3339))
		default:
			dataFormat.WriteString(fmt.Sprintf("%v", v))
		}
	}

	msg := fmt.Sprintf("%s %s [conn %d] %s {%s}\n", entry.Time.Format(time.RFC3339), lvl, connectionId, entry.Message, dataFormat.String())
	return ([]byte)(msg), nil
}

func sortedKeys(m map[string]interface{}) []string {
	keys := make([]string, len(m))
	i := 0
	for k := range m {
		keys[i] = k
		i++
	}
	sort.Strings(keys)
	return keys
}
