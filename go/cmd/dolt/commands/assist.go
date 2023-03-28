// Copyright 2022 Dolthub, Inc.
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

package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/apache/arrow/go/arrow/decimal128"
	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/go-mysql-server/sql"
)

type Assist struct {}

var _ cli.Command = Assist{}

func (a Assist) Name() string {
	return "assist"
}

func (a Assist) Description() string {
	return "Provides assistance with Dolt commands and queries."
}

func (a Assist) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	apiKey, ok := os.LookupEnv("OPENAI_API_KEY")
	if !ok {
		cli.PrintErrln("Could not find OpenAI API key. Please set the OPENAI_API_KEY environment variable.")
		return 1
	}
	
	ap := a.ArgParser()
	helpPr, _ := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, addDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, helpPr)
	
	apr.GetValueOrDefault("mode", "execute")
	
	response, err := queryGpt(ctx, dEnv, apiKey)
	if err != nil {
		return 1
	}
	
	err = handleResponse(response)
	if err != nil {
		return 1
	}
	
	return 0
}

var chatGptJsonHeader = `{
    "model": "gpt-3.5-turbo",
    "messages": [`

var chatGptJsonFooter = `]}`

var messageJson = `{"role": "%s", "content": "%s"}`

func handleResponse(response string) error {
	
}

func queryGpt(ctx context.Context, dEnv *env.DoltEnv, apiKey string) (string, error) {
	sqlEng, dbName, err := engine.NewSqlEngineForEnv(ctx, dEnv)
	if err != nil {
		return "", err
	}

	sqlCtx, err := sqlEng.NewLocalContext(ctx)
	if err != nil {
		return "", errhand.VerboseErrorFromError(err)
	}
	sqlCtx.SetCurrentDatabase(dbName)

	prompt, err := getJsonPrompt(sqlCtx, sqlEng, dEnv)
	if err != nil {
		return "", err
	}
	
	cli.Println(prompt)

	url := "https://api.openai.com/v1/chat/completions"
	client := &http.Client{}

	req, err := http.NewRequest("POST", url, prompt)
	if err != nil {	
		return "", err
	}

	req.Header.Add("Content-Type", `application/json`)
	req.Header.Add("Authorization", "Bearer " + apiKey)

	response, err := client.Do(req)
	if err != nil {
		return "", err
	}

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return "", err
	}
	
	return string(body), nil
}

func getJsonPrompt(ctx *sql.Context, sqlEngine *engine.SqlEngine, dEnv *env.DoltEnv) (io.Reader, error) {
	sb := strings.Builder{}

	statements, err := getCreateTableStatements(ctx, sqlEngine, dEnv)
	if err != nil {
		return nil, err
	}

	sb.WriteString(chatGptJsonHeader)
	writeJsonMessage(&sb, "system", "You are an expert dolt user who helps other users understand, query, and manage their dolt databases."))
	sb.WriteRune(',')
	writeJsonMessage(&sb, "user", "I'm going to give you some information about my database before I ask anything, OK?"))
	sb.WriteRune(',')
	writeJsonMessage(&sb, "assistant", "I understand. Please tell me the schema of all tables as CREATE TABLE statements."))
	sb.WriteRune(',')
	writeJsonMessage(&sb, "user", fmt.Sprintf("CREATE TABLE statements for the database are as follows: %s", statements))
	sb.WriteRune(',')
	writeJsonMessage(&sb, "assistant", fmt.Sprintf("Thank you, I'll refer to these schemas " +
		"during our talk. Since we are talking over text, for the rest of this conversation, I'll respond in a machine readable " +
		"format so that you can easily consume it. I'll use JSON for my response like this: " +
		"{\"action\": \"DOLT_QUERY\", \"content\": \"dolt log -n 1\"}. " +
		"For example, this response means that I want you to run the dolt command 'dolt log -n 1' and tell me what it says. " +
		"Let's try a few more. You ask me some questions and I'll give you some responses in JSON. We'll just keep doing" +
		" that. Go ahead when you're ready."))

	sb.WriteRune(',')
	writeJsonMessage(&sb, "user", fmt.Sprintf("who wrote the most recent commit?"))
	sb.WriteRune(',')
	responseJson, err := json.Marshal(map[string]string{"action": "DOLT_QUERY", "content": "dolt log -n 1"})
	if err != nil {
		return nil, err
	}
	
	writeJsonMessage(&sb, "assistant", string(responseJson))

	sb.WriteRune(',')
	writeJsonMessage(&sb, "user", fmt.Sprintf("who wrote the most recent commit?"))
	sb.WriteRune(',')
	responseJson, err := json.Marshal(map[string]string{"action": "DOLT_QUERY", "content": "dolt log -n 1"})
	if err != nil {
		return nil, err
	}
	
var logOutput := ` commit l2dqemamag9oq28aeam6323sgc4317sj (HEAD -> feature, main, origin/main)
 Author: timsehn <tim@dolthub.com>
 Date:  Thu Feb 02 14:49:26 -0800 2023

     Initial import of employees test db
`


	sb.WriteString(chatGptJsonFooter)

	return strings.NewReader(sb.String()), nil
}

func writeJsonMessage(sb *strings.Builder, role string, content string) error {
	jsonString, err := json.Marshal(map[string]string{"role": role, "content": content})
	if err != nil {
		return err
	}
	
	sb.WriteString(string(jsonString))
	return nil
}

func getCreateTableStatements(ctx *sql.Context, sqlEngine *engine.SqlEngine, dEnv *env.DoltEnv) (string, error) {
	sb := strings.Builder{}

	root, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return "", err
	}
	
	tables, err := root.GetTableNames(ctx)
	for _, table := range tables {
		sch, iter, err := sqlEngine.Query(ctx, fmt.Sprintf("SHOW CREATE TABLE %s", table)
		if err != nil {
			return "", err
		}
		rows, err := sql.RowIterToRows(ctx, sch, iter)
		if err != nil {
			return "", err
		}
		
		createTable := rows[0][1].(string)
		sb.WriteString(createTable)
		sb.WriteString("\n\n")
	}

	return sb.String(), nil
	
}

func (a Assist) Docs() *cli.CommandDocumentation {
	return &cli.CommandDocumentation{
		CommandStr: "dolt assist",
		ShortDesc:  "Provides assistance with Dolt commands and queries.",
		LongDesc:   `{{.EmphasisLeft}}dolt assist{{.EmphasisRight}} provides assistance with Dolt commands and queries.`,
		Synopsis: []string{
			"{{.LessThan}}command{{.GreaterThan}}",
		},
		ArgParser: a.ArgParser(),
	}
}

func (a Assist) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsString("mode", "m", "mode", "The mode of assistance to provide.  Valid values are 'command' and 'query'.")
	return ap
}

