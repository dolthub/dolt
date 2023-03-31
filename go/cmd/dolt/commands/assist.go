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
	"os/exec"
	"strings"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/go-mysql-server/sql"
   "github.com/google/shlex"
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
	
	query := apr.GetValueOrDefault("query", "what can you tell me about my database?")
	model := apr.GetValueOrDefault("model", "gpt-3.5-turbo")
	
	response, err := queryGpt(ctx, dEnv, apiKey, model, query)
	if err != nil {
		return 1
	}
	
	err = handleResponse(ctx, response)
	if err != nil {
		return 1
	}
	
	return 0
}

var chatGptJsonHeader = `{
    "model": "%s",
    "messages": [`

var chatGptJsonFooter = `]}`

var messageJson = `{"role": "%s", "content": "%s"}`

func handleResponse(ctx context.Context, response string) error {
	var respJson map[string]interface{}
	err := json.Unmarshal([]byte(response), &respJson)
	if err != nil {
		return err
	}
	
	if respJson["choices"] != nil {
		for _, choice := range respJson["choices"].([]interface{}) {
			msg := choice.(map[string]interface{})["message"].(map[string]interface{})
			innerContent := msg["content"].(string)
			
			// attempt to interpret this as a well formed json command
			var innerRespJson map[string]interface{}
			err := json.Unmarshal([]byte(innerContent), &innerRespJson)
			if err != nil {
				return textResponse(innerContent)
			}
			
			action, ok := innerRespJson["action"].(string)
			if !ok {
				return textResponse(innerContent)
			}
			content, ok := innerRespJson["content"].(string)
			if !ok {
				return textResponse(innerContent)
			}
			
			switch action {
			case "DOLT_EXEC":
				_, err = doltExec(ctx, content)
				return err
			case "DOLT_QUERY":
				return doltQuery(ctx, content)
			case "SQL_QUERY":
				return sqlQuery(ctx, content)
			case "ANSWER":
				return textResponse(content)
			default:
				return textResponse(content)
			}
		}
	}

	return textResponse(fmt.Sprintf("error: couldn't interpret response: %s", response))
}

func sqlQuery(ctx context.Context, content string) error {
	_, err := doltExec(ctx, fmt.Sprintf("dolt sql -q \"%s\"", content))
	return err
}

func doltQuery(ctx context.Context, content string) error {
	_, err := doltExec(ctx, content)
	if err != nil {
		return err
	}
	
	// TODO: feed output back into chatgpt
	return nil
}

func doltExec(ctx context.Context, commandString string) (string, error) {
	command := strings.TrimSpace(commandString)
	if !strings.HasPrefix(command, "dolt") {
		return "", textResponse(commandString)
	}
	command = strings.TrimPrefix(command, "dolt ")

	output, err := runDolt(ctx, command)

	cli.Println(commandString)
	cli.Println()
	cli.Println(output)

	// this is delayed error handling from running the dolt command
	if err != nil {
		return "", err
	}

	return output, nil
}

func textResponse(content string) error {
	cli.Println(content)
	return nil
}

func queryGpt(ctx context.Context, dEnv *env.DoltEnv, apiKey, modelId, query string) (string, error) {
	sqlEng, dbName, err := engine.NewSqlEngineForEnv(ctx, dEnv)
	if err != nil {
		return "", err
	}

	sqlCtx, err := sqlEng.NewLocalContext(ctx)
	if err != nil {
		return "", errhand.VerboseErrorFromError(err)
	}
	sqlCtx.SetCurrentDatabase(dbName)
	
	prompt, err := getJsonPrompt(sqlCtx, sqlEng, dEnv, modelId, query)
	if err != nil {
		return "", err
	}
	
	// cli.Println(prompt)

	url := "https://api.openai.com/v1/chat/completions"
	client := &http.Client{}

	// TODO: allow cancelation via SIGINT
	
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

func getJsonPrompt(ctx *sql.Context, sqlEngine *engine.SqlEngine, dEnv *env.DoltEnv, modelId string, query string) (io.Reader, error) {
	sb := strings.Builder{}

	createTableStatements, err := getCreateTableStatements(ctx, sqlEngine, dEnv)
	if err != nil {
		return nil, err
	}

	sb.WriteString(fmt.Sprintf(chatGptJsonHeader, modelId))
	
	writeJsonMessage(&sb, "system", "You are an expert dolt user who helps other users understand, query, and manage their dolt databases.")
	sb.WriteRune(',')
	writeJsonMessage(&sb, "user", "I'm going to give you some information about my database before I ask anything, OK?")
	sb.WriteRune(',')
	writeJsonMessage(&sb, "assistant", "I understand. Please tell me the schema of all tables as CREATE TABLE statements.")
	sb.WriteRune(',')
	writeJsonMessage(&sb, "user", fmt.Sprintf("CREATE TABLE statements for the database are as follows: %s", createTableStatements))
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
	
  responseJson, err := json.Marshal(map[string]string{"action": "DOLT_QUERY", "content": "dolt log -n 1"})
	if err != nil {
		return nil, err
	}
	
	sb.WriteRune(',')
	writeJsonMessage(&sb, "assistant", string(responseJson))
	
	logOutput, err := runDolt(ctx, "log -n 1")
	if err != nil {
		return nil, err
	}

	sb.WriteRune(',')
	writeJsonMessage(&sb, "user", logOutput)
	
	commit, err := dEnv.HeadCommit(ctx)
	if err != nil {
		return nil, err
	}

	cm, err := commit.GetCommitMeta(ctx)
	if err != nil {
		return nil, err
	}

	responseJson, err = json.Marshal(map[string]string{"action": "ANSWER", "content": fmt.Sprintf("The most recent commit was written by %s", cm.Name)})
	if err != nil {
		return nil, err
	}
	
	sb.WriteRune(',')
	writeJsonMessage(&sb, "assistant", string(responseJson))

	sb.WriteRune(',')
	writeJsonMessage(&sb, "user", "write a SQL query that shows me the five most recent commits on the current branch")

	responseJson, err = json.Marshal(map[string]string{"action": "SQL_QUERY", "content": "SELECT * FROM DOLT_LOG order by date LIMIT 5"})
	if err != nil {
		return nil, err
	}

	sb.WriteRune(',')
	writeJsonMessage(&sb, "assistant", string(responseJson))

	sb.WriteRune(',')
	writeJsonMessage(&sb, "user", "check out a new branch named feature2 two commits before the head of the current branch")

	responseJson, err = json.Marshal(map[string]string{"action": "DOLT_EXEC", "content": "dolt checkout -b feature2 HEAD~2"})
	if err != nil {
		return nil, err
	}
	
	sb.WriteRune(',')
	writeJsonMessage(&sb, "assistant", string(responseJson))


	sb.WriteRune(',')
	writeJsonMessage(&sb, "user", "what changed in the last 3 commits?")

	responseJson, err = json.Marshal(map[string]string{"action": "DOLT_EXEC", "content": "dolt diff HEAD~3 HEAD"})
	if err != nil {
		return nil, err
	}

	sb.WriteRune(',')
	writeJsonMessage(&sb, "assistant", string(responseJson))

	sb.WriteRune(',')
	writeJsonMessage(&sb, "user", "create a new table for storing log events")

	responseJson, err = json.Marshal(map[string]string{"action": "SQL_QUERY", "content": "CREATE TABLE log_events (id int, event_time timestamp, description varchar(255))"})
	if err != nil {
		return nil, err
	}

	sb.WriteRune(',')
	writeJsonMessage(&sb, "assistant", string(responseJson))
	
	sb.WriteRune(',')
	userQuery, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}
	
	writeJsonMessage(&sb, "user", string(userQuery))

	sb.WriteString(chatGptJsonFooter)

	return strings.NewReader(sb.String()), nil
}

// TODO: rather than forking a new process and getting its output, instantiate command structs and run in-process here
func runDolt(ctx context.Context, command string) (string, error) {
	args, err := shlex.Split(command)
	if err != nil {
		return "", err
	}
	
	cmd := exec.CommandContext(ctx, "dolt", args...)
	
	output, err := cmd.CombinedOutput()
	if err != nil {
		return string(output), err
	}
	
	return string(output), nil
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
		sch, iter, err := sqlEngine.Query(ctx, fmt.Sprintf("SHOW CREATE TABLE %s", sql.QuoteIdentifier(table)))
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
	ap.SupportsString("query", "q", "query to ask the assistant", "Query to ask the assistant")
	ap.SupportsString("model", "m", "open AI model id", "The ID of the Open AI model to use for the assistant")
	return ap
}