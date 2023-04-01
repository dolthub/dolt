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
	"time"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/google/shlex"
)

type Assist struct {
	messages []string
}

var _ cli.Command = &Assist{}

func (a Assist) Name() string {
	return "assist"
}

func (a Assist) Description() string {
	return "Provides assistance with Dolt commands and queries."
}

func (a *Assist) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	a.messages = make([]string, 0)
	
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
	debug := apr.Contains("debug")

	sqlEng, dbName, err := engine.NewSqlEngineForEnv(ctx, dEnv)
	if err != nil {
		return 1
	}

	sqlCtx, err := sqlEng.NewLocalContext(ctx)
	if err != nil {
		return 1
	}
	sqlCtx.SetCurrentDatabase(dbName)

	a.messages, err = getInitialPrompt(sqlCtx, sqlEng, dEnv)
	if err != nil {
		return 1
	}

	cont := true
	for cont {
		response, err := a.queryGpt(ctx, apiKey, model, query, debug)
		if err != nil {
			return 1
		}

		var userOutput string
		userOutput, cont, err = a.handleResponse(ctx, response, debug)
		if err != nil {
			cli.PrintErrln(err.Error())
			return 1
		}

		query = userOutput
	}
	
	return 0
}

var chatGptJsonHeader = `{
    "model": "%s",
    "messages": [`

var chatGptJsonFooter = `]}`

var messageJson = `{"role": "%s", "content": "%s"}`

func (a *Assist) handleResponse(ctx context.Context, response string, debug bool) (string, bool, error) {
	if debug {
		cli.Println(fmt.Sprintf("Assistant response: %s", response))
	}

	var respJson map[string]interface{}
	err := json.Unmarshal([]byte(response), &respJson)
	if err != nil {
		return "", false, err
	}
	
	if respJson["choices"] != nil {
		for _, choice := range respJson["choices"].([]interface{}) {
			msg := choice.(map[string]interface{})["message"].(map[string]interface{})
			innerContent := msg["content"].(string)
			
			// update our conversation log in case we want to continue it
			mustAppendJson(a.messages, "assistant", innerContent)
			
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
				return doltExec(ctx, content, true)
			case "DOLT_QUERY":
				return doltQuery(ctx, content)
			case "SQL_QUERY":
				return sqlQuery(ctx, content)
			case "ANSWER":
				return textResponse(content)
			default:
				return textResponse(content)
			}

			// should only be one choice, but just in case
			break
		}
	}

	return textResponse(fmt.Sprintf("error: couldn't interpret response: %s", response))
}

func sqlQuery(ctx context.Context, query string) (string, bool, error) {
	cli.Println(fmt.Sprintf("Runnning query \"%s\"...", query))
	
	output, _, err := doltExec(ctx, fmt.Sprintf("dolt sql -q \"%s\"", query), false)
	if err != nil {
		return "", false, err
	}

	return output, false, nil
}

func doltQuery(ctx context.Context, content string) (string, bool, error) {
	output, _, err := doltExec(ctx, content, true)
	if err != nil {
		return "", false, err
	}
	
	return output, true, nil
}

func doltExec(ctx context.Context, commandString string, echoCommand bool) (string, bool, error) {
	command := strings.TrimSpace(commandString)
	if !strings.HasPrefix(command, "dolt") {
		return textResponse(commandString)
	}
	command = strings.TrimPrefix(command, "dolt ")

	output, err := runDolt(ctx, command)

	if echoCommand {
		cli.Println(commandString)
		cli.Println()
	}
	
	cli.Println(output)

	// this is delayed error handling from running the dolt command
	if err != nil {
		return "", false, err
	}

	return output, false, nil
}

func textResponse(content string) (string, bool, error) {
	cli.Println(content)
	return "", false, nil
}

func (a *Assist) queryGpt(ctx context.Context, apiKey, modelId, query string, debug bool) (string, error) {
	prompt, err := a.getJsonPrompt(ctx, modelId, query)
	if err != nil {
		return "", err
	}
	
	if debug {
		cli.Println(prompt)
	}

	url := "https://api.openai.com/v1/chat/completions"
	client := &http.Client{}
	
	req, err := http.NewRequest("POST", url, prompt)
	if err != nil {	
		return "", err
	}

	req.Header.Add("Content-Type", `application/json`)
	req.Header.Add("Authorization", "Bearer " + apiKey)

	respChan := make(chan string)
	errChan := make(chan error)
	go func() {
		response, err := client.Do(req)
		if err != nil {
			errChan <- err
			close(errChan)
		}

		body, err := io.ReadAll(response.Body)
		if err != nil {
			errChan <- err
			close(errChan)
		}
		
		respChan <- string(body)
		close(respChan)
	}()

	spinner := TextSpinner{}
	cli.Print(spinner.next())
	defer func() {
		cli.DeleteAndPrint(1, "")
	}()
	
	for {
		select {
		case resp := <-respChan:
			return resp, nil
		case err := <-errChan:
			return "", err
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(50 * time.Millisecond):
			cli.DeleteAndPrint(1, spinner.next())
		}
	}
}

func (a *Assist) getJsonPrompt(ctx context.Context, modelId string, query string) (io.Reader, error) {
	sb := strings.Builder{}
	
	sb.WriteString(fmt.Sprintf(chatGptJsonHeader, modelId))

	for i, msg := range a.messages {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(msg)
	}
	
	msg, err := jsonMessage("user", query)
	if err != nil {
		return nil, err
	}
	
	a.messages = append(a.messages, string(msg))

	sb.WriteString(",")
	sb.WriteString(string(msg))
	sb.WriteString(chatGptJsonFooter)

	return strings.NewReader(sb.String()), nil
}

func getInitialPrompt(ctx *sql.Context, sqlEngine *engine.SqlEngine, dEnv *env.DoltEnv) ([]string, error) {
	createTableStatements, err := getCreateTableStatements(ctx, sqlEngine, dEnv)
	if err != nil {
		return nil, err
	}
	
	var messages []string

	messages = mustAppendJson(messages,  "system", "You are an expert dolt user who helps other users understand, query, and manage their dolt databases.")
	messages = mustAppendJson(messages,  "user", "I'm going to give you some information about my database before I ask anything, OK?")
	messages = mustAppendJson(messages,  "assistant", "I understand. Please tell me the schema of all tables as CREATE TABLE statements.")
	messages = mustAppendJson(messages,  "user", fmt.Sprintf("CREATE TABLE statements for the database are as follows: %s", createTableStatements))
	messages = mustAppendJson(messages,  "assistant", fmt.Sprintf("Thank you, I'll refer to these schemas " +
			"during our talk. Since we are talking over text, for the rest of this conversation, I'll respond in a machine readable " +
			"format so that you can easily consume it. I'll use JSON for my response like this: " +
			"{\"action\": \"DOLT_QUERY\", \"content\": \"dolt log -n 1\"}. " +
			"For example, this response means that I want you to run the dolt command 'dolt log -n 1' and tell me what it says. " +
			"Let's try a few more. You ask me some questions and I'll give you some responses in JSON. We'll just keep doing" +
			" that. Go ahead when you're ready."))

	messages = mustAppendJson(messages,  "user", fmt.Sprintf("who wrote the most recent commit?"))

	responseJson, err := json.Marshal(map[string]string{"action": "DOLT_QUERY", "content": "dolt log -n 1"})
	if err != nil {
		return nil, err
	}

	messages = mustAppendJson(messages,  "assistant", string(responseJson))

	logOutput, err := runDolt(ctx, "log -n 1")
	if err != nil {
		return nil, err
	}

	messages = mustAppendJson(messages,  "user", logOutput)

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

	messages = mustAppendJson(messages,  "assistant", string(responseJson))

	messages = mustAppendJson(messages,  "user", "write a SQL query that shows me the five most recent commits on the current branch")

	responseJson, err = json.Marshal(map[string]string{"action": "SQL_QUERY", "content": "SELECT * FROM DOLT_LOG order by date LIMIT 5"})
	if err != nil {
		return nil, err
	}

	messages = mustAppendJson(messages,  "assistant", string(responseJson))

	messages = mustAppendJson(messages,  "user", "check out a new branch named feature2 two commits before the head of the current branch")

	responseJson, err = json.Marshal(map[string]string{"action": "DOLT_EXEC", "content": "dolt checkout -b feature2 HEAD~2"})
	if err != nil {
		return nil, err
	}

	messages = mustAppendJson(messages,  "assistant", string(responseJson))


	messages = mustAppendJson(messages,  "user", "what changed in the last 3 commits?")

	responseJson, err = json.Marshal(map[string]string{"action": "DOLT_EXEC", "content": "dolt diff HEAD~3 HEAD"})
	if err != nil {
		return nil, err
	}

	messages = mustAppendJson(messages,  "assistant", string(responseJson))

	messages = mustAppendJson(messages,  "user", "create a new table for storing log events")

	responseJson, err = json.Marshal(map[string]string{"action": "SQL_QUERY", "content": "CREATE TABLE log_events (id int, event_time timestamp, description varchar(255))"})
	if err != nil {
		return nil, err
	}

	messages = mustAppendJson(messages,  "assistant", string(responseJson))

	return messages, nil
}

func mustAppendJson(messages []string, role string, content string) []string {
	msg, err := jsonMessage(role, content)
	if err != nil {
		return messages
	}
	return append(messages, string(msg))
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

func jsonMessage(role string, content string) ([]byte, error) {
	jsonString, err := json.Marshal(map[string]string{"role": role, "content": content})
	if err != nil {
		return nil, err
	}
	return jsonString, nil
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
	ap.SupportsFlag("debug", "d",  "log API requests to and from the assistant")
	return ap
}

func (ts *TextSpinner) next() string {
	now := time.Now()
	if now.Sub(ts.lastUpdate) > minUpdate {
		ts.seqPos = (ts.seqPos + 1) % len(spinnerSeq)
		ts.lastUpdate = now
	}

	return string([]rune{spinnerSeq[ts.seqPos]})
}

const minUpdate = 100 * time.Millisecond

var spinnerSeq = []rune{'|', '/', '-', '\\'}

type TextSpinner struct {
	seqPos     int
	lastUpdate time.Time
}


