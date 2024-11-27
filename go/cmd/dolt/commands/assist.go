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
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/google/shlex"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/libraries/doltcore/dconfig"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var assistDocs = cli.CommandDocumentationContent{
	ShortDesc: "Assists with dolt commands and queries.",
	LongDesc: `Assists with dolt commands and queries. Can run dolt commands or SQL queries on your behalf based on your questions or instructions, as well as answer questions about your database.

Powered by OpenAI's chat API. An API key is required. Please set the OPENAI_API_KEY environment variable. 
`,
	Synopsis: []string{
		"[--debug] [--model {{.LessThan}}modelId{{.GreaterThan}}]",
	},
}

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

func (a Assist) Hidden() bool {
	return true
}

func (a *Assist) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	a.messages = make([]string, 0)

	apiKey, ok := os.LookupEnv(dconfig.EnvOpenAiKey)
	if !ok {
		cli.PrintErrln("Could not find OpenAI API key. Please set the OPENAI_API_KEY environment variable.")
		return 1
	}

	ap := a.ArgParser()
	helpPr, _ := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, addDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, helpPr)

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

	scanner := bufio.NewScanner(cli.InStream)
	cli.Println("# Welcome to the Dolt Assistant, powered by ChatGPT.\n# Type your question or command, or exit to quit.")
	cli.Println("")

	if !agreeToTerms(scanner) {
		return 0
	}

	cli.Print("> ")

	scanner.Scan()
	input := strings.TrimSpace(scanner.Text())
	if input == "exit" {
		return 0
	}
	query := input

	cont := true
	for {
		// Prompt for user input if the last response was terminal
		if !cont {
			cli.Print("\n> ")
			scanner.Scan()
			input := strings.TrimSpace(scanner.Text())
			if input == "exit" {
				return 0
			}
			cli.Println("")
			query = input
		}

		response, err := a.queryGpt(ctx, apiKey, model, query, debug)
		if err != nil {
			return 1
		}

		var userOutput string
		userOutput, cont, err = a.handleResponse(ctx, response, debug)
		if err != nil {
			cli.PrintErrf("An error occurred: %s\n", err.Error())
		}

		query = userOutput
	}
}

func agreeToTerms(scanner *bufio.Scanner) bool {
	_, ok := os.LookupEnv(dconfig.EnvDoltAssistAgree)
	if ok {
		return true
	}

	cli.Println(wordWrap("# ", "DISCLAIMER: Use of this tool may send information in your database, including schema, "+
		"commit history, and rows to OpenAI. If this use of your database information is unacceptable to you, please do "+
		"not use the tool."))
	cli.Print("\nContinue? (y/n) > ")

	scanner.Scan()
	input := strings.TrimSpace(scanner.Text())
	if strings.EqualFold(input, "y") {
		cli.Println(wordWrap("# ", "You can disable this check in the future by setting the DOLT_ASSIST_AGREE "+
			"environment variable."))
		return true
	}

	return false
}

var chatGptJsonHeader = `{
    "model": "%s",
    "messages": [`

var chatGptJsonFooter = `]}`

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
			a.messages = mustAppendJson(a.messages, "assistant", innerContent)

			// attempt to interpret this as a well formed json command
			var innerRespJson map[string]interface{}
			err := json.Unmarshal([]byte(innerContent), &innerRespJson)

			if err != nil {
				// attempt to salvage the response: sometimes the assistant includes a valid JSON response buffered by
				// commentary, so attempt to extract it and try again
				innerRespJson = extractJsonResponse(innerContent)

				if innerRespJson == nil {
					return textResponse(innerContent)
				}
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
		}
	}

	return textResponse(fmt.Sprintf("error: couldn't interpret response: %s", response))
}

var jsonRegex = regexp.MustCompile(`(\{.*?\})`)

func extractJsonResponse(content string) map[string]interface{} {
	matches := jsonRegex.FindAllString(content, -1)
	if len(matches) != 1 {
		return nil
	}

	var respJson map[string]interface{}
	err := json.Unmarshal([]byte(matches[0]), &respJson)
	if err != nil {
		return nil
	}

	return respJson
}

func sqlQuery(ctx context.Context, query string) (string, bool, error) {
	cli.Println(fmt.Sprintf("Running query \"%s\"...", query))

	output, _, err := doltExec(ctx, fmt.Sprintf("dolt sql -q \"%s\"", query), false)
	if err != nil {
		return "", false, err
	}

	if strings.TrimSpace(output) == "" {
		output = "Empty set."
		cli.Println(output)
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
	commandString = strings.TrimSpace(commandString)
	tokens, err := shlex.Split(commandString)
	if err != nil {
		return "", false, err
	}

	if echoCommand {
		cli.Println(commandString)
		cli.Println()
	}

	retOutput := strings.Builder{}
	var args []string

	execFn := func() error {
		cmdOut, err := runDolt(ctx, args)

		cli.Println(cmdOut)
		retOutput.WriteString(cmdOut)

		// this is delayed error handling from running the dolt command
		return err
	}

	// ChatGPT often chains commands together with &&, so attempt to execute all of them
	firstToken := true
	for _, token := range tokens {
		if firstToken {
			if token != "dolt" {
				return textResponse(commandString)
			}
			firstToken = false
		} else {
			if token == "&&" {
				err := execFn()
				if err != nil {
					return "", false, err
				}

				args = args[:0]
				firstToken = true
				continue
			}

			args = append(args, token)
		}
	}

	err = execFn()
	if err != nil {
		return "", false, err
	}

	return retOutput.String(), false, nil
}

func textResponse(content string) (string, bool, error) {
	cli.Println(wordWrap("", content))
	return "", false, nil
}

func wordWrap(linePrefix string, content string) string {
	sb := strings.Builder{}
	col := 0
	for _, char := range content {
		if col == 0 {
			sb.WriteString(linePrefix)
			col = len(linePrefix)
		}

		sb.WriteRune(char)
		col++

		if char == '\n' {
			col = 0
		} else if col >= 80 && unicode.IsSpace(char) {
			col = 0
			sb.WriteRune('\n')
		}
	}

	return sb.String()
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
	req.Header.Add("Authorization", "Bearer "+apiKey)

	respChan := make(chan string)
	errChan := make(chan error)
	go func() {
		defer close(respChan)
		defer close(errChan)
		response, err := client.Do(req)
		if err != nil {
			errChan <- err
			return
		}

		body, err := io.ReadAll(response.Body)
		if err != nil {
			errChan <- err
			return
		}

		respChan <- string(body)
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

	messages = mustAppendJson(messages, "system", "You are an expert dolt user who helps other users understand, query, and manage their dolt databases.")
	messages = mustAppendJson(messages, "user", "I'm going to give you some information about my database before I ask anything, OK?")
	messages = mustAppendJson(messages, "assistant", "I understand. Please tell me the schema of all tables as CREATE TABLE statements.")
	messages = mustAppendJson(messages, "user", fmt.Sprintf("CREATE TABLE statements for the database are as follows: %s", createTableStatements))
	messages = mustAppendJson(messages, "assistant", fmt.Sprintf("Thank you, I'll refer to these schemas "+
		"during our talk. Since we are talking over text, for the rest of this conversation, I'll respond in a machine readable "+
		"format so that you can easily consume it. I'll use JSON for my response like this: "+
		"{\"action\": \"DOLT_QUERY\", \"content\": \"dolt log -n 1\"}. "+
		"For example, this response means that I want you to run the dolt command 'dolt log -n 1' and tell me what it says. "+
		"Let's try a few more. You ask me some questions and I'll give you some responses in JSON. We'll just keep doing"+
		" that. Go ahead when you're ready."))

	messages = mustAppendJson(messages, "user", "who wrote the most recent commit?")

	responseJson, err := json.Marshal(map[string]string{"action": "DOLT_QUERY", "content": "dolt log -n 1"})
	if err != nil {
		return nil, err
	}

	messages = mustAppendJson(messages, "assistant", string(responseJson))

	logOutput, err := runDolt(ctx, []string{"log", "-n", "1"})
	if err != nil {
		return nil, err
	}

	messages = mustAppendJson(messages, "user", logOutput)

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
	messages = mustAppendJson(messages, "assistant", string(responseJson))

	messages = mustAppendJson(messages, "user", "write a SQL query that shows me the five most recent commits on the current branch")

	responseJson, err = json.Marshal(map[string]string{"action": "SQL_QUERY", "content": "SELECT * FROM DOLT_LOG order by date LIMIT 5"})
	if err != nil {
		return nil, err
	}
	messages = mustAppendJson(messages, "assistant", string(responseJson))

	messages = mustAppendJson(messages, "user", "check out a new branch named feature2 two commits before the head of the current branch")

	responseJson, err = json.Marshal(map[string]string{"action": "DOLT_EXEC", "content": "dolt checkout -b feature2 HEAD~2"})
	if err != nil {
		return nil, err
	}
	messages = mustAppendJson(messages, "assistant", string(responseJson))

	messages = mustAppendJson(messages, "user", "what changed in the last 3 commits?")

	responseJson, err = json.Marshal(map[string]string{"action": "DOLT_EXEC", "content": "dolt diff HEAD~3 HEAD"})
	if err != nil {
		return nil, err
	}
	messages = mustAppendJson(messages, "assistant", string(responseJson))

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
func runDolt(ctx context.Context, args []string) (string, error) {
	cmd := exec.CommandContext(ctx, "dolt", args...)

	type cmdOutput struct {
		output []byte
		err    error
	}
	outputChan := make(chan cmdOutput)

	go func() {
		defer close(outputChan)
		output, err := cmd.CombinedOutput()
		outputChan <- cmdOutput{output, err}
	}()

	spinner := TextSpinner{}
	cli.Print(spinner.next())
	defer func() {
		cli.DeleteAndPrint(1, "")
	}()

	for {
		select {
		case result := <-outputChan:
			return string(result.output), result.err
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(50 * time.Millisecond):
			cli.DeleteAndPrint(1, spinner.next())
		}
	}
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

	tables, err := root.GetTableNames(ctx, doltdb.DefaultSchemaName)
	for _, table := range tables {
		_, iter, _, err := sqlEngine.Query(ctx, fmt.Sprintf("SHOW CREATE TABLE %s", sql.QuoteIdentifier(table)))
		if err != nil {
			return "", err
		}
		rows, err := sql.RowIterToRows(ctx, iter)
		if err != nil {
			return "", err
		}

		createTable := rows[0].GetValue(1).(string)
		sb.WriteString(createTable)
		sb.WriteString("\n\n")
	}

	return sb.String(), nil
}

func (a Assist) Docs() *cli.CommandDocumentation {
	ap := a.ArgParser()
	return cli.NewCommandDocumentation(assistDocs, ap)
}

func (a Assist) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(a.Name(), 0)
	ap.SupportsString("model", "m", "open AI model id",
		"The ID of the Open AI model to use for the assistant. Defaults to gpt-3.5-turbo. "+
			"See https://platform.openai.com/docs/models/overview for a full list of models.")
	ap.SupportsFlag("debug", "d", "log API requests to and from the assistant")
	return ap
}

func (ts *TextSpinner) next() string {
	now := time.Now()
	if now.Sub(ts.lastUpdate) > minSpinnerUpdate {
		ts.seqPos = (ts.seqPos + 1) % len(spinnerSeq)
		ts.lastUpdate = now
	}

	return string([]rune{spinnerSeq[ts.seqPos]})
}

const minSpinnerUpdate = 100 * time.Millisecond

var spinnerSeq = []rune{'/', '-', '\\', '|'}

type TextSpinner struct {
	seqPos     int
	lastUpdate time.Time
}
