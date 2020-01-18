// Copyright 2019 Liquidata, Inc.
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

package cli

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/fatih/color"

	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/libraries/utils/funcitr"
	"github.com/liquidata-inc/dolt/go/libraries/utils/iohelp"
)

var underline = color.New(color.Underline)

func PrintHelpText(commandStr, shortDesc, longDesc string, synopsis []string, parser *argparser.ArgParser) {
	commandStr = embolden(commandStr)
	shortDesc = embolden(shortDesc)
	longDesc = embolden(longDesc)

	_, termWidth := terminalSize()

	indent := "\t"
	helpWidth := termWidth - 10
	if helpWidth < 30 {
		helpWidth = 120
	}

	Println(embolden("<b>NAME</b>"))
	Printf("%s%s - %s\n", indent, commandStr, shortDesc)

	if len(synopsis) > 0 {
		Println()
		Println(embolden("<b>SYNOPSIS</b>"))

		for _, curr := range synopsis {
			Printf(indent+"%s %s\n", underline.Sprint(commandStr), curr)
		}
	}

	Println()
	Println(embolden("<b>DESCRIPTION</b>"))
	Println(ToIndentedParagraph(longDesc, indent, helpWidth))

	if len(parser.Supported) > 0 || len(parser.ArgListHelp) > 0 {
		Println()
		Println(embolden("<b>OPTIONS</b>"))
		optionHelp := OptionsUsage(parser, indent, helpWidth)
		Println(optionHelp)
	}
}

func CreateMarkdown(fs filesys.Filesys, path, commandStr, shortDesc, longDesc string, synopsis []string, parser *argparser.ArgParser) error {
	wr, err := fs.OpenForWrite(path)

	if err != nil {
		return err
	}

	defer wr.Close()

	err = iohelp.WriteIfNoErr(wr, []byte("## Command\n\n"), nil)
	err = iohelp.WriteIfNoErr(wr, []byte(commandStr+" - "+shortDesc+"\n\n"), err)

	if len(synopsis) > 0 {
		err = iohelp.WriteIfNoErr(wr, []byte("## Synopsis\n\n"), err)

		err = iohelp.WriteIfNoErr(wr, []byte("```sh\n"), err)
		for _, synopsisLine := range synopsis {
			err = iohelp.WriteIfNoErr(wr, []byte(commandStr+" "+synopsisLine+"\n"), err)
		}
		err = iohelp.WriteIfNoErr(wr, []byte("```\n\n"), err)
	}

	err = iohelp.WriteIfNoErr(wr, []byte("## Description\n\n"), err)
	err = iohelp.WriteIfNoErr(wr, []byte(markdownEscape(longDesc)+"\n\n"), err)

	if len(parser.Supported) > 0 || len(parser.ArgListHelp) > 0 {
		err = iohelp.WriteIfNoErr(wr, []byte("## Options\n\n"), err)

		for _, kvTuple := range parser.ArgListHelp {
			k, v := kvTuple[0], kvTuple[1]
			err = iohelp.WriteIfNoErr(wr, []byte("&lt;"+k+"&gt;\n"+v+"\n\n"), err)
		}

		for _, supOpt := range parser.Supported {
			argHelpFmt := "--%[2]s"

			if supOpt.Abbrev != "" && supOpt.ValDesc != "" {
				argHelpFmt = "-%[1]s &lt;%[3]s&gt;, --%[2]s=&lt;%[3]s&gt;"
			} else if supOpt.Abbrev != "" {
				argHelpFmt = "-%[1]s, --%[2]s"
			} else if supOpt.ValDesc != "" {
				argHelpFmt = "--%[2]s=&lt;%[3]s&gt;"
			}

			argHelp := fmt.Sprintf(argHelpFmt, supOpt.Abbrev, supOpt.Name, supOpt.ValDesc)
			err = iohelp.WriteIfNoErr(wr, []byte(argHelp+"\n"), err)
			err = iohelp.WriteIfNoErr(wr, []byte(supOpt.Desc+"\n\n"), err)
		}
	}

	return err
}

func PrintUsage(commandStr string, synopsis []string, parser *argparser.ArgParser) {
	_, termWidth := terminalSize()

	helpWidth := termWidth - 10
	if helpWidth < 30 {
		helpWidth = 120
	}

	if len(synopsis) > 0 {
		for i, curr := range synopsis {
			if i == 0 {
				Println("usage:", commandStr, curr)
			} else {
				Println("   or:", commandStr, curr)
			}
		}
	}

	if len(parser.Supported) > 0 || len(parser.ArgListHelp) > 0 {
		Println()
		Println("Specific", commandStr, "options")
		optionHelp := OptionsUsage(parser, "    ", helpWidth)
		Println(optionHelp)
	}
}

const (
	boldStart    = "<b>"
	boldEnd      = "</b>"
	boldStartLen = len(boldStart)
	boldEndLen   = len(boldEnd)
)

var bold = color.New(color.Bold)

func markdownEscape(str string) string {
	str = strings.ReplaceAll(str, "<b>", "**")
	str = strings.ReplaceAll(str, "</b>", "**")
	str = strings.ReplaceAll(str, "<", "&lt;")
	str = strings.ReplaceAll(str, ">", "&gt;")

	return str
}

func embolden(str string) string {
	res := ""
	curr := str

	start := strings.Index(curr, boldStart)
	end := strings.Index(curr, boldEnd)

	for start != -1 && end != -1 {
		if start < end {
			begin := curr[0:start]
			mid := curr[start+boldStartLen : end]
			curr = curr[end+boldEndLen:]

			res += begin + bold.Sprint(mid)

			start = strings.Index(curr, boldStart)
			end = strings.Index(curr, boldEnd)
		} else {
			nextEnd := strings.Index(curr[end+boldEndLen:], boldEnd)

			if nextEnd == -1 {
				end = -1
			} else {
				end += boldEndLen + nextEnd
			}
		}

	}

	res += curr
	return res
}

func terminalSize() (width, height int) {
	defer func() {
		recover()
	}()

	height = -1
	width = -1

	cmd := exec.Command("stty", "size")
	cmd.Stdin = os.Stdin
	out, err := cmd.Output()

	if err == nil {
		outStr := string(out)
		tokens := strings.Split(outStr, " ")
		tempWidth, err := strconv.ParseInt(strings.TrimSpace(tokens[0]), 10, 32)

		if err == nil {
			tempHeight, err := strconv.ParseInt(strings.TrimSpace(tokens[1]), 10, 32)

			if err == nil {
				width, height = int(tempWidth), int(tempHeight)
			}
		}
	}

	return
}

func OptionsUsage(ap *argparser.ArgParser, indent string, lineLen int) string {
	var lines []string

	for _, kvTuple := range ap.ArgListHelp {
		k, v := kvTuple[0], kvTuple[1]
		lines = append(lines, "<"+k+">")
		descLines := toParagraphLines(v, lineLen)
		descLines = indentLines(descLines, "  ")
		descLines = append(descLines, "")

		lines = append(lines, descLines...)
	}

	for _, supOpt := range ap.Supported {
		argHelpFmt := "--%[2]s"

		if supOpt.Abbrev != "" && supOpt.ValDesc != "" {
			argHelpFmt = "-%[1]s <%[3]s>, --%[2]s=<%[3]s>"
		} else if supOpt.Abbrev != "" {
			argHelpFmt = "-%[1]s, --%[2]s"
		} else if supOpt.ValDesc != "" {
			argHelpFmt = "--%[2]s=<%[3]s>"
		}

		lines = append(lines, fmt.Sprintf(argHelpFmt, supOpt.Abbrev, supOpt.Name, supOpt.ValDesc))

		descLines := toParagraphLines(supOpt.Desc, lineLen)
		descLines = indentLines(descLines, "  ")
		descLines = append(descLines, "")

		lines = append(lines, descLines...)
	}

	lines = indentLines(lines, indent)
	return strings.Join(lines, "\n")
}

func ToIndentedParagraph(inStr, indent string, lineLen int) string {
	lines := toParagraphLines(inStr, lineLen)
	indentedLines := indentLines(lines, indent)
	joined := strings.Join(indentedLines, "\n")
	return joined
}

func toParagraphLines(inStr string, lineLen int) []string {
	var lines []string
	descLines := strings.Split(inStr, "\n")
	for _, descLine := range descLines {
		if len(descLine) == 0 {
			lines = append(lines, "")
		} else {
			lineIndent := ""
			for len(descLine) > 0 && (descLine[0] == ' ' || descLine[0] == '\t') {
				lineIndent += string(descLine[0])
				descLine = descLine[1:]
			}

			descLineLen := lineLen - len(lineIndent)
			for remaining := descLine; len(remaining) > 0; {
				if len(remaining) > descLineLen {
					whiteSpIdx := strings.LastIndexAny(remaining[:descLineLen], " \t")

					splitPt := whiteSpIdx
					if splitPt == -1 {
						splitPt = descLineLen
					}

					line := lineIndent + remaining[:splitPt]
					lines = append(lines, line)

					remaining = remaining[splitPt+1:]
				} else {
					lines = append(lines, lineIndent+remaining)
					remaining = ""
				}
			}
		}
	}

	return lines
}

func indentLines(lines []string, indentation string) []string {
	return funcitr.MapStrings(lines, func(s string) string {
		return indentation + s
	})
}
