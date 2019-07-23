package cli

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/fatih/color"

	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/funcitr"
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

	for k, v := range ap.ArgListHelp {
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
