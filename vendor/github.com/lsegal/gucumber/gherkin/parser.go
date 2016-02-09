package gherkin

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
)

var reNL = regexp.MustCompile(`\r?\n`)

func ParseFilename(data, filename string) ([]Feature, error) {
	lines := reNL.Split(data, -1)
	p := parser{
		lines:        lines,
		features:     []Feature{},
		translations: Translations[LANG_EN],
		filename:     filename,
	}
	if err := p.parse(); err != nil {
		return nil, err
	}
	if len(p.features) == 0 {
		return p.features, p.err("no features parsed")
	}
	return p.features, nil
}

func Parse(data string) ([]Feature, error) {
	return ParseFilename(data, "")
}

type parser struct {
	translations Translation
	features     []Feature
	lines        []string
	lineNo       int
	lastLine     int
	started      bool
	filename     string
}

func (p parser) err(msg string, args ...interface{}) error {
	l := p.lineNo + 1
	if l > len(p.lines) {
		l = len(p.lines)
	}
	if p.filename == "" {
		p.filename = "<unknown>.feature"
	}
	return fmt.Errorf("parse error (%s:%d): %s.",
		filepath.Base(p.filename), l, fmt.Sprintf(msg, args...))
}

func (p *parser) line() string {
	if p.lineNo < len(p.lines) {
		return p.lines[p.lineNo]
	}
	return ""
}

func (p *parser) nextLine() bool {
	p.lastLine = p.lineNo
	if p.started {
		p.lineNo++
	}
	p.started = true

	for ; p.lineNo < len(p.lines); p.lineNo++ {
		line, _ := p.lineStripped()
		if line != "" && !strings.HasPrefix(line, "#") {
			break
		}
	}

	return p.stillReading()
}

func (p *parser) stillReading() bool {
	return p.lineNo < len(p.lines)
}

func (p *parser) unread() {
	if p.lineNo == 0 {
		p.started = false
	}
	p.lineNo = p.lastLine
}

func (p *parser) parse() error {
	for p.stillReading() {
		if err := p.consumeFeature(); err != nil {
			return err
		}
	}

	return nil
}

func (p *parser) lineStripped() (string, int) {
	line := p.line()
	for i := 0; i < len(line); i++ {
		c := line[i : i+1]
		if c != " " && c != "\t" {
			return line[i:], i
		}
	}
	return line, 0
}

func (p *parser) consumeFeature() error {
	desc := []string{}
	tags, err := p.consumeTags()
	if err != nil {
		return err
	}

	if !p.nextLine() {
		if len(tags) > 0 {
			return p.err("tags not applied to feature")
		}
		return nil
	}

	line, startIndent := p.lineStripped()
	parts := strings.SplitN(line, " ", 2)
	prefix := p.translations.Feature + ":"
	title := ""

	if parts[0] != prefix {
		return p.err("expected %q, found %q", prefix, line)
	}

	if len(parts) > 1 {
		title = parts[1]
	}
	f := Feature{
		Title: title, Tags: tags, Scenarios: []Scenario{},
		Filename: p.filename, Line: p.lineNo,
	}
	var stags *[]string
	seenScenario, seenBackground := false, false
	for p.stillReading() { // consume until we get to Background or Scenario
		// find indentation of next line
		if p.nextLine() {
			_, indent := p.lineStripped()
			p.unread()
			if indent <= startIndent { // de-dented
				break // done parsing this feature
			}
		}

		prevLine := p.lineNo
		tags, err = p.consumeTags()
		if err != nil {
			return err
		}
		if p.lineNo != prevLine { // tags found
			stags = &tags
		}

		if !p.nextLine() {
			break
		}

		line, _ := p.lineStripped()
		parts := strings.SplitN(line, ":", 2)

		switch parts[0] {
		case p.translations.Background:
			if seenScenario {
				return p.err("illegal background after scenario")
			} else if seenBackground {
				return p.err("multiple backgrounds not allowed")
			}
			seenBackground = true

			b := Scenario{Filename: p.filename, Line: p.lineNo}
			err = p.consumeScenario(&b)
			if err != nil {
				return err
			}
			if stags != nil {
				b.Tags = *stags
			} else {
				b.Tags = []string{}
			}
			f.Background = b
			stags = nil
		case p.translations.Scenario, p.translations.Outline:
			seenScenario = true

			s := Scenario{Filename: p.filename, Line: p.lineNo}
			err = p.consumeScenario(&s)
			if err != nil {
				return err
			}
			if stags != nil {
				s.Tags = *stags
			} else {
				s.Tags = []string{}
			}
			if len(parts) > 1 {
				s.Title = strings.TrimSpace(parts[1])
			}
			f.Scenarios = append(f.Scenarios, s)
			stags = nil
		default: // then this must be a description
			if stags != nil {
				return p.err("illegal description text after tags")
			} else if seenScenario || seenBackground {
				return p.err("illegal description text after scenario")
			}
			desc = append(desc, line)
		}
	}
	f.Description = strings.Join(desc, "\n")
	p.features = append(p.features, f)

	return nil
}

func (p *parser) consumeScenario(scenario *Scenario) error {
	scenario.Steps = []Step{}
	_, startIndent := p.lineStripped()
	for p.nextLine() { // consume all steps
		_, indent := p.lineStripped()
		if indent <= startIndent { // de-dented
			p.unread()
			break
		}

		if err := p.consumeStep(scenario); err != nil {
			return err
		}
	}

	return nil
}

func (p *parser) consumeStep(scenario *Scenario) error {
	line, indent := p.lineStripped()
	parts := strings.SplitN(line, " ", 2)

	switch parts[0] {
	case p.translations.Given, p.translations.Then,
		p.translations.When, p.translations.And:
		var arg StringData
		if len(parts) < 2 {
			return p.err("expected step text after %q", parts[0])
		}
		if p.nextLine() {
			l, _ := p.lineStripped()
			p.unread()
			if len(l) > 0 && (l[0] == '|' || l == `"""`) {
				// this is step argument data
				arg = p.consumeIndentedData(indent)
			}
		}

		var stype StepType
		switch parts[0] {
		case p.translations.Given:
			stype = StepType("Given")
		case p.translations.When:
			stype = StepType("When")
		case p.translations.Then:
			stype = StepType("Then")
		case p.translations.And:
			stype = StepType("And")
		}
		s := Step{
			Filename: p.filename, Line: p.lineNo,
			Type: stype, Text: parts[1], Argument: arg,
		}
		scenario.Steps = append(scenario.Steps, s)
	case p.translations.Examples + ":":
		scenario.Examples = p.consumeIndentedData(indent)
	default:
		return p.err("illegal step prefix %q", parts[0])
	}
	return nil
}

func (p *parser) consumeIndentedData(scenarioIndent int) StringData {
	stringData := []string{}
	startIndent, quoted := -1, false
	for p.nextLine() {
		var line string
		var indent int
		if startIndent == -1 { // first line
			line, indent = p.lineStripped()
			startIndent = indent

			if line == `"""` {
				quoted = true // this is a docstring data block
				continue      // ignore this from data
			}
		} else {
			line = p.line()
			if len(line) <= startIndent {
				// not enough text, not part of indented data
				p.unread()
				break
			}
			if line[0:startIndent] != strings.Repeat(" ", startIndent) {
				// not enough indentation, not part of indented data
				p.unread()
				break
			}

			line = line[startIndent:]
			if !quoted && line[0] != '|' {
				// tabular data must start with | on each line
				p.unread()
				break
			}
			if quoted && line == `"""` { // end quote on docstring block
				break
			}

		}

		stringData = append(stringData, line)
	}

	return StringData(strings.Join(stringData, "\n"))
}

func (p *parser) consumeTags() ([]string, error) {
	tags := []string{}
	if !p.nextLine() {
		return tags, nil
	}

	line, _ := p.lineStripped()
	if len(p.lines) == 0 || !strings.HasPrefix(line, "@") {
		p.unread()
		return tags, nil
	}

	for _, t := range strings.Split(line, " ") {
		if t == "" {
			continue
		}
		if t[0:1] != "@" {
			return nil, p.err("invalid tag %q", t)
		}
		tags = append(tags, t)
	}

	return tags, nil
}
