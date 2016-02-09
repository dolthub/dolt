package gucumber

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/lsegal/gucumber/gherkin"
)

var (
	GlobalContext = Context{
		Steps:         []StepDefinition{},
		World:         map[string]interface{}{},
		BeforeFilters: map[string]func(){},
		AfterFilters:  map[string]func(){},
		Filters:       []string{},
	}

	T Tester

	World = GlobalContext.World

	errNoMatchingStepFns = fmt.Errorf("no functions matched step.")
)

func Given(match string, fn interface{}) {
	GlobalContext.Given(match, fn)
}

func Then(match string, fn interface{}) {
	GlobalContext.Then(match, fn)
}

func When(match string, fn interface{}) {
	GlobalContext.When(match, fn)
}

func And(match string, fn interface{}) {
	GlobalContext.And(match, fn)
}

func Before(filter string, fn func()) {
	GlobalContext.Before(filter, fn)
}

func After(filter string, fn func()) {
	GlobalContext.After(filter, fn)
}

func BeforeMulti(filters []string, fn func()) {
	GlobalContext.BeforeMulti(filters, fn)
}

func AfterMulti(filters []string, fn func()) {
	GlobalContext.AfterMulti(filters, fn)
}

type Context struct {
	Filters         []string
	World           map[string]interface{}
	BeforeFilters   map[string]func()
	AfterFilters    map[string]func()
	BeforeAllFilter func()
	AfterAllFilter  func()
	Steps           []StepDefinition
	T               Tester
}

func (c *Context) addStep(match string, fn interface{}) {
	c.Steps = append(c.Steps, StepDefinition{
		Matcher:  regexp.MustCompile(match),
		Function: reflect.ValueOf(fn),
	})
}

func (c *Context) Given(match string, fn interface{}) {
	c.addStep(match, fn)
}

func (c *Context) Then(match string, fn interface{}) {
	c.addStep(match, fn)
}

func (c *Context) When(match string, fn interface{}) {
	c.addStep(match, fn)
}

func (c *Context) And(match string, fn interface{}) {
	c.addStep(match, fn)
}

func (c *Context) Before(filter string, fn func()) {
	c.BeforeFilters[filter] = fn
}

func (c *Context) After(filter string, fn func()) {
	c.AfterFilters[filter] = fn
}

func (c *Context) BeforeMulti(filters []string, fn func()) {
	c.BeforeFilters[strings.Join(filters, "|")] = fn
}

func (c *Context) AfterMulti(filters []string, fn func()) {
	c.AfterFilters[strings.Join(filters, "|")] = fn
}

func (c *Context) BeforeAll(fn func()) {
	c.BeforeAllFilter = fn
}

func (c *Context) AfterAll(fn func()) {
	c.AfterAllFilter = fn
}

func (c *Context) Execute(t Tester, line string, arg string) (bool, error) {
	T = t
	c.T = t

	found := false
	for _, step := range c.Steps {
		f, err := step.CallIfMatch(c, t, line, arg)
		if err != nil {
			return f, err
		}
		if f {
			found = true
		}
	}

	return found, nil
}

type StepDefinition struct {
	Matcher  *regexp.Regexp
	Function reflect.Value
}

func (s *StepDefinition) CallIfMatch(c *Context, test Tester, line string, arg string) (bool, error) {
	if match := s.Matcher.FindStringSubmatch(line); match != nil {
		match = match[1:] // discard full line match

		// adjust arity if there is step arg data
		numArgs := len(match)
		if arg != "" {
			numArgs++
		}

		t := s.Function.Type()
		if t.NumIn() > 0 && t.In(0).Kind() == reflect.Ptr {
			e := t.In(0).Elem()
			if e.String() == "testing.T" {
				numArgs++ // first param is *testing.T
			}
		}
		if numArgs != t.NumIn() { // function has different arity
			return true, fmt.Errorf("matcher function has different arity %d != %d",
				numArgs, t.NumIn())
		}

		values := make([]reflect.Value, numArgs)
		for m, i := 0, 0; i < t.NumIn(); i++ {
			param := t.In(i)

			var v interface{}
			switch param.Kind() {
			case reflect.Slice:
				param = param.Elem()
				if param.String() == "gherkin.TabularData" {
					v = gherkin.StringData(arg).ToTable()
				} else if param.Kind() == reflect.Slice && param.Elem().Kind() == reflect.String {
					// just a raw [][]string slice
					v = gherkin.StringData(arg).ToTable()
				}
			case reflect.Ptr:
				if param.Elem().String() == "testing.T" {
					v = test
				}
			case reflect.Int:
				i, _ := strconv.ParseInt(match[m], 10, 32)
				v = int(i)
				m++
			case reflect.Int64:
				v, _ = strconv.ParseInt(match[m], 10, 64)
				m++
			case reflect.String:
				// this could be from `arg`, check match index
				if m >= len(match) {
					v = arg
				} else {
					v = match[m]
					m++
				}
			case reflect.Float64:
				v, _ = strconv.ParseFloat(match[m], 64)
				m++
			}

			if v == nil {
				panic("type " + t.String() + "is not supported.")
			}
			values[i] = reflect.ValueOf(v)
		}

		s.Function.Call(values)
		return true, nil
	}
	return false, nil
}
