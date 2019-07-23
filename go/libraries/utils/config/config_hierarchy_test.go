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

package config

import "testing"

func TestCHGetAndSet(t *testing.T) {
	ch := NewConfigHierarchy()
	ch.AddConfig("priority", NewMapConfig(map[string]string{}))
	ch.AddConfig("fallback", NewMapConfig(map[string]string{}))

	priority, ok := ch.GetConfig("priority")

	if !ok {
		t.Fatal("Could not get priority config from hierarchy.")
	}

	fallback, ok := ch.GetConfig("fallback")

	if !ok {
		t.Fatal("Could not get fallback config from hierarchy.")
	}

	err := priority.SetStrings(map[string]string{
		"both":          "priority",
		"only_priority": "only_priority_value",
	})

	if err != nil {
		t.Fatal("Unable to set strings in priority config")
	}

	err = fallback.SetStrings(map[string]string{
		"both":          "whatever",
		"only_fallback": "only_fallback_value",
	})

	if err != nil {
		t.Fatal("Unable to set strings in priority config")
	}

	if str, err := ch.GetString("both"); err != nil || str != "priority" {
		t.Error("\"both\" key error.")
	}

	if str, err := ch.GetString("fallback::both"); err != nil || str != "whatever" {
		t.Error("\"fallback::both\" key error.")
	}

	if str, err := ch.GetString("only_priority"); err != nil || str != "only_priority_value" {
		t.Error("\"only_priority\" key error.")
	}

	if str, err := ch.GetString("only_fallback"); err != nil || str != "only_fallback_value" {
		t.Error("\"only_fallback\" key error.")
	}

	if _, err := ch.GetString("missing"); err != ErrConfigParamNotFound {
		t.Error("\"missing\" returned the wrong error.")
	}

	if _, err := ch.GetString("priority::missing"); err != ErrConfigParamNotFound {
		t.Error("\"missing\" returned the wrong error.")
	}

	if _, err := ch.GetString("invalid::missing"); err == nil || err == ErrConfigParamNotFound {
		t.Error("invalid namespace returned unexpected result")
	}

	ch.SetStrings(map[string]string{
		"priority::new":           "new_value",
		"fallback::only_fallback": "updated",
	})

	expectedIter := map[string]string{
		"priority::new":           "new_value",
		"priority::both":          "priority",
		"priority::only_priority": "only_priority_value",
		"fallback::both":          "whatever",
		"fallback::only_fallback": "updated",
	}

	testIteration(t, expectedIter, ch)

	err = ch.Unset([]string{"priority::only_priority"})

	if err != nil {
		t.Error("Failed to unset value. " + err.Error())
	}

	if _, err := ch.GetString("only_priority"); err != ErrConfigParamNotFound {
		t.Error("Unset failed.")
	}
}

func TestSplitParamName(t *testing.T) {
	testSplitParamName(t, "", "", "")
	testSplitParamName(t, "param_no_ns", "", "param_no_ns")
	testSplitParamName(t, "ns::param", "ns", "param")
	testSplitParamName(t, "ns::param::token", "ns", "param::token")
	testSplitParamName(t, "NS :: param :: token ", "ns", "param::token")
}

func testSplitParamName(t *testing.T, paramName, expectedNS, expectedName string) {
	ns, name := splitParamName(paramName)

	if ns != expectedNS || expectedName != name {
		t.Error("Error splitting " + paramName)
	}
}
