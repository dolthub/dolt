/* gonuts10seqB.go - https://groups.google.com/forum/?fromgroups#!topic/golang-nuts/tf4aDQ1Hn_c

Objective:  assign Comment.CommentText attribute value to Request.ReportingName attribute that immediately follows Comment.

NOTE: use NewMapXmlSeq() and mv.XmlSeqIndent() to preserve structure.

See data value at EOF - from: https://gist.github.com/suntong/e4dcdc6c85dcf769eec4
*/

package main

import (
	"bytes"
	"fmt"
	"github.com/clbanning/mxj"
	"io"
)

func main() {
	// fmt.Println(string(data))
	rdr := bytes.NewReader(data)
	var m mxj.Map
	var err error
	// We read processing docs sequentially.
	// Un-rooted ProcInst or Comments are processed AND just reencoded. (XmlSeqIndent() knows how, now.)
	for m, err = mxj.NewMapXmlSeqReader(rdr); m != nil || err != io.EOF; m, err = mxj.NewMapXmlSeqReader(rdr) {
		if err != nil {
			if err != mxj.NO_ROOT {
				fmt.Println("NewMapXmlSeq err:", err)
				fmt.Println("m:", m)
			} else if m != nil {
				x, _ := m.XmlSeqIndent("", "  ")
				fmt.Println(string(x))
			}
			continue
		}
		// fmt.Println(m.StringIndent())

		// We have Two different arrays of Items in the XML doc, one nested in the other.
		if err = copyCmts(m, "WebTest.Items"); err != nil {
			fmt.Println("err:", err)
		}
		if err = copyCmts(m, "WebTest.Items.TransactionTimer.Items"); err != nil {
			fmt.Println("err:", err)
		}
		// re-encode the map with the Items.Comment[#seq==n].#attr.CommentText
		// values copied to the Items.Request[#seq==n+1].#attr.ReportingName elements.
		b, err := m.XmlSeqIndent("", "  ")
		if err != nil {
			fmt.Println("XmlIndent err:", err)
			return
		}
		fmt.Println(string(b))
	}
}

// Uncomment the print statements to details of the process here.
func copyCmts(m mxj.Map, path string) error {
	// get the array of Items entries for the 'path'
	vals, err := m.ValuesForPath(path)
	if err != nil {
		return fmt.Errorf("ValuesForPath err: %s", err.Error())
	} else if len(vals) == 0 {
		return fmt.Errorf("no vals for path: %s", path)
	}
	// process each Items entry
	for _, v := range vals {
		vm, ok := v.(map[string]interface{})
		if !ok {
			return fmt.Errorf("assertion failed")
		}
		// get the Comment list
		c, ok := vm["Comment"]
		if !ok { // --> no Items.Comment elements
			continue
		}
		// Don't assume that Comment is an array.
		// There may be just one value, in which case it will decode as map[string]interface{}.
		switch c.(type) {
		case map[string]interface{}:
			c = []interface{}{c}
		}
		cmt := c.([]interface{})
		// get the Request list
		r, ok := vm["Request"]
		if !ok { // --> no Items.Request elements
			continue
		}
		// Don't assume the Request is an array.
		// There may be just one value, in which case it will decode as map[string]interface{}.
		switch r.(type) {
		case map[string]interface{}:
			r = []interface{}{r}
		}
		req := r.([]interface{})

		// fmt.Println("Comment:", cmt)
		// fmt.Println("Request:", req)

		// Comment elements with #seq==n are followed by Request element with #seq==n+1.
		// For each Comment.#seq==n extract the CommentText attribute value and use it to
		// set the ReportingName attribute value in Request.#seq==n+1.
		for _, v := range cmt {
			vmap := v.(map[string]interface{})
			seq := vmap["#seq"].(int) // type is int
			// extract CommentText attr from "#attr"
			acmt, _ := mxj.Map(vmap).ValueForPath("#attr.CommentText.#text")
			if acmt == "" {
				fmt.Println("no CommentText value in Comment attributes")
			}
			// fmt.Println(seq, acmt)
			// find the request with the #seq==seq+1 value
			var r map[string]interface{}
			for _, vv := range req {
				rt := vv.(map[string]interface{})
				if rt["#seq"].(int) == seq+1 {
					r = rt
					break
				}
			}
			if r == nil { // no Request with #seq==seq+1
				continue
			}
			if err := mxj.Map(r).SetValueForPath(acmt, "#attr.ReportingName.#text"); err != nil {
				fmt.Println("SetValueForPath err:", err)
				break
			}
		}
	}
	return nil
}

var data = []byte(`
<?xml version="1.0" encoding="utf-8"?>
<WebTest Name="FirstAnonymousVisit" Id="ac766d08-f940-4b0a-b8f8-80675978894e" Owner="" Priority="0" Enabled="True" CssProjectStructure="" CssIteration="" Timeout="0" WorkItemIds="" xmlns="http://microsoft.com/schemas/VisualStudio/TeamTest/2010" Description="" CredentialUserName="" CredentialPassword="" PreAuthenticate="True" Proxy="" StopOnError="False" RecordedResultFile="">
  <Items>
    <Comment CommentText="Visit Homepage and ensure new page setup is created" />
    <Request Method="GET" Version="1.1" Url="{{Config.TestParameters.ServerURL}}/Default.aspx" ThinkTime="0" Timeout="300" ParseDependentRequests="False" FollowRedirects="True" RecordResult="True" Cache="False" ResponseTimeGoal="0.5" Encoding="utf-8" ExpectedHttpStatusCode="0" ExpectedResponseUrl="" ReportingName="">
      <ValidationRules>
        <ValidationRule Classname="Dropthings.Test.Rules.CookieValidationRule, Dropthings.Test, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null" DisplayName="Check Cookie From Response" Description="" Level="High" ExectuionOrder="BeforeDependents">
          <RuleParameters>
            <RuleParameter Name="StopOnError" Value="False" />
            <RuleParameter Name="CookieValueToMatch" Value="" />
            <RuleParameter Name="MatchValue" Value="False" />
            <RuleParameter Name="Exists" Value="True" />
            <RuleParameter Name="CookieName" Value="{{Config.TestParameters.AnonCookieName}}" />
            <RuleParameter Name="IsPersistent" Value="True" />
            <RuleParameter Name="Domain" Value="" />
            <RuleParameter Name="Index" Value="0" />
          </RuleParameters>
        </ValidationRule>
        <ValidationRule Classname="Dropthings.Test.Rules.CookieValidationRule, Dropthings.Test, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null" DisplayName="Check Cookie From Response" Description="" Level="High" ExectuionOrder="BeforeDependents">
          <RuleParameters>
            <RuleParameter Name="StopOnError" Value="False" />
            <RuleParameter Name="CookieValueToMatch" Value="" />
            <RuleParameter Name="MatchValue" Value="False" />
            <RuleParameter Name="Exists" Value="False" />
            <RuleParameter Name="CookieName" Value="{{Config.TestParameters.SessionCookieName}}" />
            <RuleParameter Name="IsPersistent" Value="False" />
            <RuleParameter Name="Domain" Value="" />
            <RuleParameter Name="Index" Value="0" />
          </RuleParameters>
        </ValidationRule>
        <ValidationRule Classname="Dropthings.Test.Rules.CacheHeaderValidation, Dropthings.Test, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null" DisplayName="Cache Header Validation" Description="" Level="High" ExectuionOrder="BeforeDependents">
          <RuleParameters>
            <RuleParameter Name="Enabled" Value="True" />
            <RuleParameter Name="DifferenceThresholdSec" Value="0" />
            <RuleParameter Name="CacheControlPrivate" Value="False" />
            <RuleParameter Name="CacheControlPublic" Value="False" />
            <RuleParameter Name="CacheControlNoCache" Value="True" />
            <RuleParameter Name="ExpiresAfterSeconds" Value="0" />
            <RuleParameter Name="StopOnError" Value="False" />
          </RuleParameters>
        </ValidationRule>
        <ValidationRule Classname="Microsoft.VisualStudio.TestTools.WebTesting.Rules.ValidationRuleFindText, Microsoft.VisualStudio.QualityTools.WebTestFramework, Version=10.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a" DisplayName="Find Text" Description="Verifies the existence of the specified text in the response." Level="High" ExectuionOrder="BeforeDependents">
          <RuleParameters>
            <RuleParameter Name="FindText" Value="How to of the Day" />
            <RuleParameter Name="IgnoreCase" Value="False" />
            <RuleParameter Name="UseRegularExpression" Value="False" />
            <RuleParameter Name="PassIfTextFound" Value="True" />
          </RuleParameters>
        </ValidationRule>
        <ValidationRule Classname="Microsoft.VisualStudio.TestTools.WebTesting.Rules.ValidationRuleFindText, Microsoft.VisualStudio.QualityTools.WebTestFramework, Version=10.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a" DisplayName="Find Text" Description="Verifies the existence of the specified text in the response." Level="High" ExectuionOrder="BeforeDependents">
          <RuleParameters>
            <RuleParameter Name="FindText" Value="Weather" />
            <RuleParameter Name="IgnoreCase" Value="False" />
            <RuleParameter Name="UseRegularExpression" Value="False" />
            <RuleParameter Name="PassIfTextFound" Value="True" />
          </RuleParameters>
        </ValidationRule>
        <ValidationRule Classname="Microsoft.VisualStudio.TestTools.WebTesting.Rules.ValidationRuleFindText, Microsoft.VisualStudio.QualityTools.WebTestFramework, Version=10.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a" DisplayName="Find Text" Description="Verifies the existence of the specified text in the response." Level="High" ExectuionOrder="BeforeDependents">
          <RuleParameters>
            <RuleParameter Name="FindText" Value="All rights reserved" />
            <RuleParameter Name="IgnoreCase" Value="False" />
            <RuleParameter Name="UseRegularExpression" Value="False" />
            <RuleParameter Name="PassIfTextFound" Value="True" />
          </RuleParameters>
        </ValidationRule>
      </ValidationRules>
    </Request>
    <TransactionTimer Name="Show Hide Widget List">
      <Items>
        <Comment CommentText="Show Widget List and expect Widget List to produce BBC Word widget link" />
        <Request Method="GET" Version="1.1" Url="{{Config.TestParameters.ServerURL}}/Default.aspx" ThinkTime="0" Timeout="300" ParseDependentRequests="False" FollowRedirects="True" RecordResult="True" Cache="False" ResponseTimeGoal="0.5" Encoding="utf-8" ExpectedHttpStatusCode="0" ExpectedResponseUrl="" ReportingName="">
          <ValidationRules>
            <ValidationRule Classname="Microsoft.VisualStudio.TestTools.WebTesting.Rules.ValidationRuleFindText, Microsoft.VisualStudio.QualityTools.WebTestFramework, Version=10.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a" DisplayName="Find Text" Description="Verifies the existence of the specified text in the response." Level="High" ExectuionOrder="BeforeDependents">
              <RuleParameters>
                <RuleParameter Name="FindText" Value="BBC World" />
                <RuleParameter Name="IgnoreCase" Value="False" />
                <RuleParameter Name="UseRegularExpression" Value="False" />
                <RuleParameter Name="PassIfTextFound" Value="True" />
              </RuleParameters>
            </ValidationRule>
          </ValidationRules>
          <RequestPlugins>
            <RequestPlugin Classname="Dropthings.Test.Plugin.AsyncPostbackRequestPlugin, Dropthings.Test, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null" DisplayName="AsyncPostbackRequestPlugin" Description="">
              <RuleParameters>
                <RuleParameter Name="ControlName" Value="TabControlPanel$ShowAddContentPanel" />
                <RuleParameter Name="UpdatePanelName" Value="{{$UPDATEPANEL.OnPageMenuUpdatePanel.1}}" />
              </RuleParameters>
            </RequestPlugin>
          </RequestPlugins>
        </Request>
        <Comment CommentText="Hide Widget List and expect the outpu does not have the BBC World Widget" />
        <Request Method="GET" Version="1.1" Url="{{Config.TestParameters.ServerURL}}/Default.aspx" ThinkTime="0" Timeout="300" ParseDependentRequests="False" FollowRedirects="True" RecordResult="True" Cache="False" ResponseTimeGoal="0.5" Encoding="utf-8" ExpectedHttpStatusCode="0" ExpectedResponseUrl="" ReportingName="">
          <ValidationRules>
            <ValidationRule Classname="Microsoft.VisualStudio.TestTools.WebTesting.Rules.ValidationRuleFindText, Microsoft.VisualStudio.QualityTools.WebTestFramework, Version=10.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a" DisplayName="Find Text" Description="Verifies the existence of the specified text in the response." Level="High" ExectuionOrder="BeforeDependents">
              <RuleParameters>
                <RuleParameter Name="FindText" Value="TabControlPanel$ShowAddContentPanel" />
                <RuleParameter Name="IgnoreCase" Value="False" />
                <RuleParameter Name="UseRegularExpression" Value="False" />
                <RuleParameter Name="PassIfTextFound" Value="True" />
              </RuleParameters>
            </ValidationRule>
          </ValidationRules>
          <RequestPlugins>
            <RequestPlugin Classname="Dropthings.Test.Plugin.AsyncPostbackRequestPlugin, Dropthings.Test, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null" DisplayName="AsyncPostbackRequestPlugin" Description="">
              <RuleParameters>
                <RuleParameter Name="ControlName" Value="TabControlPanel$HideAddContentPanel" />
                <RuleParameter Name="UpdatePanelName" Value="{{$UPDATEPANEL.OnPageMenuUpdatePanel.1}}" />
              </RuleParameters>
            </RequestPlugin>
          </RequestPlugins>
        </Request>
      </Items>
    </TransactionTimer>
    <Request Method="GET" Version="1.1" Url="{{Config.TestParameters.ServerURL}}/API/Proxy.svc/ajax/GetRss?url=%22http%3A%2F%2Ffeeds.feedburner.com%2FOmarAlZabirBlog%22&amp;count=10&amp;cacheDuration=10" ThinkTime="0" Timeout="300" ParseDependentRequests="True" FollowRedirects="True" RecordResult="True" Cache="False" ResponseTimeGoal="0" Encoding="utf-8" ExpectedHttpStatusCode="0" ExpectedResponseUrl="" ReportingName="">
      <ValidationRules>
        <ValidationRule Classname="Microsoft.VisualStudio.TestTools.WebTesting.Rules.ValidationRuleFindText, Microsoft.VisualStudio.QualityTools.WebTestFramework, Version=10.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a" DisplayName="Find Text" Description="Verifies the existence of the specified text in the response." Level="High" ExectuionOrder="BeforeDependents">
          <RuleParameters>
            <RuleParameter Name="FindText" Value="{&quot;d&quot;:[{&quot;__type&quot;:&quot;RssItem:#Dropthings.Web.Util&quot;" />
            <RuleParameter Name="IgnoreCase" Value="False" />
            <RuleParameter Name="UseRegularExpression" Value="False" />
            <RuleParameter Name="PassIfTextFound" Value="True" />
          </RuleParameters>
        </ValidationRule>
      </ValidationRules>
    </Request>
    <Request Method="GET" Version="1.1" Url="{{Config.TestParameters.ServerURL}}/API/Proxy.svc/ajax/GetUrl?url=%22http%3A%2F%2Ffeeds.feedburner.com%2FOmarAlZabirBlog%22&amp;cacheDuration=10" ThinkTime="0" Timeout="300" ParseDependentRequests="True" FollowRedirects="True" RecordResult="True" Cache="False" ResponseTimeGoal="0" Encoding="utf-8" ExpectedHttpStatusCode="0" ExpectedResponseUrl="" ReportingName="">
      <ValidationRules>
        <ValidationRule Classname="Microsoft.VisualStudio.TestTools.WebTesting.Rules.ValidationRuleFindText, Microsoft.VisualStudio.QualityTools.WebTestFramework, Version=10.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a" DisplayName="Find Text" Description="Verifies the existence of the specified text in the response." Level="High" ExectuionOrder="BeforeDependents">
          <RuleParameters>
            <RuleParameter Name="FindText" Value="&lt;channel&gt;" />
            <RuleParameter Name="IgnoreCase" Value="False" />
            <RuleParameter Name="UseRegularExpression" Value="False" />
            <RuleParameter Name="PassIfTextFound" Value="True" />
          </RuleParameters>
        </ValidationRule>
      </ValidationRules>
    </Request>
    <TransactionTimer Name="Edit Collapse Expand Widget">
      <Items>
        <Comment CommentText="Click edit on first widget &quot;How to of the Day&quot; and expect URL textbox to be present with Feed Url" />
        <Request Method="GET" Version="1.1" Url="{{Config.TestParameters.ServerURL}}/Default.aspx" ThinkTime="0" Timeout="300" ParseDependentRequests="False" FollowRedirects="True" RecordResult="True" Cache="False" ResponseTimeGoal="0.5" Encoding="utf-8" ExpectedHttpStatusCode="0" ExpectedResponseUrl="" ReportingName="">
          <ValidationRules>
            <ValidationRule Classname="Microsoft.VisualStudio.TestTools.WebTesting.Rules.ValidationRuleRequiredAttributeValue, Microsoft.VisualStudio.QualityTools.WebTestFramework, Version=10.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a" DisplayName="Required Attribute Value" Description="Verifies the existence of a specified HTML tag that contains an attribute with a specified value." Level="High" ExectuionOrder="BeforeDependents">
              <RuleParameters>
                <RuleParameter Name="TagName" Value="input" />
                <RuleParameter Name="AttributeName" Value="value" />
                <RuleParameter Name="MatchAttributeName" Value="" />
                <RuleParameter Name="MatchAttributeValue" Value="" />
                <RuleParameter Name="ExpectedValue" Value="http://www.wikihow.com/feed.rss" />
                <RuleParameter Name="IgnoreCase" Value="False" />
                <RuleParameter Name="Index" Value="-1" />
              </RuleParameters>
            </ValidationRule>
          </ValidationRules>
          <ExtractionRules>
            <ExtractionRule Classname="Dropthings.Test.Rules.ExtractFormElements, Dropthings.Test, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null" VariableName="" DisplayName="Extract Form Elements" Description="">
              <RuleParameters>
                <RuleParameter Name="ContextParameterName" Value="" />
              </RuleParameters>
            </ExtractionRule>
          </ExtractionRules>
          <RequestPlugins>
            <RequestPlugin Classname="Dropthings.Test.Plugin.AsyncPostbackRequestPlugin, Dropthings.Test, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null" DisplayName="AsyncPostbackRequestPlugin" Description="">
              <RuleParameters>
                <RuleParameter Name="ControlName" Value="{{$POSTBACK.EditWidget.1}}" />
                <RuleParameter Name="UpdatePanelName" Value="{{$UPDATEPANEL.WidgetHeaderUpdatePanel.1}}" />
              </RuleParameters>
            </RequestPlugin>
          </RequestPlugins>
        </Request>
        <Comment CommentText="Change the Feed Count Dropdown list to 10 and expect 10 Feed Link controls are generated" />
        <Request Method="POST" Version="1.1" Url="{{Config.TestParameters.ServerURL}}/Default.aspx" ThinkTime="0" Timeout="300" ParseDependentRequests="False" FollowRedirects="True" RecordResult="True" Cache="False" ResponseTimeGoal="0.5" Encoding="utf-8" ExpectedHttpStatusCode="0" ExpectedResponseUrl="" ReportingName="">
          <ValidationRules>
            <ValidationRule Classname="Microsoft.VisualStudio.TestTools.WebTesting.Rules.ValidationRuleFindText, Microsoft.VisualStudio.QualityTools.WebTestFramework, Version=10.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a" DisplayName="Find Text" Description="Verifies the existence of the specified text in the response." Level="High" ExectuionOrder="BeforeDependents">
              <RuleParameters>
                <RuleParameter Name="FindText" Value="FeedList_ctl09_FeedLink" />
                <RuleParameter Name="IgnoreCase" Value="False" />
                <RuleParameter Name="UseRegularExpression" Value="False" />
                <RuleParameter Name="PassIfTextFound" Value="True" />
              </RuleParameters>
            </ValidationRule>
          </ValidationRules>
          <RequestPlugins>
            <RequestPlugin Classname="Dropthings.Test.Plugin.AsyncPostbackRequestPlugin, Dropthings.Test, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null" DisplayName="AsyncPostbackRequestPlugin" Description="">
              <RuleParameters>
                <RuleParameter Name="ControlName" Value="{{$POSTBACK.CancelEditWidget.1}}" />
                <RuleParameter Name="UpdatePanelName" Value="{{$UPDATEPANEL.WidgetHeaderUpdatePanel.1}}" />
              </RuleParameters>
            </RequestPlugin>
          </RequestPlugins>
          <FormPostHttpBody>
            <FormPostParameter Name="{{$INPUT.FeedUrl.1}}" Value="http://www.wikihow.com/feed.rss" RecordedValue="" CorrelationBinding="" UrlEncode="True" />
            <FormPostParameter Name="{{$SELECT.FeedCountDropDownList.1}}" Value="10" RecordedValue="" CorrelationBinding="" UrlEncode="True" />
          </FormPostHttpBody>
        </Request>
        <Comment CommentText="Delete the How to of the Day widget and expect it's not found from response" />
        <Request Method="GET" Version="1.1" Url="{{Config.TestParameters.ServerURL}}/Default.aspx" ThinkTime="0" Timeout="300" ParseDependentRequests="False" FollowRedirects="True" RecordResult="True" Cache="False" ResponseTimeGoal="0.5" Encoding="utf-8" ExpectedHttpStatusCode="0" ExpectedResponseUrl="" ReportingName="">
          <ValidationRules>
            <ValidationRule Classname="Microsoft.VisualStudio.TestTools.WebTesting.Rules.ValidationRuleFindText, Microsoft.VisualStudio.QualityTools.WebTestFramework, Version=10.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a" DisplayName="Find Text" Description="Verifies the existence of the specified text in the response." Level="High" ExectuionOrder="BeforeDependents">
              <RuleParameters>
                <RuleParameter Name="FindText" Value="How to of the Day" />
                <RuleParameter Name="IgnoreCase" Value="False" />
                <RuleParameter Name="UseRegularExpression" Value="False" />
                <RuleParameter Name="PassIfTextFound" Value="False" />
              </RuleParameters>
            </ValidationRule>
          </ValidationRules>
          <RequestPlugins>
            <RequestPlugin Classname="Dropthings.Test.Plugin.AsyncPostbackRequestPlugin, Dropthings.Test, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null" DisplayName="AsyncPostbackRequestPlugin" Description="">
              <RuleParameters>
                <RuleParameter Name="ControlName" Value="{{$POSTBACK.CloseWidget.1}}" />
                <RuleParameter Name="UpdatePanelName" Value="{{$UPDATEPANEL.WidgetHeaderUpdatePanel.1}}" />
              </RuleParameters>
            </RequestPlugin>
          </RequestPlugins>
        </Request>
      </Items>
    </TransactionTimer>
    <TransactionTimer Name="Add New Widget">
      <Items>
        <Comment CommentText="Show widget list and expect Digg to be there" />
        <Request Method="GET" Version="1.1" Url="{{Config.TestParameters.ServerURL}}/Default.aspx" ThinkTime="0" Timeout="300" ParseDependentRequests="False" FollowRedirects="True" RecordResult="True" Cache="False" ResponseTimeGoal="0.5" Encoding="utf-8" ExpectedHttpStatusCode="0" ExpectedResponseUrl="" ReportingName="">
          <ValidationRules>
            <ValidationRule Classname="Microsoft.VisualStudio.TestTools.WebTesting.Rules.ValidationRuleFindText, Microsoft.VisualStudio.QualityTools.WebTestFramework, Version=10.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a" DisplayName="Find Text" Description="Verifies the existence of the specified text in the response." Level="High" ExectuionOrder="BeforeDependents">
              <RuleParameters>
                <RuleParameter Name="FindText" Value="Digg" />
                <RuleParameter Name="IgnoreCase" Value="False" />
                <RuleParameter Name="UseRegularExpression" Value="False" />
                <RuleParameter Name="PassIfTextFound" Value="True" />
              </RuleParameters>
            </ValidationRule>
          </ValidationRules>
          <RequestPlugins>
            <RequestPlugin Classname="Dropthings.Test.Plugin.AsyncPostbackRequestPlugin, Dropthings.Test, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null" DisplayName="AsyncPostbackRequestPlugin" Description="">
              <RuleParameters>
                <RuleParameter Name="ControlName" Value="TabControlPanel$ShowAddContentPanel" />
                <RuleParameter Name="UpdatePanelName" Value="{{$UPDATEPANEL.OnPageMenuUpdatePanel.1}}" />
              </RuleParameters>
            </RequestPlugin>
          </RequestPlugins>
        </Request>
        <Comment CommentText="Add New Widget" />
        <Request Method="GET" Version="1.1" Url="{{Config.TestParameters.ServerURL}}/Default.aspx" ThinkTime="0" Timeout="300" ParseDependentRequests="False" FollowRedirects="True" RecordResult="True" Cache="False" ResponseTimeGoal="0.5" Encoding="utf-8" ExpectedHttpStatusCode="0" ExpectedResponseUrl="" ReportingName="">
          <ValidationRules>
            <ValidationRule Classname="Microsoft.VisualStudio.TestTools.WebTesting.Rules.ValidationRuleFindText, Microsoft.VisualStudio.QualityTools.WebTestFramework, Version=10.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a" DisplayName="Find Text" Description="Verifies the existence of the specified text in the response." Level="High" ExectuionOrder="BeforeDependents">
              <RuleParameters>
                <RuleParameter Name="FindText" Value="Digg" />
                <RuleParameter Name="IgnoreCase" Value="False" />
                <RuleParameter Name="UseRegularExpression" Value="False" />
                <RuleParameter Name="PassIfTextFound" Value="True" />
              </RuleParameters>
            </ValidationRule>
          </ValidationRules>
          <RequestPlugins>
            <RequestPlugin Classname="Dropthings.Test.Plugin.AsyncPostbackRequestPlugin, Dropthings.Test, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null" DisplayName="AsyncPostbackRequestPlugin" Description="">
              <RuleParameters>
                <RuleParameter Name="ControlName" Value="{{$POSTBACK.AddWidget.1}}" />
                <RuleParameter Name="UpdatePanelName" Value="{{$UPDATEPANEL.OnPageMenuUpdatePanel.1}}" />
              </RuleParameters>
            </RequestPlugin>
          </RequestPlugins>
        </Request>
        <Comment CommentText="Delete the newly added widget" />
        <Request Method="GET" Version="1.1" Url="{{Config.TestParameters.ServerURL}}/Default.aspx" ThinkTime="0" Timeout="300" ParseDependentRequests="False" FollowRedirects="True" RecordResult="True" Cache="False" ResponseTimeGoal="0.5" Encoding="utf-8" ExpectedHttpStatusCode="0" ExpectedResponseUrl="" ReportingName="">
          <ValidationRules>
            <ValidationRule Classname="Microsoft.VisualStudio.TestTools.WebTesting.Rules.ValidationRuleFindText, Microsoft.VisualStudio.QualityTools.WebTestFramework, Version=10.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a" DisplayName="Find Text" Description="Verifies the existence of the specified text in the response." Level="High" ExectuionOrder="BeforeDependents">
              <RuleParameters>
                <RuleParameter Name="FindText" Value="How to of the Day" />
                <RuleParameter Name="IgnoreCase" Value="False" />
                <RuleParameter Name="UseRegularExpression" Value="False" />
                <RuleParameter Name="PassIfTextFound" Value="False" />
              </RuleParameters>
            </ValidationRule>
          </ValidationRules>
          <RequestPlugins>
            <RequestPlugin Classname="Dropthings.Test.Plugin.AsyncPostbackRequestPlugin, Dropthings.Test, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null" DisplayName="AsyncPostbackRequestPlugin" Description="">
              <RuleParameters>
                <RuleParameter Name="ControlName" Value="{{$POSTBACK.CloseWidget.1}}" />
                <RuleParameter Name="UpdatePanelName" Value="{{$UPDATEPANEL.WidgetHeaderUpdatePanel.1}}" />
              </RuleParameters>
            </RequestPlugin>
          </RequestPlugins>
        </Request>
      </Items>
    </TransactionTimer>
    <Comment CommentText="Revisit and ensure the Digg widget exists and How to widget does not exist" />
    <Request Method="GET" Version="1.1" Url="{{Config.TestParameters.ServerURL}}/Default.aspx" ThinkTime="0" Timeout="300" ParseDependentRequests="False" FollowRedirects="True" RecordResult="True" Cache="False" ResponseTimeGoal="0.5" Encoding="utf-8" ExpectedHttpStatusCode="0" ExpectedResponseUrl="" ReportingName="">
      <ValidationRules>
        <ValidationRule Classname="Dropthings.Test.Rules.CookieValidationRule, Dropthings.Test, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null" DisplayName="Check Cookie From Response" Description="" Level="High" ExectuionOrder="BeforeDependents">
          <RuleParameters>
            <RuleParameter Name="StopOnError" Value="False" />
            <RuleParameter Name="CookieValueToMatch" Value="" />
            <RuleParameter Name="MatchValue" Value="False" />
            <RuleParameter Name="Exists" Value="False" />
            <RuleParameter Name="CookieName" Value="{{Config.TestParameters.AnonCookieName}}" />
            <RuleParameter Name="IsPersistent" Value="True" />
            <RuleParameter Name="Domain" Value="" />
            <RuleParameter Name="Index" Value="0" />
          </RuleParameters>
        </ValidationRule>
        <ValidationRule Classname="Dropthings.Test.Rules.CookieValidationRule, Dropthings.Test, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null" DisplayName="Check Cookie From Response" Description="" Level="High" ExectuionOrder="BeforeDependents">
          <RuleParameters>
            <RuleParameter Name="StopOnError" Value="False" />
            <RuleParameter Name="CookieValueToMatch" Value="" />
            <RuleParameter Name="MatchValue" Value="False" />
            <RuleParameter Name="Exists" Value="False" />
            <RuleParameter Name="CookieName" Value="{{Config.TestParameters.SessionCookieName}}" />
            <RuleParameter Name="IsPersistent" Value="False" />
            <RuleParameter Name="Domain" Value="" />
            <RuleParameter Name="Index" Value="0" />
          </RuleParameters>
        </ValidationRule>
        <ValidationRule Classname="Dropthings.Test.Rules.CacheHeaderValidation, Dropthings.Test, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null" DisplayName="Cache Header Validation" Description="" Level="High" ExectuionOrder="BeforeDependents">
          <RuleParameters>
            <RuleParameter Name="Enabled" Value="True" />
            <RuleParameter Name="DifferenceThresholdSec" Value="0" />
            <RuleParameter Name="CacheControlPrivate" Value="False" />
            <RuleParameter Name="CacheControlPublic" Value="False" />
            <RuleParameter Name="CacheControlNoCache" Value="True" />
            <RuleParameter Name="ExpiresAfterSeconds" Value="0" />
            <RuleParameter Name="StopOnError" Value="False" />
          </RuleParameters>
        </ValidationRule>
        <ValidationRule Classname="Microsoft.VisualStudio.TestTools.WebTesting.Rules.ValidationRuleFindText, Microsoft.VisualStudio.QualityTools.WebTestFramework, Version=10.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a" DisplayName="Find Text" Description="Verifies the existence of the specified text in the response." Level="High" ExectuionOrder="BeforeDependents">
          <RuleParameters>
            <RuleParameter Name="FindText" Value="How to of the Day" />
            <RuleParameter Name="IgnoreCase" Value="False" />
            <RuleParameter Name="UseRegularExpression" Value="False" />
            <RuleParameter Name="PassIfTextFound" Value="False" />
          </RuleParameters>
        </ValidationRule>
        <ValidationRule Classname="Microsoft.VisualStudio.TestTools.WebTesting.Rules.ValidationRuleFindText, Microsoft.VisualStudio.QualityTools.WebTestFramework, Version=10.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a" DisplayName="Find Text" Description="Verifies the existence of the specified text in the response." Level="High" ExectuionOrder="BeforeDependents">
          <RuleParameters>
            <RuleParameter Name="FindText" Value="Digg" />
            <RuleParameter Name="IgnoreCase" Value="False" />
            <RuleParameter Name="UseRegularExpression" Value="False" />
            <RuleParameter Name="PassIfTextFound" Value="True" />
          </RuleParameters>
        </ValidationRule>
        <ValidationRule Classname="Microsoft.VisualStudio.TestTools.WebTesting.Rules.ValidationRuleFindText, Microsoft.VisualStudio.QualityTools.WebTestFramework, Version=10.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a" DisplayName="Find Text" Description="Verifies the existence of the specified text in the response." Level="High" ExectuionOrder="BeforeDependents">
          <RuleParameters>
            <RuleParameter Name="FindText" Value="All rights reserved" />
            <RuleParameter Name="IgnoreCase" Value="False" />
            <RuleParameter Name="UseRegularExpression" Value="False" />
            <RuleParameter Name="PassIfTextFound" Value="True" />
          </RuleParameters>
        </ValidationRule>
      </ValidationRules>
    </Request>
    <Comment CommentText="- Logout and ensure Anon Cookie is set to expire" />
    <Request Method="GET" Version="1.1" Url="{{Config.TestParameters.ServerURL}}/Logout.ashx" ThinkTime="0" Timeout="300" ParseDependentRequests="False" FollowRedirects="False" RecordResult="True" Cache="False" ResponseTimeGoal="0.5" Encoding="utf-8" ExpectedHttpStatusCode="302" ExpectedResponseUrl="" ReportingName="">
      <ValidationRules>
        <ValidationRule Classname="Dropthings.Test.Rules.CookieSetToExpire, Dropthings.Test, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null" DisplayName="Ensure Cookie Set to Expire" Description="" Level="High" ExectuionOrder="BeforeDependents">
          <RuleParameters>
            <RuleParameter Name="CookieName" Value="{{Config.TestParameters.AnonCookieName}}" />
            <RuleParameter Name="Domain" Value="" />
            <RuleParameter Name="StopOnError" Value="False" />
          </RuleParameters>
        </ValidationRule>
      </ValidationRules>
    </Request>
  </Items>
  <DataSources>
    <DataSource Name="Config" Provider="Microsoft.VisualStudio.TestTools.DataSource.XML" Connection="|DataDirectory|\Config\TestParameters.xml">
      <Tables>
        <DataSourceTable Name="TestParameters" SelectColumns="SelectOnlyBoundColumns" AccessMethod="Sequential" />
      </Tables>
    </DataSource>
  </DataSources>
  <ValidationRules>
    <ValidationRule Classname="Microsoft.VisualStudio.TestTools.WebTesting.Rules.ValidateResponseUrl, Microsoft.VisualStudio.QualityTools.WebTestFramework, Version=10.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a" DisplayName="Response URL" Description="Validates that the response URL after redirects are followed is the same as the recorded response URL.  QueryString parameters are ignored." Level="Low" ExectuionOrder="BeforeDependents" />
  </ValidationRules>
  <WebTestPlugins>
    <WebTestPlugin Classname="Dropthings.Test.Plugin.ASPNETWebTestPlugin, Dropthings.Test, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null" DisplayName="ASPNETWebTestPlugin" Description="" />
  </WebTestPlugins>
</WebTest>
`)
