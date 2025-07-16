package mcp

import (
	"encoding/json"
	"github.com/mark3labs/mcp-go/mcp"
	"io"
	"net/http"
	"testing"
	"bytes"
)

var initRequest = map[string]any{
	"jsonrpc": "2.0",
	"id":      1,
	"method":  "initialize",
	"params": map[string]any{
		"protocolVersion": "2025-03-26",
		"clientInfo": map[string]any{
			"name":    "test-client",
			"version": "1.0.0",
		},
	},
}

type jsonRPCResponse struct {
	ID     int               `json:"id"`
	Result map[string]any    `json:"result"`
	Error  *mcp.JSONRPCError `json:"error"`
}

func TestMCPServer(t *testing.T) {
	RunTest(t, "TestClientInitializeHTTP", testClientInitializeHTTP)
	// RunTest(t, "TestPingToolCall", testPingToolCall)
}

func testClientInitializeHTTP(suite *testSuite) {
	// Send initialize request
	resp, err := postJSON(suite.GetMCPServerUrl(), initRequest)
	if err != nil {
		suite.t.Fatalf("Failed to send message: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		suite.t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	bodyBytes, _ := io.ReadAll(resp.Body)
	var responseMessage jsonRPCResponse
	if err := json.Unmarshal(bodyBytes, &responseMessage); err != nil {
		suite.t.Fatalf("Failed to unmarshal response: %v", err)
	}
	if responseMessage.Result["protocolVersion"] != "2025-03-26" {
		suite.t.Errorf("Expected protocol version 2025-03-26, got %s", responseMessage.Result["protocolVersion"])
	}
}

// func testPingToolCall(suite *testSuite) {
// }

func postJSON(url string, bodyObject any) (*http.Response, error) {
	jsonBody, _ := json.Marshal(bodyObject)
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	return http.DefaultClient.Do(req)
}
