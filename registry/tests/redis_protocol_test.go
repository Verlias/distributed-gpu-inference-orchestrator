package tests

import (
	"bufio"
	"bytes"
	"strings"
	"testing"

	"github.com/Verlias/distributed-gpu-inference-orchestrator/registry/internal/resp"
)

func TestRedisCommandEncoding(t *testing.T) {
	var output bytes.Buffer
	if err := resp.WriteCommand(&output, "HGETALL", "worker:1"); err != nil {
		t.Fatalf("encode Redis command: %v", err)
	}
	expected := "*2\r\n$7\r\nHGETALL\r\n$8\r\nworker:1\r\n"
	if output.String() != expected {
		t.Fatalf("unexpected RESP command:\nwant %q\n got %q", expected, output.String())
	}
}

func TestRedisResponseDecoding(t *testing.T) {
	tests := []struct {
		name     string
		response string
		expected any
	}{
		{name: "simple string", response: "+PONG\r\n", expected: "PONG"},
		{name: "integer", response: ":3\r\n", expected: int64(3)},
		{name: "bulk string", response: "$5\r\nhello\r\n", expected: "hello"},
		{name: "array", response: "*2\r\n$3\r\none\r\n$3\r\ntwo\r\n", expected: []any{"one", "two"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value, err := resp.ReadResponse(bufio.NewReader(strings.NewReader(test.response)))
			if err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if !equalRESPValue(value, test.expected) {
				t.Fatalf("expected %#v, got %#v", test.expected, value)
			}
		})
	}
}

func equalRESPValue(left, right any) bool {
	leftArray, leftIsArray := left.([]any)
	rightArray, rightIsArray := right.([]any)
	if leftIsArray || rightIsArray {
		if !leftIsArray || !rightIsArray || len(leftArray) != len(rightArray) {
			return false
		}
		for index := range leftArray {
			if !equalRESPValue(leftArray[index], rightArray[index]) {
				return false
			}
		}
		return true
	}
	return left == right
}
