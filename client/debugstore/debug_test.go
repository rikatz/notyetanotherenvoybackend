package debugstore

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	resourceapi "github.com/agentgateway/agentgateway/api"
	"google.golang.org/protobuf/proto"
	"istio.io/istio/pkg/workloadapi"
)

func TestNotifyMsg_Update(t *testing.T) {
	resource := &resourceapi.Resource{}

	msg := NotifyMsg{
		Operation: "UPDATE",
		Resources: []proto.Message{resource},
	}

	if msg.Operation != "UPDATE" {
		t.Errorf("Expected operation 'UPDATE', got '%s'", msg.Operation)
	}

	if len(msg.Resources) != 1 {
		t.Errorf("Expected 1 resource, got %d", len(msg.Resources))
	}

	if msg.ResourcesRemoved != nil {
		t.Error("Expected ResourcesRemoved to be nil for UPDATE")
	}
}

func TestNotifyMsg_Remove(t *testing.T) {
	msg := NotifyMsg{
		Operation:        "REMOVE",
		ResourcesRemoved: []string{"resource-1", "resource-2"},
	}

	if msg.Operation != "REMOVE" {
		t.Errorf("Expected operation 'REMOVE', got '%s'", msg.Operation)
	}

	if len(msg.ResourcesRemoved) != 2 {
		t.Errorf("Expected 2 removed resources, got %d", len(msg.ResourcesRemoved))
	}

	if msg.Resources != nil {
		t.Error("Expected Resources to be nil for REMOVE")
	}
}

func TestNotifyMsg_Empty(t *testing.T) {
	msg := NotifyMsg{}

	if msg.Operation != "" {
		t.Errorf("Expected empty operation, got '%s'", msg.Operation)
	}

	if msg.Resources != nil {
		t.Error("Expected nil Resources")
	}

	if msg.ResourcesRemoved != nil {
		t.Error("Expected nil ResourcesRemoved")
	}
}

func TestDebug_WithDebugLogging(t *testing.T) {
	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Set log level to debug
	logger := slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	ctx := context.Background()
	resource := &resourceapi.Resource{}

	debug(ctx, resource)

	// Restore stdout
	w.Close()
	os.Stdout = old

	// Read captured output
	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	// Should contain debug-level log output with the message
	if !strings.Contains(output, "DEBUG") || !strings.Contains(output, "xDS resource details") {
		t.Errorf("Expected DEBUG output with 'xDS resource details', got: %s", output)
	}
}

func TestDebug_WithoutDebugLogging(t *testing.T) {
	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Set log level to INFO (higher than DEBUG)
	logger := slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	ctx := context.Background()
	resource := &resourceapi.Resource{}

	debug(ctx, resource)

	// Restore stdout
	w.Close()
	os.Stdout = old

	// Read captured output
	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	// Should NOT contain debug-level output
	if strings.Contains(output, "xDS resource details") {
		t.Error("Expected no debug output when debug logging is disabled")
	}
}

func TestDebug_WithWorkloadAddress(t *testing.T) {
	// Capture stdout for verification
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Enable debug logging
	logger := slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	ctx := context.Background()
	workload := &workloadapi.Workload{
		Addresses: [][]byte{{192, 168, 1, 100}},
		Uid:       "test-workload",
	}

	addr := &workloadapi.Address{
		Type: &workloadapi.Address_Workload{
			Workload: workload,
		},
	}

	debug(ctx, addr)

	// Restore stdout
	w.Close()
	os.Stdout = old

	// Read captured output
	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	// Should contain debug output
	if !strings.Contains(output, "DEBUG") || !strings.Contains(output, "xDS resource details") {
		t.Errorf("Expected DEBUG output for workload address, got: %s", output)
	}
}

func TestDebug_NilMessage(t *testing.T) {
	// Should handle nil gracefully
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("debug() panicked with nil message: %v", r)
		}
	}()

	// Capture output to avoid cluttering test output
	old := os.Stdout
	_, w, _ := os.Pipe()
	os.Stdout = w

	logger := slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	ctx := context.Background()
	debug(ctx, nil)

	w.Close()
	os.Stdout = old
}

func TestDebug_MultipleMessages(t *testing.T) {
	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	logger := slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	ctx := context.Background()

	// Debug multiple messages
	for range 3 {
		resource := &resourceapi.Resource{}
		debug(ctx, resource)
	}

	// Restore stdout
	w.Close()
	os.Stdout = old

	// Read captured output
	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	// Should contain multiple debug entries (count by "xDS resource details")
	debugCount := strings.Count(output, "xDS resource details")
	if debugCount < 3 {
		t.Errorf("Expected at least 3 debug entries, found %d. Output: %s", debugCount, output)
	}
}

func TestJSONMarshaler_Options(t *testing.T) {
	// Verify the marshaler has correct options
	if !jsonMarshaler.Multiline {
		t.Error("Expected Multiline to be true")
	}

	if jsonMarshaler.Indent != "  " {
		t.Errorf("Expected Indent to be '  ', got '%s'", jsonMarshaler.Indent)
	}

	if !jsonMarshaler.EmitUnpopulated {
		t.Error("Expected EmitUnpopulated to be true")
	}
}

func TestDebug_JSONFormat(t *testing.T) {
	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	logger := slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	ctx := context.Background()
	resource := &resourceapi.Resource{}

	debug(ctx, resource)

	// Restore stdout
	w.Close()
	os.Stdout = old

	// Read captured output
	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	// Should contain JSON in the log output
	// The JSON is in the "json" field of the structured log
	if !strings.Contains(output, "{") || !strings.Contains(output, "}") {
		t.Errorf("Expected JSON format in debug output, got: %s", output)
	}
}

func TestNotifyMsg_MultipleResources(t *testing.T) {
	resources := []proto.Message{
		&resourceapi.Resource{},
		&resourceapi.Resource{},
		&resourceapi.Resource{},
	}

	msg := NotifyMsg{
		Operation: "UPDATE",
		Resources: resources,
	}

	if len(msg.Resources) != 3 {
		t.Errorf("Expected 3 resources, got %d", len(msg.Resources))
	}

	// Verify each resource is the correct type
	for i, res := range msg.Resources {
		if _, ok := res.(*resourceapi.Resource); !ok {
			t.Errorf("Resource %d is not *resourceapi.Resource", i)
		}
	}
}

func TestNotifyMsg_MixedTypes(t *testing.T) {
	// NotifyMsg can hold different proto.Message types
	resources := []proto.Message{
		&resourceapi.Resource{},
		&workloadapi.Workload{Uid: "workload"},
	}

	msg := NotifyMsg{
		Operation: "UPDATE",
		Resources: resources,
	}

	if len(msg.Resources) != 2 {
		t.Errorf("Expected 2 resources, got %d", len(msg.Resources))
	}

	// First should be Resource
	if _, ok := msg.Resources[0].(*resourceapi.Resource); !ok {
		t.Error("First message should be *resourceapi.Resource")
	}

	// Second should be Workload
	if _, ok := msg.Resources[1].(*workloadapi.Workload); !ok {
		t.Error("Second message should be *workloadapi.Workload")
	}
}
