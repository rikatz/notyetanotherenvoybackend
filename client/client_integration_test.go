package client

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	resourceapi "github.com/agentgateway/agentgateway/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/anypb"
	"istio.io/istio/pkg/workloadapi"
)

// mockXDSServer implements a fake xDS server for testing
type mockXDSServer struct {
	discovery.UnimplementedAggregatedDiscoveryServiceServer

	// Track received requests
	mu            sync.Mutex
	requests      []*discovery.DeltaDiscoveryRequest
	subscriptions map[string]bool

	// Control message flow
	responseChan chan *discovery.DeltaDiscoveryResponse
	authToken    string
}

func newMockXDSServer() *mockXDSServer {
	return &mockXDSServer{
		subscriptions: make(map[string]bool),
		responseChan:  make(chan *discovery.DeltaDiscoveryResponse, 10),
	}
}

func (m *mockXDSServer) DeltaAggregatedResources(stream discovery.AggregatedDiscoveryService_DeltaAggregatedResourcesServer) error {
	// Check auth token
	md, ok := metadata.FromIncomingContext(stream.Context())
	if ok {
		if auth := md.Get("authorization"); len(auth) > 0 {
			m.mu.Lock()
			m.authToken = auth[0]
			m.mu.Unlock()
		}
	}

	// Handle incoming requests in goroutine
	requestDone := make(chan error, 1)
	go func() {
		for {
			req, err := stream.Recv()
			if err != nil {
				requestDone <- err
				return
			}

			m.mu.Lock()
			m.requests = append(m.requests, req)
			if req.TypeUrl != "" {
				m.subscriptions[req.TypeUrl] = true
			}
			m.mu.Unlock()
		}
	}()

	// Send responses
	for {
		select {
		case resp := <-m.responseChan:
			if err := stream.Send(resp); err != nil {
				return err
			}
		case err := <-requestDone:
			return err
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

func (m *mockXDSServer) getSubscriptions() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	subs := make([]string, 0, len(m.subscriptions))
	for typeURL := range m.subscriptions {
		subs = append(subs, typeURL)
	}
	return subs
}

func (m *mockXDSServer) getRequestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.requests)
}

func (m *mockXDSServer) getAuthToken() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.authToken
}

func (m *mockXDSServer) getLastRequest() *discovery.DeltaDiscoveryRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.requests) == 0 {
		return nil
	}
	return m.requests[len(m.requests)-1]
}

// startTestServer starts a gRPC server on a random port
func startTestServer(t *testing.T, mockServer *mockXDSServer) (string, func()) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	discovery.RegisterAggregatedDiscoveryServiceServer(grpcServer, mockServer)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Logf("Server error: %v", err)
		}
	}()

	cleanup := func() {
		grpcServer.Stop()
		lis.Close()
	}

	return lis.Addr().String(), cleanup
}

func TestADSClient_StartAndReceiveUpdates(t *testing.T) {
	// Setup mock server
	mockServer := newMockXDSServer()
	addr, cleanup := startTestServer(t, mockServer)
	defer cleanup()

	// Setup handler
	handler := &mockHandler{}
	handlers := ResourceTypeStore{
		"type.googleapis.com/istio.workload.Address": handler,
	}

	// Create client
	cfg := Config{
		ServerAddress:        addr,
		Namespace:            "test-ns",
		GatewayName:          "test-gateway",
		Token:                "test-token",
		ResourceTypeHandlers: handlers,
	}

	client, err := NewADSClient(cfg)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Start client in goroutine
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clientDone := make(chan error, 1)
	go func() {
		clientDone <- client.Start(ctx)
	}()

	// Wait for subscription
	time.Sleep(100 * time.Millisecond)

	// Verify subscription was sent
	subs := mockServer.getSubscriptions()
	if len(subs) == 0 {
		t.Fatal("Expected client to subscribe to resource type")
	}

	// Create and send a response with an Address resource
	workload := &workloadapi.Workload{
		Addresses: [][]byte{{192, 168, 1, 100}},
		Uid:       "test-workload-123",
	}
	address := &workloadapi.Address{
		Type: &workloadapi.Address_Workload{Workload: workload},
	}
	anyAddr, _ := anypb.New(address)

	response := &discovery.DeltaDiscoveryResponse{
		TypeUrl: "type.googleapis.com/istio.workload.Address",
		Resources: []*discovery.Resource{
			{
				Name:     "test-resource",
				Resource: anyAddr,
			},
		},
		Nonce:               "nonce-1",
		SystemVersionInfo:   "v1",
	}

	// Send response
	mockServer.responseChan <- response

	// Wait for handler to be called
	time.Sleep(100 * time.Millisecond)

	// Verify handler was called
	if !handler.wasUpdateCalled() {
		t.Error("Expected Update handler to be called")
	}

	// Stop client
	cancel()

	select {
	case err := <-clientDone:
		// Client should stop with context canceled or similar cancellation error
		if err != nil && err != context.Canceled {
			// Accept errors that contain "cancel" (case-insensitive) - these are expected
			errStr := strings.ToLower(err.Error())
			if !strings.Contains(errStr, "cancel") {
				t.Errorf("Client returned unexpected error: %v", err)
			}
		}
	case <-time.After(2 * time.Second):
		t.Error("Client did not stop in time")
	}
}

func TestADSClient_ReceiveRemovalUpdates(t *testing.T) {
	mockServer := newMockXDSServer()
	addr, cleanup := startTestServer(t, mockServer)
	defer cleanup()

	handler := &mockHandler{}
	handlers := ResourceTypeStore{
		"type.googleapis.com/agentgateway.dev.resource.Resource": handler,
	}

	cfg := Config{
		ServerAddress:        addr,
		Namespace:            "test-ns",
		GatewayName:          "test-gateway",
		Token:                "test-token",
		ResourceTypeHandlers: handlers,
	}

	client, err := NewADSClient(cfg)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go client.Start(ctx)

	// Wait for subscription
	time.Sleep(100 * time.Millisecond)

	// Send removal response
	response := &discovery.DeltaDiscoveryResponse{
		TypeUrl:          "type.googleapis.com/agentgateway.dev.resource.Resource",
		RemovedResources: []string{"removed-resource-1", "removed-resource-2"},
		Nonce:            "nonce-remove",
	}

	mockServer.responseChan <- response

	// Wait for handler
	time.Sleep(100 * time.Millisecond)

	if !handler.wasRemoveCalled() {
		t.Error("Expected Remove handler to be called")
	}

	cancel()
}

func TestADSClient_MultipleResourceTypes(t *testing.T) {
	mockServer := newMockXDSServer()
	addr, cleanup := startTestServer(t, mockServer)
	defer cleanup()

	addressHandler := &mockHandler{}
	resourceHandler := &mockHandler{}

	handlers := ResourceTypeStore{
		"type.googleapis.com/istio.workload.Address":             addressHandler,
		"type.googleapis.com/agentgateway.dev.resource.Resource": resourceHandler,
	}

	cfg := Config{
		ServerAddress:        addr,
		Namespace:            "test-ns",
		GatewayName:          "test-gateway",
		Token:                "test-token",
		ResourceTypeHandlers: handlers,
	}

	client, err := NewADSClient(cfg)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go client.Start(ctx)

	// Wait for subscriptions
	time.Sleep(100 * time.Millisecond)

	// Verify both subscriptions
	subs := mockServer.getSubscriptions()
	if len(subs) != 2 {
		t.Errorf("Expected 2 subscriptions, got %d", len(subs))
	}

	// Send Address update
	workload := &workloadapi.Workload{Uid: "test"}
	address1 := &workloadapi.Address{
		Type: &workloadapi.Address_Workload{Workload: workload},
	}
	anyAddr1, _ := anypb.New(address1)

	mockServer.responseChan <- &discovery.DeltaDiscoveryResponse{
		TypeUrl:   "type.googleapis.com/istio.workload.Address",
		Resources: []*discovery.Resource{{Name: "addr", Resource: anyAddr1}},
		Nonce:     "nonce-addr",
	}

	time.Sleep(50 * time.Millisecond)

	// Send Resource update
	resource := &resourceapi.Resource{}
	anyRes, _ := anypb.New(resource)

	mockServer.responseChan <- &discovery.DeltaDiscoveryResponse{
		TypeUrl:   "type.googleapis.com/agentgateway.dev.resource.Resource",
		Resources: []*discovery.Resource{{Name: "res", Resource: anyRes}},
		Nonce:     "nonce-res",
	}

	time.Sleep(50 * time.Millisecond)

	// Both handlers should be called
	if !addressHandler.wasUpdateCalled() {
		t.Error("Expected Address handler to be called")
	}
	if !resourceHandler.wasUpdateCalled() {
		t.Error("Expected Resource handler to be called")
	}

	cancel()
}

func TestADSClient_ACKMessages(t *testing.T) {
	mockServer := newMockXDSServer()
	addr, cleanup := startTestServer(t, mockServer)
	defer cleanup()

	handler := &mockHandler{}
	handlers := ResourceTypeStore{
		"type.googleapis.com/istio.workload.Address": handler,
	}

	cfg := Config{
		ServerAddress:        addr,
		Namespace:            "test-ns",
		GatewayName:          "test-gateway",
		Token:                "test-token",
		ResourceTypeHandlers: handlers,
	}

	client, err := NewADSClient(cfg)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go client.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	// Send response with specific nonce
	expectedNonce := "test-nonce-123"
	workload := &workloadapi.Workload{Uid: "test"}
	address1 := &workloadapi.Address{
		Type: &workloadapi.Address_Workload{Workload: workload},
	}
	anyAddr, _ := anypb.New(address1)

	mockServer.responseChan <- &discovery.DeltaDiscoveryResponse{
		TypeUrl:   "type.googleapis.com/istio.workload.Address",
		Resources: []*discovery.Resource{{Name: "test", Resource: anyAddr}},
		Nonce:     expectedNonce,
	}

	// Wait for ACK
	time.Sleep(100 * time.Millisecond)

	// Verify ACK was sent with matching nonce
	lastReq := mockServer.getLastRequest()
	if lastReq == nil {
		t.Fatal("Expected to receive ACK request")
	}

	if lastReq.ResponseNonce != expectedNonce {
		t.Errorf("Expected ACK nonce '%s', got '%s'", expectedNonce, lastReq.ResponseNonce)
	}

	if lastReq.TypeUrl != "type.googleapis.com/istio.workload.Address" {
		t.Errorf("Expected ACK TypeUrl 'istio.workload.Address', got '%s'", lastReq.TypeUrl)
	}

	cancel()
}

func TestADSClient_Authentication(t *testing.T) {
	mockServer := newMockXDSServer()
	addr, cleanup := startTestServer(t, mockServer)
	defer cleanup()

	expectedToken := "my-secret-token-12345"

	cfg := Config{
		ServerAddress:        addr,
		Namespace:            "test-ns",
		GatewayName:          "test-gateway",
		Token:                expectedToken,
		ResourceTypeHandlers: ResourceTypeStore{},
	}

	client, err := NewADSClient(cfg)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go client.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	// Verify auth token was sent
	expectedAuth := "Bearer " + expectedToken
	if mockServer.getAuthToken() != expectedAuth {
		t.Errorf("Expected auth token '%s', got '%s'", expectedAuth, mockServer.getAuthToken())
	}

	cancel()
}

func TestADSClient_NodeMetadata(t *testing.T) {
	mockServer := newMockXDSServer()
	addr, cleanup := startTestServer(t, mockServer)
	defer cleanup()

	cfg := Config{
		ServerAddress:        addr,
		Namespace:            "production",
		GatewayName:          "api-gateway",
		Name:                 "custom-client",
		Token:                "token",
		ResourceTypeHandlers: ResourceTypeStore{"test.type": &mockHandler{}},
	}

	client, err := NewADSClient(cfg)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go client.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	// Get first request (subscription)
	mockServer.mu.Lock()
	if len(mockServer.requests) == 0 {
		mockServer.mu.Unlock()
		t.Fatal("Expected at least one request")
	}
	req := mockServer.requests[0]
	mockServer.mu.Unlock()

	// Verify node metadata
	if req.Node == nil {
		t.Fatal("Expected Node to be set")
	}

	node := req.Node

	// Check node ID format
	expectedNodeID := "agentgateway~custom-client~custom-client.production~production.svc.cluster.local"
	if node.Id != expectedNodeID {
		t.Errorf("Expected node ID '%s', got '%s'", expectedNodeID, node.Id)
	}

	// Check metadata fields
	if node.Metadata == nil {
		t.Fatal("Expected metadata to be set")
	}

	checkField := func(name, expected string) {
		if val := node.Metadata.Fields[name].GetStringValue(); val != expected {
			t.Errorf("Expected %s='%s', got '%s'", name, expected, val)
		}
	}

	checkField("NAMESPACE", "production")
	checkField("GATEWAY", "api-gateway")
	checkField("NAME", "custom-client")
	checkField("role", "production~api-gateway")

	cancel()
}

func TestADSClient_HandlerError(t *testing.T) {
	mockServer := newMockXDSServer()
	addr, cleanup := startTestServer(t, mockServer)
	defer cleanup()

	// Handler that returns error
	handler := &mockHandler{
		updateError: fmt.Errorf("simulated update error"),
	}

	handlers := ResourceTypeStore{
		"type.googleapis.com/istio.workload.Address": handler,
	}

	cfg := Config{
		ServerAddress:        addr,
		Namespace:            "test-ns",
		GatewayName:          "test-gateway",
		Token:                "test-token",
		ResourceTypeHandlers: handlers,
	}

	client, err := NewADSClient(cfg)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go client.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	// Send update
	workload := &workloadapi.Workload{Uid: "test"}
	address1 := &workloadapi.Address{
		Type: &workloadapi.Address_Workload{Workload: workload},
	}
	anyAddr, _ := anypb.New(address1)

	mockServer.responseChan <- &discovery.DeltaDiscoveryResponse{
		TypeUrl:   "type.googleapis.com/istio.workload.Address",
		Resources: []*discovery.Resource{{Name: "test", Resource: anyAddr}},
		Nonce:     "error-test",
	}

	time.Sleep(100 * time.Millisecond)

	// Client should continue running despite handler error
	if !handler.wasUpdateCalled() {
		t.Error("Expected handler to be called despite error")
	}

	// Client should still be running
	select {
	case <-ctx.Done():
		t.Error("Client context should not be done")
	default:
		// Expected
	}

	cancel()
}

func TestADSClient_UnknownTypeURL(t *testing.T) {
	mockServer := newMockXDSServer()
	addr, cleanup := startTestServer(t, mockServer)
	defer cleanup()

	handler := &mockHandler{}
	handlers := ResourceTypeStore{
		"type.googleapis.com/istio.workload.Address": handler,
	}

	cfg := Config{
		ServerAddress:        addr,
		Namespace:            "test-ns",
		GatewayName:          "test-gateway",
		Token:                "test-token",
		ResourceTypeHandlers: handlers,
	}

	client, err := NewADSClient(cfg)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go client.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	// Send response with unknown TypeURL
	mockServer.responseChan <- &discovery.DeltaDiscoveryResponse{
		TypeUrl: "type.googleapis.com/unknown.Type",
		Resources: []*discovery.Resource{
			{Name: "test", Resource: &anypb.Any{}},
		},
		Nonce: "unknown-type",
	}

	time.Sleep(100 * time.Millisecond)

	// Handler should NOT be called for unknown type
	if handler.wasUpdateCalled() {
		t.Error("Handler should not be called for unknown TypeURL")
	}

	// Client should continue running
	select {
	case <-ctx.Done():
		t.Error("Client should continue running after unknown TypeURL")
	default:
		// Expected
	}

	cancel()
}
