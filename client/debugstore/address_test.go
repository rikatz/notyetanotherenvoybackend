package debugstore

import (
	"context"
	"net/netip"
	"testing"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/protobuf/types/known/anypb"
	"istio.io/istio/pkg/workloadapi"
)

func TestNewAddressStore(t *testing.T) {
	store := NewAddressStore()

	if store == nil {
		t.Fatal("Expected non-nil store")
	}

	if store.notifyCh == nil {
		t.Error("Expected notifyCh to be initialized")
	}

	if store.wantNotify {
		t.Error("Expected wantNotify to be false by default")
	}
}

func TestAddressStore_Update(t *testing.T) {
	store := NewAddressStore()
	ctx := context.Background()

	// Create a workload address
	workload := &workloadapi.Workload{
		Addresses: [][]byte{
			{192, 168, 1, 100}, // 192.168.1.100
		},
		Uid: "test-workload-uid",
	}

	addr := &workloadapi.Address{
		Type: &workloadapi.Address_Workload{
			Workload: workload,
		},
	}

	anyAddr, err := anypb.New(addr)
	if err != nil {
		t.Fatalf("Failed to create Any: %v", err)
	}

	resources := []*discovery.Resource{
		{
			Name:     "test-resource",
			Resource: anyAddr,
		},
	}

	err = store.Update(ctx, resources)
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	// Verify resource was stored
	val, ok := store.content.Load("test-resource")
	if !ok {
		t.Error("Expected resource to be stored")
	}

	if val == nil {
		t.Error("Expected non-nil stored value")
	}
}

func TestAddressStore_Update_WithNotification(t *testing.T) {
	store := NewAddressStore()
	ctx := context.Background()

	// Enable notifications
	notifyCh := store.Notify()

	workload := &workloadapi.Workload{
		Addresses: [][]byte{{192, 168, 1, 100}},
		Uid:       "test-workload",
	}

	addr := &workloadapi.Address{
		Type: &workloadapi.Address_Workload{
			Workload: workload,
		},
	}

	anyAddr, err := anypb.New(addr)
	if err != nil {
		t.Fatalf("Failed to create Any: %v", err)
	}

	resources := []*discovery.Resource{
		{
			Name:     "test-resource",
			Resource: anyAddr,
		},
	}

	// Run update in goroutine
	errCh := make(chan error, 1)
	go func() {
		errCh <- store.Update(ctx, resources)
	}()

	// Wait for notification
	select {
	case msg := <-notifyCh:
		if msg.Operation != "UPDATE" {
			t.Errorf("Expected operation 'UPDATE', got '%s'", msg.Operation)
		}
		if len(msg.Resources) != 1 {
			t.Errorf("Expected 1 resource in notification, got %d", len(msg.Resources))
		}
	case err := <-errCh:
		if err != nil {
			t.Errorf("Update failed: %v", err)
		}
		t.Error("Expected notification but got none")
	}
}

func TestAddressStore_Update_InvalidResource(t *testing.T) {
	store := NewAddressStore()
	ctx := context.Background()

	// Create invalid Any (not an Address)
	invalidAny := &anypb.Any{
		TypeUrl: "type.googleapis.com/invalid",
		Value:   []byte("invalid data"),
	}

	resources := []*discovery.Resource{
		{
			Name:     "invalid-resource",
			Resource: invalidAny,
		},
	}

	// Should not return error, just log and continue
	err := store.Update(ctx, resources)
	if err != nil {
		t.Errorf("Expected no error for invalid resource, got: %v", err)
	}

	// Resource should not be stored
	_, ok := store.content.Load("invalid-resource")
	if ok {
		t.Error("Expected invalid resource not to be stored")
	}
}

func TestAddressStore_Remove(t *testing.T) {
	store := NewAddressStore()
	ctx := context.Background()

	// Add a resource first
	workload := &workloadapi.Workload{
		Addresses: [][]byte{{192, 168, 1, 100}},
		Uid:       "test-workload",
	}

	addr := &workloadapi.Address{
		Type: &workloadapi.Address_Workload{
			Workload: workload,
		},
	}

	store.content.Store("test-resource", addr)

	// Verify it exists
	_, ok := store.content.Load("test-resource")
	if !ok {
		t.Fatal("Resource should exist before removal")
	}

	// Remove it
	err := store.Remove(ctx, []string{"test-resource"})
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	// Verify it's gone
	_, ok = store.content.Load("test-resource")
	if ok {
		t.Error("Expected resource to be removed")
	}
}

func TestAddressStore_Remove_WithNotification(t *testing.T) {
	store := NewAddressStore()
	ctx := context.Background()

	// Enable notifications
	notifyCh := store.Notify()

	// Add and then remove
	store.content.Store("test-resource", &workloadapi.Address{})

	// Run removal in goroutine
	errCh := make(chan error, 1)
	go func() {
		errCh <- store.Remove(ctx, []string{"test-resource"})
	}()

	// Wait for notification
	select {
	case msg := <-notifyCh:
		if msg.Operation != "REMOVE" {
			t.Errorf("Expected operation 'REMOVE', got '%s'", msg.Operation)
		}
		if len(msg.ResourcesRemoved) != 1 {
			t.Errorf("Expected 1 removed resource, got %d", len(msg.ResourcesRemoved))
		}
		if msg.ResourcesRemoved[0] != "test-resource" {
			t.Errorf("Expected 'test-resource' to be removed, got '%s'", msg.ResourcesRemoved[0])
		}
	case err := <-errCh:
		if err != nil {
			t.Errorf("Remove failed: %v", err)
		}
		t.Error("Expected notification but got none")
	}
}

func TestAddressStore_Remove_NonExistent(t *testing.T) {
	store := NewAddressStore()
	ctx := context.Background()

	// Try to remove non-existent resource (should just log warning)
	err := store.Remove(ctx, []string{"non-existent"})
	if err != nil {
		t.Errorf("Expected no error for removing non-existent resource, got: %v", err)
	}
}

func TestAddressStore_Notify(t *testing.T) {
	store := NewAddressStore()

	if store.wantNotify {
		t.Error("Expected wantNotify to be false initially")
	}

	ch := store.Notify()

	if !store.wantNotify {
		t.Error("Expected wantNotify to be true after calling Notify()")
	}

	if ch == nil {
		t.Error("Expected non-nil channel")
	}

	// Calling again should return the same channel
	ch2 := store.Notify()
	if ch != ch2 {
		t.Error("Expected same channel on subsequent calls")
	}
}

func TestGetWorkloadIPs(t *testing.T) {
	testCases := []struct {
		name      string
		addresses [][]byte
		expected  []netip.Addr
	}{
		{
			name: "IPv4 addresses",
			addresses: [][]byte{
				{192, 168, 1, 100},
				{10, 0, 0, 1},
			},
			expected: []netip.Addr{
				netip.MustParseAddr("192.168.1.100"),
				netip.MustParseAddr("10.0.0.1"),
			},
		},
		{
			name: "IPv6 addresses",
			addresses: [][]byte{
				{0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
			},
			expected: []netip.Addr{
				netip.MustParseAddr("2001:db8::1"),
			},
		},
		{
			name:      "empty addresses",
			addresses: [][]byte{},
			expected:  []netip.Addr{},
		},
		{
			name: "mixed valid and invalid",
			addresses: [][]byte{
				{192, 168, 1, 100},
				{1, 2, 3}, // Invalid length
				{10, 0, 0, 1},
			},
			expected: []netip.Addr{
				netip.MustParseAddr("192.168.1.100"),
				netip.MustParseAddr("10.0.0.1"),
			},
		},
		{
			name: "invalid addresses only",
			addresses: [][]byte{
				{1, 2, 3},       // Too short
				{1, 2, 3, 4, 5}, // Too long for IPv4, too short for IPv6
			},
			expected: []netip.Addr{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := getWorkloadIPs(tc.addresses)

			if len(result) != len(tc.expected) {
				t.Errorf("Expected %d IPs, got %d", len(tc.expected), len(result))
				return
			}

			for i, expected := range tc.expected {
				if result[i] != expected {
					t.Errorf("IP %d: expected %s, got %s", i, expected, result[i])
				}
			}
		})
	}
}

func TestAddressStore_MultipleUpdates(t *testing.T) {
	store := NewAddressStore()
	ctx := context.Background()

	// Add first resource
	workload1 := &workloadapi.Workload{
		Addresses: [][]byte{{192, 168, 1, 100}},
		Uid:       "workload-1",
	}
	addr1 := &workloadapi.Address{
		Type: &workloadapi.Address_Workload{Workload: workload1},
	}
	anyAddr1, _ := anypb.New(addr1)

	err := store.Update(ctx, []*discovery.Resource{{Name: "resource-1", Resource: anyAddr1}})
	if err != nil {
		t.Fatalf("First update failed: %v", err)
	}

	// Add second resource
	workload2 := &workloadapi.Workload{
		Addresses: [][]byte{{192, 168, 1, 101}},
		Uid:       "workload-2",
	}
	addr2 := &workloadapi.Address{
		Type: &workloadapi.Address_Workload{Workload: workload2},
	}
	anyAddr2, _ := anypb.New(addr2)

	err = store.Update(ctx, []*discovery.Resource{{Name: "resource-2", Resource: anyAddr2}})
	if err != nil {
		t.Fatalf("Second update failed: %v", err)
	}

	// Verify both exist
	_, ok1 := store.content.Load("resource-1")
	_, ok2 := store.content.Load("resource-2")

	if !ok1 || !ok2 {
		t.Error("Expected both resources to exist")
	}
}

func TestAddressStore_UpdateOverwrite(t *testing.T) {
	store := NewAddressStore()
	ctx := context.Background()

	// Add initial resource
	workload1 := &workloadapi.Workload{
		Addresses: [][]byte{{192, 168, 1, 100}},
		Uid:       "workload-1",
	}
	addr1 := &workloadapi.Address{
		Type: &workloadapi.Address_Workload{Workload: workload1},
	}
	anyAddr1, _ := anypb.New(addr1)

	store.Update(ctx, []*discovery.Resource{{Name: "resource-1", Resource: anyAddr1}})

	// Update with different data
	workload2 := &workloadapi.Workload{
		Addresses: [][]byte{{192, 168, 1, 200}},
		Uid:       "workload-2",
	}
	addr2 := &workloadapi.Address{
		Type: &workloadapi.Address_Workload{Workload: workload2},
	}
	anyAddr2, _ := anypb.New(addr2)

	store.Update(ctx, []*discovery.Resource{{Name: "resource-1", Resource: anyAddr2}})

	// Verify it was overwritten
	val, ok := store.content.Load("resource-1")
	if !ok {
		t.Fatal("Expected resource to exist")
	}

	addr := val.(*workloadapi.Address)
	workload := addr.GetWorkload()
	if workload.Uid != "workload-2" {
		t.Errorf("Expected workload UID 'workload-2', got '%s'", workload.Uid)
	}
}
