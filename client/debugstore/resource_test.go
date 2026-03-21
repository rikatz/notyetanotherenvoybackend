package debugstore

import (
	"context"
	"testing"

	resourceapi "github.com/agentgateway/agentgateway/api"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestNewResourceStore(t *testing.T) {
	store := NewResourceStore()

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

func TestResourceStore_Update(t *testing.T) {
	store := NewResourceStore()
	ctx := context.Background()

	// Create a resource
	resource := &resourceapi.Resource{}

	anyRes, err := anypb.New(resource)
	if err != nil {
		t.Fatalf("Failed to create Any: %v", err)
	}

	resources := []*discovery.Resource{
		{
			Name:     "test-resource",
			Resource: anyRes,
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

	// Verify it's the right type
	_, ok = val.(*resourceapi.Resource)
	if !ok {
		t.Error("Expected stored value to be *resourceapi.Resource")
	}
}

func TestResourceStore_Update_WithNotification(t *testing.T) {
	store := NewResourceStore()
	ctx := context.Background()

	// Enable notifications
	notifyCh := store.Notify()

	resource := &resourceapi.Resource{}

	anyRes, err := anypb.New(resource)
	if err != nil {
		t.Fatalf("Failed to create Any: %v", err)
	}

	resources := []*discovery.Resource{
		{
			Name:     "test-resource",
			Resource: anyRes,
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

func TestResourceStore_Update_MultipleResources(t *testing.T) {
	store := NewResourceStore()
	ctx := context.Background()

	// Enable notifications to verify count
	notifyCh := store.Notify()

	// Create multiple resources
	resources := make([]*discovery.Resource, 3)
	for i := 0; i < 3; i++ {
		res := &resourceapi.Resource{}
		anyRes, _ := anypb.New(res)
		resources[i] = &discovery.Resource{
			Name:     "test-resource-" + string(rune('1'+i)),
			Resource: anyRes,
		}
	}

	// Run update in goroutine
	go func() {
		store.Update(ctx, resources)
	}()

	// Wait for notification
	msg := <-notifyCh
	if len(msg.Resources) != 3 {
		t.Errorf("Expected 3 resources in notification, got %d", len(msg.Resources))
	}
}

func TestResourceStore_Update_InvalidResource(t *testing.T) {
	store := NewResourceStore()
	ctx := context.Background()

	// Create invalid Any
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

func TestResourceStore_Remove(t *testing.T) {
	store := NewResourceStore()
	ctx := context.Background()

	// Add a resource first
	resource := &resourceapi.Resource{}
	store.content.Store("test-resource", resource)

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

func TestResourceStore_Remove_WithNotification(t *testing.T) {
	store := NewResourceStore()
	ctx := context.Background()

	// Enable notifications
	notifyCh := store.Notify()

	// Add and then remove
	store.content.Store("test-resource", &resourceapi.Resource{})

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

func TestResourceStore_Remove_MultipleResources(t *testing.T) {
	store := NewResourceStore()
	ctx := context.Background()

	// Add multiple resources
	for i := 1; i <= 3; i++ {
		name := "test-resource-" + string(rune('0'+i))
		store.content.Store(name, &resourceapi.Resource{})
	}

	// Remove them all
	toRemove := []string{"test-resource-1", "test-resource-2", "test-resource-3"}
	err := store.Remove(ctx, toRemove)
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	// Verify all are gone
	for _, name := range toRemove {
		if _, ok := store.content.Load(name); ok {
			t.Errorf("Expected resource '%s' to be removed", name)
		}
	}
}

func TestResourceStore_Remove_NonExistent(t *testing.T) {
	store := NewResourceStore()
	ctx := context.Background()

	// Try to remove non-existent resource (should just log warning)
	err := store.Remove(ctx, []string{"non-existent"})
	if err != nil {
		t.Errorf("Expected no error for removing non-existent resource, got: %v", err)
	}
}

func TestResourceStore_Remove_PartialSuccess(t *testing.T) {
	store := NewResourceStore()
	ctx := context.Background()

	// Add only one resource
	store.content.Store("exists", &resourceapi.Resource{})

	// Try to remove both existing and non-existing
	err := store.Remove(ctx, []string{"exists", "does-not-exist"})
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	// Verify existing one was removed
	if _, ok := store.content.Load("exists"); ok {
		t.Error("Expected 'exists' to be removed")
	}
}

func TestResourceStore_Notify(t *testing.T) {
	store := NewResourceStore()

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

func TestResourceStore_UpdateOverwrite(t *testing.T) {
	store := NewResourceStore()
	ctx := context.Background()

	// Add initial resource
	resource1 := &resourceapi.Resource{}
	anyRes1, _ := anypb.New(resource1)
	store.Update(ctx, []*discovery.Resource{{Name: "resource-1", Resource: anyRes1}})

	// Verify first resource exists
	val1, ok := store.content.Load("resource-1")
	if !ok {
		t.Fatal("Expected first resource to exist")
	}

	// Update with different data
	resource2 := &resourceapi.Resource{}
	anyRes2, _ := anypb.New(resource2)
	store.Update(ctx, []*discovery.Resource{{Name: "resource-1", Resource: anyRes2}})

	// Verify it was overwritten (value should be different)
	val2, ok := store.content.Load("resource-1")
	if !ok {
		t.Fatal("Expected resource to exist after update")
	}

	// The values should be different objects (overwritten)
	if val1 == val2 {
		t.Error("Expected resource to be overwritten with new instance")
	}
}

func TestResourceStore_ConcurrentAccess(t *testing.T) {
	store := NewResourceStore()
	ctx := context.Background()

	// Test concurrent updates and reads
	done := make(chan bool, 10)

	// Start multiple writers
	for i := 0; i < 5; i++ {
		go func(id int) {
			resource := &resourceapi.Resource{}
			anyRes, _ := anypb.New(resource)
			resources := []*discovery.Resource{{Name: "resource", Resource: anyRes}}
			store.Update(ctx, resources)
			done <- true
		}(i)
	}

	// Start multiple readers
	for i := 0; i < 5; i++ {
		go func() {
			store.content.Load("resource")
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Should not panic or race
}
