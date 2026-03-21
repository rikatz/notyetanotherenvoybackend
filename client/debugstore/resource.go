package debugstore

import (
	"context"
	"log/slog"
	"sync"

	resourceapi "github.com/agentgateway/agentgateway/api"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type ResourceStore struct {
	content    sync.Map
	wantNotify bool
	notifyCh   chan NotifyMsg
}

func NewResourceStore() *ResourceStore {
	return &ResourceStore{
		notifyCh: make(chan NotifyMsg),
	}
}

func (a *ResourceStore) Update(ctx context.Context, resources []*discovery.Resource) error {
	arr := make([]proto.Message, 0)
	for _, res := range resources {
		agres := &resourceapi.Resource{}
		log := slog.With("name", res.Name)

		if err := anypb.UnmarshalTo(res.Resource, agres, proto.UnmarshalOptions{}); err != nil {
			log.Error("failed to unmarshal Address", "error", err)
			continue
		}
		debug(ctx, agres)
		a.content.Store(res.Name, agres)
		if a.wantNotify {
			arr = append(arr, agres)
		}

		log.Info("stored resource")
	}
	if a.wantNotify {
		a.notifyCh <- NotifyMsg{
			Operation: "UPDATE",
			Resources: arr,
		}
	}

	return nil
}

func (a *ResourceStore) Remove(ctx context.Context, resources []string) error {
	for _, res := range resources {
		_, ok := a.content.LoadAndDelete(res)
		if !ok {
			slog.Warn("failed to remove resource", "name", res)
			continue
		}
		slog.Info("removed resource", "name", res)
	}

	if a.wantNotify {
		a.notifyCh <- NotifyMsg{
			Operation:        "REMOVE",
			ResourcesRemoved: resources,
		}
	}

	return nil
}

// This is not concurrent safe! If you get 2 routines getting the same
// channel, just one will get the message! This is used for debug only!
func (a *ResourceStore) Notify() <-chan NotifyMsg {
	a.wantNotify = true
	return a.notifyCh
}
