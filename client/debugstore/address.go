package debugstore

import (
	"context"
	"log/slog"
	"net/netip"
	"sync"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"istio.io/istio/pkg/workloadapi"
)

type AddressStore struct {
	content    sync.Map
	wantNotify bool
	notifyCh   chan NotifyMsg
}

// AddressStore will also try to convert our IPs bytes into real IP address
func NewAddressStore() *AddressStore {
	return &AddressStore{
		notifyCh: make(chan NotifyMsg),
	}
}

func (a *AddressStore) Update(ctx context.Context, resources []*discovery.Resource) error {
	arr := make([]proto.Message, 0)

	for _, res := range resources {
		addr := &workloadapi.Address{}
		log := slog.With("name", res.Name)

		if err := anypb.UnmarshalTo(res.Resource, addr, proto.UnmarshalOptions{}); err != nil {
			log.Error("failed to unmarshal Address", "error", err)
			continue
		}

		if wload, ok := addr.Type.(*workloadapi.Address_Workload); ok {
			addresses := getWorkloadIPs(wload.Workload.GetAddresses())
			wload.Workload.GetServices()
			log.Info("addresses found", "addr", addresses)
		}

		debug(ctx, addr)
		a.content.Store(res.Name, addr)
		if a.wantNotify {
			arr = append(arr, addr)
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

func (a *AddressStore) Remove(ctx context.Context, resources []string) error {
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
func (a *AddressStore) Notify() <-chan NotifyMsg {
	a.wantNotify = true
	return a.notifyCh
}

func getWorkloadIPs(addresses [][]byte) []netip.Addr {
	var ips []netip.Addr
	for _, ipBytes := range addresses {
		if ip, ok := netip.AddrFromSlice(ipBytes); ok {
			ips = append(ips, ip)
		}
	}
	return ips
}
