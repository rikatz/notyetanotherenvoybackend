package store

import (
	"context"
	"fmt"
	"log/slog"
	"net/netip"
	"sync"

	"github.com/davecgh/go-spew/spew"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	crossplane "github.com/nginxinc/nginx-go-crossplane"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"istio.io/istio/pkg/workloadapi"

	"github.com/rikatz/notyetanotherenvoybackend/nginxbackend/pkg/nginx"
)

/*
Address is the store for the endpoints.

This store will do two actions:
- Store the array of endpoints on a map[string][]Endpoint, being the endpoint a composite
of address:port (should be evolved later). The key is the workload name.
- Store each workload name as a map[string] containing the services name this workload serves
- Reconfigure the files on endpoints to have a file called service_name.conf containing
all of the endpoints of this given service
- In case of a workload removal, we fetch the services that this workload serves and remove
from the map of service/endpoint.

On a service update, the targetPort is the pod port, while servicePort must be the
same port from a service.

This way, for now we care just about the targetPort. We also just support one IP per
pod for now

The app protocol is part of a Service resource (not covered yet)
BackendTLSPolicy is part of the Route/AGW resource and should be covered as part of the route
*/

// Service is a definition of a service and all of the underlying workloads it have
// Service resources from xDS are the source of truth for service-level configuration.
// Workloads may only add themselves to existing services or trigger creation with defaults.
type Service struct {
	// Port is the service port (frontend)
	Port uint32
	// TargetPort is the backend port workloads listen on
	TargetPort uint32
	// AppProtocol defines the application protocol (HTTP, HTTP2, GRPC, etc.)
	AppProtocol workloadapi.AppProtocol
	// LoadBalancing defines the load balancing policy (optional, may be nil)
	LoadBalancing *workloadapi.LoadBalancing
	// IPFamilies defines the IP families this service supports
	IPFamilies workloadapi.IPFamilies
	// Workloads contains the set of workload UIDs serving this service
	Workloads map[string]struct{}
}

// ServiceMap contains a service configuration
// Its key will be ns/name/port
type ServiceMap map[string]Service

// Workload contains a workload definition (basically its address) and what services
// it serves.
// In case a service the workload is removed, it should be removed also from ServiceMap
// In case it is updated, the ServiceMap that contains it should also be updated accordingly.
type Workload struct {
	Address  netip.Addr
	Services map[string]struct{}
}

// WorkloadMap contains a map of all the workloads we care. Only workloads that have
// a valid Address and at least 1 service will be saved.
// Its key is the workload uid.
type WorkloadMap map[string]Workload

// ServiceAvailableCallback is called when a service becomes available (has workloads)
// This allows ResourceStore to regenerate route files that reference the service
type ServiceAvailableCallback func(serviceKey string)

type AddressStore struct {
	lock sync.Mutex
	// this map is used to store all the endpoints that a service contains. It will
	// be used to manage when a service still has endpoints or not.
	// The key is a composition of a service resource with namespace/name/port
	serviceMap ServiceMap
	// this map contains all the workload addresses. It will save just workloads
	// that contain at least one service reference
	workloadMap WorkloadMap
	// nginxMgr handles NGINX configuration and process management
	nginxMgr *nginx.Manager
	// activeServices tracks which services are currently used by routes and have upstream files
	// When these services are updated, we need to regenerate their config files
	activeServices map[string]struct{}
	// serviceAvailableCallback is called when a service becomes newly available
	serviceAvailableCallback ServiceAvailableCallback
}

// NewAddressStore creates a new AddressStore with the given nginx manager
func NewAddressStore(nginxMgr *nginx.Manager) (*AddressStore, error) {
	return &AddressStore{
		serviceMap:     make(ServiceMap),
		workloadMap:    make(WorkloadMap),
		nginxMgr:       nginxMgr,
		activeServices: make(map[string]struct{}),
	}, nil
}

// SetServiceAvailableCallback sets the callback to be called when a service becomes available
func (a *AddressStore) SetServiceAvailableCallback(callback ServiceAvailableCallback) {
	a.lock.Lock()
	defer a.lock.Unlock()
	a.serviceAvailableCallback = callback
}

func (a *AddressStore) Update(ctx context.Context, resources []*discovery.Resource) error {
	a.lock.Lock()

	// Track which services need config updates
	affectedServices := make(map[string]struct{})
	// Track services that became newly available (for callback notification)
	newlyAvailableServices := make(map[string]struct{})

	for _, res := range resources {
		log := slog.With("name", res.GetName())
		log.Info("updating resource")

		addr := &workloadapi.Address{}
		if err := anypb.UnmarshalTo(res.Resource, addr, proto.UnmarshalOptions{}); err != nil {
			log.Error("failed to unmarshal address", "error", err)
			continue
		}

		switch t := addr.Type.(type) {
		case *workloadapi.Address_Workload:
			id := t.Workload.Uid
			log = log.With("id", id)

			if !isPod(id) {
				log.Warn("received unknown workload type, skipping")
				continue
			}

			// Check if workload is unhealthy - treat as removal
			if t.Workload.Status != workloadapi.WorkloadStatus_HEALTHY {
				log.Info("workload is unhealthy, removing from configuration")
				// Get services before removing workload
				if existingWorkload, exists := a.workloadMap[id]; exists {
					for svc := range existingWorkload.Services {
						affectedServices[svc] = struct{}{}
					}
				}
				a.removeWorkloadFromServices(id)
				delete(a.workloadMap, id)
				continue
			}

			// Get previous services to track changes
			var previousServices map[string]struct{}
			if existingWorkload, exists := a.workloadMap[id]; exists {
				previousServices = existingWorkload.Services
			}

			updated, msg, err := a.updateWorkload(t.Workload)
			if err != nil {
				log.Error("error updating workload", "error", err)
				continue
			}
			if !updated {
				log.Warn("the workload could not be added, skipping", "reason", msg)
				continue
			}

			// Track affected services (both old and new)
			for svc := range previousServices {
				affectedServices[svc] = struct{}{}
			}
			if newWorkload, exists := a.workloadMap[id]; exists {
				for svc := range newWorkload.Services {
					affectedServices[svc] = struct{}{}

					// Check if this service became newly available (first workload added)
					if svcData, svcExists := a.serviceMap[svc]; svcExists {
						// Service just got its first workload
						if len(svcData.Workloads) == 1 {
							if _, isActive := a.activeServices[svc]; isActive {
								newlyAvailableServices[svc] = struct{}{}
								log.Info("service became newly available after workload addition", "service", svc)
							}
						}
					}
				}
			}

		case *workloadapi.Address_Service:
			// Service resources are the source of truth for service configuration
			svc := t.Service
			log = log.With("namespace", svc.Namespace, "name", svc.Name)
			log.Info("processing service resource")

			// Update service definitions for each port
			for _, port := range svc.Ports {
				serviceKey := fmt.Sprintf("%s/%s/%d", svc.Namespace, svc.Name, port.ServicePort)
				log := log.With("serviceKey", serviceKey, "port", port.ServicePort)

				existing, exists := a.serviceMap[serviceKey]
				hadWorkloadsBefore := exists && len(existing.Workloads) > 0

				// Create or update service entry with service-level metadata
				serviceDef := Service{
					Port:          port.ServicePort,
					TargetPort:    port.TargetPort,
					AppProtocol:   port.AppProtocol,
					LoadBalancing: svc.LoadBalancing,
					IPFamilies:    svc.IpFamilies,
					Workloads:     make(map[string]struct{}),
				}

				// Preserve existing workloads if service already exists
				if exists {
					serviceDef.Workloads = existing.Workloads
				}

				a.serviceMap[serviceKey] = serviceDef
				affectedServices[serviceKey] = struct{}{}

				// Check if this service became newly available (transitioned to having workloads)
				hasWorkloadsNow := len(serviceDef.Workloads) > 0
				if !hadWorkloadsBefore && hasWorkloadsNow {
					// Service is now available - check if routes are waiting for it
					if _, isActive := a.activeServices[serviceKey]; isActive {
						newlyAvailableServices[serviceKey] = struct{}{}
						log.Info("service became newly available for waiting routes")
					}
				}

				log.Info("service definition updated")
			}
		}
	}

	// For services that are actively used by routes, regenerate their upstream files
	// This ensures that scaling deployments triggers config updates
	needsReload := false
	for serviceKey := range affectedServices {
		// Only update config if this service is active (used by routes)
		if _, isActive := a.activeServices[serviceKey]; isActive {
			slog.Info("updating active service upstream file", "service", serviceKey)
			if err := a.updateServiceConfig(serviceKey); err != nil {
				slog.Error("failed to update active service config", "service", serviceKey, "error", err)
				continue
			}
			needsReload = true
		}
	}

	// Reload NGINX if we updated any active service configurations
	if needsReload {
		if err := a.nginxMgr.Reload(); err != nil {
			slog.Error("failed to reload nginx after service updates", "error", err)
			return err
		}
	}

	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		fmt.Printf("service map: %s\n", spew.Sdump(a.serviceMap))
		fmt.Printf("workload map: %s\n", spew.Sdump(a.workloadMap))
	}

	// Capture callback before releasing lock
	callback := a.serviceAvailableCallback
	a.lock.Unlock()

	// Notify about newly available services (done after releasing lock to avoid deadlock)
	if callback != nil {
		for serviceKey := range newlyAvailableServices {
			slog.Info("notifying about newly available service", "service", serviceKey)
			callback(serviceKey)
		}
	}

	return nil
}

func (a *AddressStore) Remove(ctx context.Context, resources []string) error {
	a.lock.Lock()
	defer a.lock.Unlock()

	// Track which services need config updates or deletion
	affectedServices := make(map[string]struct{})

	for _, res := range resources {
		log := slog.With("name", res)
		// Because Pods contains the services they belong to, we don't care about a
		// service removal. A service removal kicks an update on all workloads to
		// remove service from them so we fix it on the Update() process
		if !isPod(res) {
			log.Info("skipping as it is not a Workload")
			continue
		}

		// Track which services this workload belonged to
		if workload, exists := a.workloadMap[res]; exists {
			for svc := range workload.Services {
				affectedServices[svc] = struct{}{}
			}
		}

		a.removeWorkloadFromServices(res)
		log.Info("deleting from workload map")
		delete(a.workloadMap, res)
	}

	// Update or clean up affected services
	needsReload := false
	for serviceKey := range affectedServices {
		svc, exists := a.serviceMap[serviceKey]
		if !exists || len(svc.Workloads) == 0 {
			// Service has no workloads, remove from map
			// Note: File deletion will be handled by ResourceStore when routes are removed
			delete(a.serviceMap, serviceKey)

			// If this was an active service, update its config (which will remove endpoints)
			// or delete it if no workloads remain
			if _, isActive := a.activeServices[serviceKey]; isActive {
				slog.Info("active service has no workloads, deleting upstream file", "service", serviceKey)
				if err := a.deleteServiceConfig(serviceKey); err != nil {
					slog.Error("failed to delete service config", "service", serviceKey, "error", err)
				}
				delete(a.activeServices, serviceKey)
				needsReload = true
			}
		} else {
			// Service still has workloads, update config if active
			if _, isActive := a.activeServices[serviceKey]; isActive {
				slog.Info("updating active service upstream file after removal", "service", serviceKey)
				if err := a.updateServiceConfig(serviceKey); err != nil {
					slog.Error("failed to update active service config", "service", serviceKey, "error", err)
					continue
				}
				needsReload = true
			}
		}
	}

	// Reload NGINX if we updated any active service configurations
	if needsReload {
		if err := a.nginxMgr.Reload(); err != nil {
			slog.Error("failed to reload nginx after workload removals", "error", err)
			return err
		}
	}

	return nil
}

// updateWorkload updates our workload on maps. It returns a boolean false in case
// the workload is skipped. The string is the message returned for logging reasons
func (a *AddressStore) updateWorkload(workload *workloadapi.Workload) (bool, string, error) {
	// TODO: We should actually remove endpoints that are unhealthy
	if workload.Status != workloadapi.WorkloadStatus_HEALTHY {
		return false, "endpoint is unhealthy", nil
	}

	svcs := workload.GetServices()
	addrs := getWorkloadIPs(workload.GetAddresses())

	_, ok := a.workloadMap[workload.Uid]

	// In case our workload does not exist yet, we add it only if it has services
	// associated and addresses we create it
	if !ok {
		if len(svcs) == 0 || len(addrs) == 0 {
			return false, "workload does not have services nor addresses", nil
		}
	}
	a.createWorkload(workload, addrs[0])
	return true, "", nil
}

func (a *AddressStore) createWorkload(workload *workloadapi.Workload, addr netip.Addr) {
	svcs := workload.GetServices()

	newWorkload := Workload{
		Address:  addr,
		Services: make(map[string]struct{}),
	}

	// We go through all the services from this workload, and add it to existing
	// services (or create with defaults if Service resource hasn't arrived yet)
	for svcID, v := range svcs {
		for _, port := range v.GetPorts() {
			svcName := fmt.Sprintf("%s/%d", svcID, port.GetServicePort())
			newWorkload.Services[svcName] = struct{}{}
			svc, ok := a.serviceMap[svcName]
			if !ok {
				// Initialize the service map if the entry does not exist.
				// Service resource is the source of truth and will update this later.
				// We only set minimal defaults here to allow workloads to be tracked.
				a.serviceMap[svcName] = Service{
					Workloads: map[string]struct{}{
						workload.Uid: {},
					},
					TargetPort:  port.GetTargetPort(),
					Port:        port.GetServicePort(),
					AppProtocol: workloadapi.AppProtocol_UNKNOWN,
					IPFamilies:  workloadapi.IPFamilies_AUTOMATIC,
				}
			} else {
				// Service already exists (likely from Service resource).
				// Only update the workload list, preserving service-level metadata.
				svcMapWorkload := svc.Workloads
				if svcMapWorkload == nil {
					svcMapWorkload = make(map[string]struct{})
				}
				svcMapWorkload[workload.Uid] = struct{}{}
				svc.Workloads = svcMapWorkload
				a.serviceMap[svcName] = svc
			}
		}
	}
	a.workloadMap[workload.Uid] = newWorkload
}

func (a *AddressStore) removeWorkloadFromServices(resource string) {
	workload, ok := a.workloadMap[resource]
	// workload does not exist on our map anymore, skipping
	if !ok {
		return
	}
	for svc := range workload.Services {
		s, ok := a.serviceMap[svc]
		if !ok {
			continue
		}
		slog.Info("deleting from service map", "name", resource, "service", svc)
		delete(s.Workloads, resource)
	}
}

// updateServiceConfig generates and writes the NGINX upstream configuration for a service using crossplane
func (a *AddressStore) updateServiceConfig(serviceKey string) error {
	svc, ok := a.serviceMap[serviceKey]
	if !ok {
		return fmt.Errorf("service %s not found in service map", serviceKey)
	}

	log := slog.With("service", serviceKey, "workloads", len(svc.Workloads))
	log.Info("updating service configuration")

	// Generate upstream configuration using crossplane
	upstreamName := nginx.ServiceKeyToUpstreamName(serviceKey)

	// Build server directives
	var serverDirectives crossplane.Directives
	for workloadUID := range svc.Workloads {
		workload, ok := a.workloadMap[workloadUID]
		if !ok {
			log.Warn("workload not found in workload map", "workload", workloadUID)
			continue
		}
		// Format: server IP:targetPort;
		serverDirectives = append(serverDirectives, &crossplane.Directive{
			Directive: "server",
			Args:      []string{fmt.Sprintf("%s:%d", workload.Address.String(), svc.TargetPort)},
		})
	}

	if len(serverDirectives) == 0 {
		log.Warn("no valid servers found for upstream, skipping config generation")
		return nil
	}

	// Build comment directives for metadata
	var commentDirectives crossplane.Directives

	// Add AppProtocol as comment if set
	if svc.AppProtocol != workloadapi.AppProtocol_UNKNOWN {
		commentText := fmt.Sprintf("AppProtocol: %s", svc.AppProtocol.String())
		commentDirectives = append(commentDirectives, &crossplane.Directive{
			Directive: "#",
			Args:      []string{commentText},
			Comment:   &commentText,
		})
	}

	// Add LoadBalancing as comment if set
	if svc.LoadBalancing != nil {
		lbInfo := fmt.Sprintf("LoadBalancing: mode=%s, health_policy=%s",
			svc.LoadBalancing.Mode.String(),
			svc.LoadBalancing.HealthPolicy.String())
		if len(svc.LoadBalancing.RoutingPreference) > 0 {
			prefs := make([]string, len(svc.LoadBalancing.RoutingPreference))
			for i, pref := range svc.LoadBalancing.RoutingPreference {
				prefs[i] = pref.String()
			}
			lbInfo += fmt.Sprintf(", routing_preference=[%s]", fmt.Sprintf("%v", prefs))
		}
		commentDirectives = append(commentDirectives, &crossplane.Directive{
			Directive: "#",
			Args:      []string{lbInfo},
			Comment:   &lbInfo,
		})
	}

	// Add IPFamilies as comment if not automatic
	if svc.IPFamilies != workloadapi.IPFamilies_AUTOMATIC {
		ipFamText := fmt.Sprintf("IPFamilies: %s", svc.IPFamilies.String())
		commentDirectives = append(commentDirectives, &crossplane.Directive{
			Directive: "#",
			Args:      []string{ipFamText},
			Comment:   &ipFamText,
		})
	}

	// Combine comments and server directives
	allDirectives := append(commentDirectives, serverDirectives...)

	// Build the upstream block
	upstreamBlock := &crossplane.Directive{
		Directive: "upstream",
		Args:      []string{upstreamName},
		Block:     allDirectives,
	}

	config := &crossplane.Config{
		Parsed: crossplane.Directives{upstreamBlock},
	}

	// Write configuration using nginx manager (in endpoints subdirectory)
	filename := "endpoints/" + nginx.ServiceKeyToFilename(serviceKey)
	if err := a.nginxMgr.WriteConfig(filename, config); err != nil {
		log.Error("failed to write config", "error", err)
		return err
	}

	log.Info("service configuration updated", "file", filename)
	return nil
}

// deleteServiceConfig removes the NGINX configuration file for a service
func (a *AddressStore) deleteServiceConfig(serviceKey string) error {
	filename := "endpoints/" + nginx.ServiceKeyToFilename(serviceKey)
	return a.nginxMgr.DeleteConfig(filename)
}

// EnsureUpstreamFile writes the upstream configuration file for a service if it exists in memory
// This is called by ResourceStore when a route references a backend
// Returns true if the service exists and file was written, false if service doesn't exist
func (a *AddressStore) EnsureUpstreamFile(serviceKey string) (bool, error) {
	a.lock.Lock()
	defer a.lock.Unlock()

	// Always mark service as active (used by routes), even if it doesn't exist yet
	// This ensures that when the service arrives later, it will trigger config updates
	a.activeServices[serviceKey] = struct{}{}

	svc, ok := a.serviceMap[serviceKey]
	if !ok {
		slog.Info("service not yet available, marked as pending", "service", serviceKey)
		return false, nil
	}

	if len(svc.Workloads) == 0 {
		// Service has no workloads, don't write file yet but keep it marked as active
		slog.Info("service has no workloads yet, marked as pending", "service", serviceKey)
		return false, nil
	}

	// Service exists with workloads, write the config file
	if err := a.updateServiceConfig(serviceKey); err != nil {
		return true, fmt.Errorf("failed to write upstream config: %w", err)
	}

	return true, nil
}

// ServiceExists checks if a service exists in memory (regardless of whether file is written)
func (a *AddressStore) ServiceExists(serviceKey string) bool {
	a.lock.Lock()
	defer a.lock.Unlock()

	svc, ok := a.serviceMap[serviceKey]
	return ok && len(svc.Workloads) > 0
}

// DeleteUpstreamFile removes the upstream configuration file for a service
// This is called by ResourceStore when no routes reference a backend anymore
func (a *AddressStore) DeleteUpstreamFile(serviceKey string) error {
	a.lock.Lock()
	defer a.lock.Unlock()

	// Mark service as inactive (no longer used by routes)
	delete(a.activeServices, serviceKey)

	return a.deleteServiceConfig(serviceKey)
}
