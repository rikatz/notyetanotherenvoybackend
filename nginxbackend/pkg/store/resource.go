package store

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	resourceapi "github.com/agentgateway/agentgateway/api"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	crossplane "github.com/nginxinc/nginx-go-crossplane"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/rikatz/notyetanotherenvoybackend/nginxbackend/pkg/nginx"
)

// ServerFileConfig tracks the metadata for a server block file
type ServerFileConfig struct {
	ListenerKey string
	Hostname    string
	Port        uint32
	Protocol    resourceapi.Protocol
	TLSConfig   *resourceapi.TLSConfig
	RouteKeys   []string // Routes that use this listener+hostname
}

// ResourceStore handles Gateway API resources (Bind, Listener, Route) and generates
// NGINX server blocks and routing configurations
type ResourceStore struct {
	lock sync.Mutex

	// Resource storage
	binds     map[string]*resourceapi.Bind     // key: bind.Key
	listeners map[string]*resourceapi.Listener // key: listener.Key
	routes    map[string]*resourceapi.Route    // key: route.Key

	// Reverse lookups for dependency tracking
	bindToListeners  map[string][]string // bind_key -> []listener_key
	listenerToRoutes map[string][]string // listener_key -> []route_key

	// Service to route mapping for tracking which routes use which services
	serviceToRoutes map[string][]string // serviceKey -> []route_key

	// Server file metadata
	serverFiles map[string]*ServerFileConfig // "listenerKey:hostname" -> config

	// nginxMgr handles NGINX configuration and process management
	nginxMgr *nginx.Manager

	// addressStore provides upstream file coordination
	addressStore *AddressStore
}

// NewResourceStore creates a new ResourceStore with the given nginx manager and address store
func NewResourceStore(nginxMgr *nginx.Manager, addressStore *AddressStore) (*ResourceStore, error) {
	rs := &ResourceStore{
		binds:            make(map[string]*resourceapi.Bind),
		listeners:        make(map[string]*resourceapi.Listener),
		routes:           make(map[string]*resourceapi.Route),
		bindToListeners:  make(map[string][]string),
		listenerToRoutes: make(map[string][]string),
		serviceToRoutes:  make(map[string][]string),
		serverFiles:      make(map[string]*ServerFileConfig),
		nginxMgr:         nginxMgr,
		addressStore:     addressStore,
	}

	// Register callback for when services become available
	addressStore.SetServiceAvailableCallback(rs.onServiceAvailable)

	return rs, nil
}

func (r *ResourceStore) Update(ctx context.Context, resources []*discovery.Resource) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	// Track what needs regeneration
	affectedServerFiles := make(map[string]struct{})
	affectedRouteFiles := make(map[string]struct{})

	// Phase 1: Unmarshal and categorize all resources
	var binds []*resourceapi.Bind
	var listeners []*resourceapi.Listener
	var routes []*resourceapi.Route

	for _, res := range resources {
		agres := &resourceapi.Resource{}
		log := slog.With("name", res.Name)
		if err := anypb.UnmarshalTo(res.Resource, agres, proto.UnmarshalOptions{}); err != nil {
			log.Error("failed to unmarshal resource", "error", err)
			continue
		}

		switch kind := agres.Kind.(type) {
		case *resourceapi.Resource_Bind:
			binds = append(binds, kind.Bind)
		case *resourceapi.Resource_Listener:
			listeners = append(listeners, kind.Listener)
		case *resourceapi.Resource_Route:
			routes = append(routes, kind.Route)
			// Ignore Workload, Service (handled by AddressStore)
			// Ignore Backend, Policy, TcpRoute (not in scope for this implementation)
		}
	}

	// Phase 2: Process Binds (foundation layer)
	for _, bind := range binds {
		r.updateBind(bind, affectedServerFiles)
	}

	// Phase 3: Process Listeners (middle layer)
	for _, listener := range listeners {
		r.updateListener(listener, affectedServerFiles)
	}

	// Phase 4: Process Routes (top layer)
	for _, route := range routes {
		r.updateRoute(route, affectedServerFiles, affectedRouteFiles)
	}

	// Phase 5: Regenerate affected configurations
	for serverFileKey := range affectedServerFiles {
		if err := r.generateServerFile(serverFileKey); err != nil {
			slog.Error("failed to generate server file", "key", serverFileKey, "error", err)
		}
	}

	for routeKey := range affectedRouteFiles {
		if err := r.generateRouteFile(routeKey); err != nil {
			slog.Error("failed to generate route file", "key", routeKey, "error", err)
		}
	}

	// Phase 6: Reload NGINX if any changes were made
	if len(affectedServerFiles) > 0 || len(affectedRouteFiles) > 0 {
		if err := r.nginxMgr.Reload(); err != nil {
			slog.Error("failed to reload nginx", "error", err)
		}
	}

	return nil
}

func (r *ResourceStore) Remove(ctx context.Context, resourceNames []string) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	affectedServerFiles := make(map[string]struct{})
	deletedRoutes := make(map[string]struct{})

	for _, name := range resourceNames {
		log := slog.With("resource", name)

		// Try each resource type
		if bind, ok := r.binds[name]; ok {
			r.removeBind(bind, affectedServerFiles)
		} else if listener, ok := r.listeners[name]; ok {
			r.removeListener(listener, affectedServerFiles, deletedRoutes)
		} else if route, ok := r.routes[name]; ok {
			r.removeRoute(route, affectedServerFiles, deletedRoutes)
		} else {
			log.Warn("resource not found in any store")
		}
	}

	// Regenerate affected server files
	for serverFileKey := range affectedServerFiles {
		// Check if server still has routes
		if serverConfig, ok := r.serverFiles[serverFileKey]; ok && len(serverConfig.RouteKeys) > 0 {
			if err := r.generateServerFile(serverFileKey); err != nil {
				slog.Error("failed to regenerate server file", "key", serverFileKey, "error", err)
			}
		} else {
			// No routes left, delete the server file
			r.deleteServerFile(serverFileKey)
			r.deleteTLSFiles(serverFileKey)
			delete(r.serverFiles, serverFileKey)
		}
	}

	// Delete route files for removed routes
	for routeKey := range deletedRoutes {
		r.deleteRouteFile(routeKey)
	}

	// Reload NGINX if any changes were made
	if len(affectedServerFiles) > 0 || len(deletedRoutes) > 0 {
		if err := r.nginxMgr.Reload(); err != nil {
			slog.Error("failed to reload nginx", "error", err)
		}
	}

	return nil
}

// updateBind stores a bind resource and marks affected listeners
func (r *ResourceStore) updateBind(bind *resourceapi.Bind, affectedServers map[string]struct{}) {
	log := slog.With("bind_key", bind.Key)

	// Store the bind
	r.binds[bind.Key] = bind
	log.Info("updated bind", "port", bind.Port, "protocol", bind.Protocol)

	// Mark all listeners using this bind as affected
	if listenerKeys, ok := r.bindToListeners[bind.Key]; ok {
		for _, listenerKey := range listenerKeys {
			if listener, exists := r.listeners[listenerKey]; exists {
				serverKey := makeServerFileKey(listenerKey, listener.Hostname)
				affectedServers[serverKey] = struct{}{}
			}
		}
	}
}

// updateListener stores a listener resource and marks affected servers
func (r *ResourceStore) updateListener(listener *resourceapi.Listener, affectedServers map[string]struct{}) {
	log := slog.With("listener_key", listener.Key)

	// Check if listener changed
	oldListener, existed := r.listeners[listener.Key]

	// Store the listener
	r.listeners[listener.Key] = listener

	// Update bind -> listener mapping
	r.addToReverseMap(r.bindToListeners, listener.BindKey, listener.Key)

	// If hostname or bind changed, mark old server file for cleanup
	if existed && (oldListener.Hostname != listener.Hostname || oldListener.BindKey != listener.BindKey) {
		oldServerKey := makeServerFileKey(listener.Key, oldListener.Hostname)
		delete(r.serverFiles, oldServerKey)
		r.deleteServerFile(oldServerKey)
	}

	// Mark new server file as affected
	serverKey := makeServerFileKey(listener.Key, listener.Hostname)
	affectedServers[serverKey] = struct{}{}

	log.Info("updated listener", "hostname", listener.Hostname, "bind_key", listener.BindKey)
}

// updateRoute stores a route resource and marks affected servers and route files
func (r *ResourceStore) updateRoute(route *resourceapi.Route, affectedServers, affectedRoutes map[string]struct{}) {
	log := slog.With("route_key", route.Key)

	// Check if route changed
	oldRoute, existed := r.routes[route.Key]

	// If route existed, clean up old service-to-route mappings
	if existed {
		for _, backend := range oldRoute.Backends {
			_, serviceKey := r.backendToUpstreamName(backend.Backend)
			if serviceKey != "" {
				r.removeFromReverseMap(r.serviceToRoutes, serviceKey, route.Key)
			}
		}
	}

	// Store the route
	r.routes[route.Key] = route

	// Update listener -> route mapping
	r.addToReverseMap(r.listenerToRoutes, route.ListenerKey, route.Key)

	// Mark route file for regeneration
	affectedRoutes[route.Key] = struct{}{}

	// Handle hostname changes
	var hostnamesChanged bool
	if existed {
		oldHostnames := make(map[string]struct{})
		for _, h := range oldRoute.Hostnames {
			oldHostnames[h] = struct{}{}
		}

		// Check if hostnames differ
		if len(oldRoute.Hostnames) != len(route.Hostnames) {
			hostnamesChanged = true
		} else {
			for _, h := range route.Hostnames {
				if _, ok := oldHostnames[h]; !ok {
					hostnamesChanged = true
					break
				}
			}
		}
	}

	// If route uses new hostnames, mark old server files for update
	if existed && hostnamesChanged {
		for _, hostname := range oldRoute.Hostnames {
			serverKey := makeServerFileKey(route.ListenerKey, hostname)
			affectedServers[serverKey] = struct{}{}
		}
	}

	// Mark new server files as affected
	listener, listenerExists := r.listeners[route.ListenerKey]
	if !listenerExists {
		log.Warn("listener not found for route, deferring server file generation")
		return
	}

	// Routes can specify multiple hostnames
	if len(route.Hostnames) > 0 {
		for _, hostname := range route.Hostnames {
			serverKey := makeServerFileKey(route.ListenerKey, hostname)
			affectedServers[serverKey] = struct{}{}
		}
	} else {
		// If route has no hostnames, use listener's hostname
		serverKey := makeServerFileKey(route.ListenerKey, listener.Hostname)
		affectedServers[serverKey] = struct{}{}
	}

	log.Info("updated route", "listener_key", route.ListenerKey, "hostnames", route.Hostnames)
}

// removeBind removes a bind and marks affected listeners
func (r *ResourceStore) removeBind(bind *resourceapi.Bind, affectedServers map[string]struct{}) {
	// Mark all listeners using this bind as affected
	if listenerKeys, ok := r.bindToListeners[bind.Key]; ok {
		for _, listenerKey := range listenerKeys {
			if listener, exists := r.listeners[listenerKey]; exists {
				serverKey := makeServerFileKey(listenerKey, listener.Hostname)
				affectedServers[serverKey] = struct{}{}
			}
		}
	}

	delete(r.binds, bind.Key)
	delete(r.bindToListeners, bind.Key)
	slog.Info("removed bind", "key", bind.Key)
}

// removeListener removes a listener and all its routes
func (r *ResourceStore) removeListener(listener *resourceapi.Listener, affectedServers map[string]struct{}, deletedRoutes map[string]struct{}) {
	// Remove all routes using this listener
	if routeKeys, ok := r.listenerToRoutes[listener.Key]; ok {
		for _, routeKey := range routeKeys {
			if route, exists := r.routes[routeKey]; exists {
				for _, hostname := range route.Hostnames {
					serverKey := makeServerFileKey(listener.Key, hostname)
					affectedServers[serverKey] = struct{}{}
				}
				delete(r.routes, routeKey)
				deletedRoutes[routeKey] = struct{}{}
			}
		}
	}

	// Remove server file
	serverKey := makeServerFileKey(listener.Key, listener.Hostname)
	delete(r.serverFiles, serverKey)
	affectedServers[serverKey] = struct{}{}

	// Clean up mappings
	delete(r.listeners, listener.Key)
	delete(r.listenerToRoutes, listener.Key)
	r.removeFromReverseMap(r.bindToListeners, listener.BindKey, listener.Key)

	slog.Info("removed listener", "key", listener.Key)
}

// removeRoute removes a route and marks server files as affected
func (r *ResourceStore) removeRoute(route *resourceapi.Route, affectedServers map[string]struct{}, deletedRoutes map[string]struct{}) {
	// Mark server files as affected
	listener, exists := r.listeners[route.ListenerKey]
	if exists {
		if len(route.Hostnames) > 0 {
			for _, hostname := range route.Hostnames {
				serverKey := makeServerFileKey(route.ListenerKey, hostname)
				affectedServers[serverKey] = struct{}{}
			}
		} else {
			serverKey := makeServerFileKey(route.ListenerKey, listener.Hostname)
			affectedServers[serverKey] = struct{}{}
		}
	}

	// Clean up service-to-route mappings
	for _, backend := range route.Backends {
		_, serviceKey := r.backendToUpstreamName(backend.Backend)
		if serviceKey != "" {
			r.removeFromReverseMap(r.serviceToRoutes, serviceKey, route.Key)
		}
	}

	// Clean up
	delete(r.routes, route.Key)
	deletedRoutes[route.Key] = struct{}{}
	r.removeFromReverseMap(r.listenerToRoutes, route.ListenerKey, route.Key)

	slog.Info("removed route", "key", route.Key)
}

// generateServerFile creates an NGINX server block configuration file
func (r *ResourceStore) generateServerFile(serverFileKey string) error {
	log := slog.With("server_file", serverFileKey)

	// Parse the key
	listenerKey, hostname := parseServerFileKey(serverFileKey)

	// Get listener
	listener, ok := r.listeners[listenerKey]
	if !ok {
		log.Warn("listener not found, skipping server file generation")
		return nil
	}

	// Get bind
	bind, ok := r.binds[listener.BindKey]
	if !ok {
		log.Warn("bind not found, skipping server file generation")
		return nil
	}

	// Find all routes that match this listener+hostname
	var routeKeys []string
	if allRoutes, ok := r.listenerToRoutes[listenerKey]; ok {
		for _, routeKey := range allRoutes {
			route := r.routes[routeKey]
			if r.routeMatchesHostname(route, hostname) {
				routeKeys = append(routeKeys, routeKey)
			}
		}
	}

	// Store server file config
	r.serverFiles[serverFileKey] = &ServerFileConfig{
		ListenerKey: listenerKey,
		Hostname:    hostname,
		Port:        bind.Port,
		Protocol:    listener.Protocol,
		TLSConfig:   listener.Tls,
		RouteKeys:   routeKeys,
	}

	// Generate TLS certificate files if needed
	if listener.Tls != nil {
		if err := r.generateTLSFiles(listenerKey, listener.Tls); err != nil {
			log.Error("failed to generate TLS files", "error", err)
			return err
		}
	}

	// Build server block using crossplane
	var serverDirectives crossplane.Directives

	// Add metadata comments at the top
	if listener.Name != nil {
		comment := fmt.Sprintf("Gateway: %s/%s, Listener: %s",
			listener.Name.GatewayNamespace, listener.Name.GatewayName, listener.Name.ListenerName)
		serverDirectives = append(serverDirectives, &crossplane.Directive{
			Directive: "#",
			Args:      []string{comment},
			Comment:   &comment,
		})
	}

	// Listen directive
	listenArgs := []string{fmt.Sprintf("%d", bind.Port)}
	if listener.Protocol == resourceapi.Protocol_HTTPS || listener.Tls != nil {
		listenArgs = append(listenArgs, "ssl")
	}
	serverDirectives = append(serverDirectives, &crossplane.Directive{
		Directive: "listen",
		Args:      listenArgs,
	})

	// Server name
	if hostname != "" && hostname != "*" {
		serverDirectives = append(serverDirectives, &crossplane.Directive{
			Directive: "server_name",
			Args:      []string{hostname},
		})
	}

	// SSL certificates
	if listener.Tls != nil {
		certPath := fmt.Sprintf("certs/%s.crt", sanitizeFilename(listenerKey))
		keyPath := fmt.Sprintf("certs/%s.key", sanitizeFilename(listenerKey))

		serverDirectives = append(serverDirectives,
			&crossplane.Directive{
				Directive: "ssl_certificate",
				Args:      []string{certPath},
			},
			&crossplane.Directive{
				Directive: "ssl_certificate_key",
				Args:      []string{keyPath},
			},
		)
	}

	// Include route files
	for _, routeKey := range routeKeys {
		includeFile := fmt.Sprintf("routes/%s.conf", routeFileKey(routeKey, listenerKey))
		serverDirectives = append(serverDirectives, &crossplane.Directive{
			Directive: "include",
			Args:      []string{includeFile},
		})
	}

	// Build server block
	serverBlock := &crossplane.Directive{
		Directive: "server",
		Block:     serverDirectives,
	}

	config := &crossplane.Config{
		Parsed: crossplane.Directives{serverBlock},
	}

	// Write to file
	filename := fmt.Sprintf("servers/%s.conf", sanitizeFilename(serverFileKey))
	if err := r.nginxMgr.WriteConfig(filename, config); err != nil {
		log.Error("failed to write server config", "error", err)
		return err
	}

	log.Info("generated server file", "routes", len(routeKeys))
	return nil
}

// generateRouteFile creates NGINX location block configuration for a route
func (r *ResourceStore) generateRouteFile(routeKey string) error {
	log := slog.With("route_key", routeKey)

	route, ok := r.routes[routeKey]
	if !ok {
		log.Warn("route not found")
		return nil
	}

	var allDirectives crossplane.Directives

	// Add metadata comments at the top
	if route.Name != nil {
		comment := fmt.Sprintf("Route: %s/%s (kind: %s)",
			route.Name.Namespace, route.Name.Name, route.Name.Kind)
		allDirectives = append(allDirectives, &crossplane.Directive{
			Directive: "#",
			Args:      []string{comment},
			Comment:   &comment,
		})
	}

	// Generate a location block for each match
	for _, match := range route.Matches {
		locationDirectives := r.generateLocationDirectives(route, match)

		// Determine location pattern
		locationPattern := r.buildLocationPattern(match)

		locationBlock := &crossplane.Directive{
			Directive: "location",
			Args:      []string{locationPattern},
			Block:     locationDirectives,
		}

		allDirectives = append(allDirectives, locationBlock)
	}

	// If no matches, create default location
	if len(route.Matches) == 0 {
		locationDirectives := r.generateLocationDirectives(route, nil)
		locationBlock := &crossplane.Directive{
			Directive: "location",
			Args:      []string{"/"},
			Block:     locationDirectives,
		}
		allDirectives = append(allDirectives, locationBlock)
	}

	config := &crossplane.Config{
		Parsed: allDirectives,
	}

	// Write to file
	filename := fmt.Sprintf("routes/%s.conf", routeFileKey(routeKey, route.ListenerKey))
	if err := r.nginxMgr.WriteConfig(filename, config); err != nil {
		log.Error("failed to write route config", "error", err)
		return err
	}

	log.Info("generated route file", "matches", len(route.Matches))
	return nil
}

// generateLocationDirectives creates the directives for a location block
func (r *ResourceStore) generateLocationDirectives(route *resourceapi.Route, match *resourceapi.RouteMatch) crossplane.Directives {
	var directives crossplane.Directives

	// Add method/header/query matching using if directives
	if match != nil {
		directives = append(directives, r.generateMatchDirectives(match)...)
	}

	// Handle backends
	if len(route.Backends) == 0 {
		// No backends, return 404
		directives = append(directives, &crossplane.Directive{
			Directive: "return",
			Args:      []string{"404"},
		})
		return directives
	}

	// Ensure upstream files exist and collect valid backends
	var validBackends []struct {
		upstreamName string
		weight       int32
	}
	totalWeight := int32(0)

	for _, backend := range route.Backends {
		upstreamName, serviceKey := r.backendToUpstreamName(backend.Backend)
		if upstreamName == "" {
			continue
		}

		// Track that this route uses this service
		r.addToReverseMap(r.serviceToRoutes, serviceKey, route.Key)

		// Ensure the upstream file is written
		exists, err := r.addressStore.EnsureUpstreamFile(serviceKey)
		if err != nil {
			slog.Error("failed to ensure upstream file", "service", serviceKey, "error", err)
			continue
		}
		if !exists {
			slog.Warn("backend service not found in AddressStore (will retry when available)", "service", serviceKey, "route", route.Key)
			continue
		}

		weight := backend.Weight
		if weight <= 0 {
			weight = 1 // Default weight
		}

		validBackends = append(validBackends, struct {
			upstreamName string
			weight       int32
		}{upstreamName, weight})
		totalWeight += weight
	}

	if len(validBackends) == 0 {
		// No valid backends, return 404
		directives = append(directives, &crossplane.Directive{
			Directive: "return",
			Args:      []string{"404"},
		})
		slog.Warn("no valid backends found for route, returning 404", "route", route.Key)
		return directives
	}

	// Single backend: simple proxy_pass
	if len(validBackends) == 1 {
		directives = append(directives, &crossplane.Directive{
			Directive: "proxy_pass",
			Args:      []string{fmt.Sprintf("http://%s", validBackends[0].upstreamName)},
		})
	} else {
		// Multiple backends: use split_clients for weighted distribution
		directives = append(directives, r.generateWeightedBackendDirectives(validBackends, totalWeight)...)
	}

	// Add standard proxy headers
	directives = append(directives,
		&crossplane.Directive{
			Directive: "proxy_set_header",
			Args:      []string{"Host", "$host"},
		},
		&crossplane.Directive{
			Directive: "proxy_set_header",
			Args:      []string{"X-Real-IP", "$remote_addr"},
		},
		&crossplane.Directive{
			Directive: "proxy_set_header",
			Args:      []string{"X-Forwarded-For", "$proxy_add_x_forwarded_for"},
		},
	)

	return directives
}

// generateMatchDirectives creates if directives for method/header/query matching
func (r *ResourceStore) generateMatchDirectives(match *resourceapi.RouteMatch) crossplane.Directives {
	var directives crossplane.Directives
	conditions := []string{}

	// Method matching
	if match.Method != nil && match.Method.Exact != "" {
		condition := fmt.Sprintf("$request_method != \"%s\"", match.Method.Exact)
		conditions = append(conditions, condition)
	}

	// Header matching
	for _, header := range match.Headers {
		varName := fmt.Sprintf("$http_%s", strings.ReplaceAll(strings.ToLower(header.Name), "-", "_"))
		switch v := header.Value.(type) {
		case *resourceapi.HeaderMatch_Exact:
			condition := fmt.Sprintf("%s != \"%s\"", varName, v.Exact)
			conditions = append(conditions, condition)
		case *resourceapi.HeaderMatch_Regex:
			// NGINX doesn't support != with regex in simple if, so we check for positive match
			// and return if NOT matched
			condition := fmt.Sprintf("%s !~ \"%s\"", varName, v.Regex)
			conditions = append(conditions, condition)
		}
	}

	// Query parameter matching
	for _, query := range match.QueryParams {
		varName := fmt.Sprintf("$arg_%s", query.Name)
		switch v := query.Value.(type) {
		case *resourceapi.QueryMatch_Exact:
			condition := fmt.Sprintf("%s != \"%s\"", varName, v.Exact)
			conditions = append(conditions, condition)
		case *resourceapi.QueryMatch_Regex:
			condition := fmt.Sprintf("%s !~ \"%s\"", varName, v.Regex)
			conditions = append(conditions, condition)
		}
	}

	// Generate if directives to return 404 if conditions don't match
	for _, condition := range conditions {
		ifBlock := &crossplane.Directive{
			Directive: "if",
			Args:      []string{"(" + condition + ")"},
			Block: crossplane.Directives{
				{
					Directive: "return",
					Args:      []string{"404"},
				},
			},
		}
		directives = append(directives, ifBlock)
	}

	return directives
}

// generateWeightedBackendDirectives creates split_clients configuration for weighted backends
func (r *ResourceStore) generateWeightedBackendDirectives(backends []struct {
	upstreamName string
	weight       int32
}, totalWeight int32) crossplane.Directives {
	var directives crossplane.Directives

	// Use split_clients to distribute traffic based on weights
	var splitDirectives crossplane.Directives
	cumulative := float64(0)

	for i, backend := range backends {
		percentage := (float64(backend.weight) / float64(totalWeight)) * 100
		cumulative += percentage

		var percentArg string
		if i == len(backends)-1 {
			// Last backend gets everything else to handle rounding
			percentArg = "*"
		} else {
			percentArg = fmt.Sprintf("%.2f%%", cumulative)
		}

		splitDirectives = append(splitDirectives, &crossplane.Directive{
			Directive: percentArg,
			Args:      []string{backend.upstreamName},
		})
	}

	// split_clients directive
	directives = append(directives, &crossplane.Directive{
		Directive: "split_clients",
		Args:      []string{"$request_id", "$backend_upstream"},
		Block:     splitDirectives,
	})

	// proxy_pass using the variable
	directives = append(directives, &crossplane.Directive{
		Directive: "proxy_pass",
		Args:      []string{"http://$backend_upstream"},
	})

	return directives
}

// buildLocationPattern converts a RouteMatch to an NGINX location pattern
func (r *ResourceStore) buildLocationPattern(match *resourceapi.RouteMatch) string {
	if match == nil || match.Path == nil {
		return "/"
	}

	switch pathMatch := match.Path.Kind.(type) {
	case *resourceapi.PathMatch_Exact:
		return fmt.Sprintf("= %s", pathMatch.Exact)
	case *resourceapi.PathMatch_PathPrefix:
		return pathMatch.PathPrefix
	case *resourceapi.PathMatch_Regex:
		return fmt.Sprintf("~ %s", pathMatch.Regex)
	default:
		return "/"
	}
}

// backendToUpstreamName converts a BackendReference to AddressStore's upstream naming
// Returns the upstream name and the service key (for EnsureUpstreamFile)
func (r *ResourceStore) backendToUpstreamName(backendRef *resourceapi.BackendReference) (string, string) {
	if backendRef == nil {
		return "", ""
	}

	switch kind := backendRef.Kind.(type) {
	case *resourceapi.BackendReference_Service_:
		svc := kind.Service
		port := backendRef.Port

		// Convert to AddressStore's upstream naming: namespace_name_port
		// The service hostname format is typically "name.namespace.svc.cluster.local"
		// We need to build the service key as namespace/hostname/port
		serviceKey := fmt.Sprintf("%s/%s/%d", svc.Namespace, svc.Hostname, port)
		upstreamName := nginx.ServiceKeyToUpstreamName(serviceKey)
		return upstreamName, serviceKey

	case *resourceapi.BackendReference_Backend:
		// Backend references (not service) - not implemented yet
		return "", ""
	}

	return "", ""
}

// generateTLSFiles writes TLS certificate and key files to disk
func (r *ResourceStore) generateTLSFiles(listenerKey string, tlsConfig *resourceapi.TLSConfig) error {
	log := slog.With("listener_key", listenerKey)

	// Write certificate
	certFile := filepath.Join("certs", fmt.Sprintf("%s.crt", sanitizeFilename(listenerKey)))
	if err := r.nginxMgr.WriteConfig(certFile, &crossplane.Config{
		Parsed: crossplane.Directives{},
	}); err != nil {
		// WriteConfig uses crossplane, but for raw files we need to write directly
		// Let's write the file directly instead
	}

	// Write files directly since they're not NGINX config format
	certPath := filepath.Join(r.nginxMgr.ConfigPath(), "certs", fmt.Sprintf("%s.crt", sanitizeFilename(listenerKey)))
	keyPath := filepath.Join(r.nginxMgr.ConfigPath(), "certs", fmt.Sprintf("%s.key", sanitizeFilename(listenerKey)))

	if err := os.WriteFile(certPath, tlsConfig.Cert, 0644); err != nil {
		log.Error("failed to write certificate", "error", err)
		return err
	}

	if err := os.WriteFile(keyPath, tlsConfig.PrivateKey, 0600); err != nil {
		log.Error("failed to write private key", "error", err)
		return err
	}

	log.Info("wrote TLS files")
	return nil
}

// deleteServerFile removes a server configuration file
func (r *ResourceStore) deleteServerFile(serverFileKey string) error {
	filename := fmt.Sprintf("servers/%s.conf", sanitizeFilename(serverFileKey))
	return r.nginxMgr.DeleteConfig(filename)
}

// deleteRouteFile removes a route configuration file
func (r *ResourceStore) deleteRouteFile(routeKey string) error {
	// Find route's listener key (need to track this)
	route, ok := r.routes[routeKey]
	if !ok {
		return nil
	}

	filename := fmt.Sprintf("routes/%s.conf", routeFileKey(routeKey, route.ListenerKey))
	return r.nginxMgr.DeleteConfig(filename)
}

// deleteTLSFiles removes TLS certificate and key files
func (r *ResourceStore) deleteTLSFiles(listenerKey string) error {
	certFile := filepath.Join(r.nginxMgr.ConfigPath(), "certs", fmt.Sprintf("%s.crt", sanitizeFilename(listenerKey)))
	keyFile := filepath.Join(r.nginxMgr.ConfigPath(), "certs", fmt.Sprintf("%s.key", sanitizeFilename(listenerKey)))

	os.Remove(certFile) // Ignore errors
	os.Remove(keyFile)

	return nil
}

// routeMatchesHostname checks if a route matches a given hostname
func (r *ResourceStore) routeMatchesHostname(route *resourceapi.Route, hostname string) bool {
	// If route has no hostnames, it matches listener's hostname
	if len(route.Hostnames) == 0 {
		return true
	}

	// Check if hostname matches any route hostname
	for _, h := range route.Hostnames {
		if h == hostname || h == "*" {
			return true
		}
		// Could add wildcard matching here (*.example.com)
	}

	return false
}

// Helper functions

// addToReverseMap adds a value to a reverse lookup map
func (r *ResourceStore) addToReverseMap(m map[string][]string, key, value string) {
	if m[key] == nil {
		m[key] = []string{}
	}

	// Avoid duplicates
	for _, v := range m[key] {
		if v == value {
			return
		}
	}

	m[key] = append(m[key], value)
}

// removeFromReverseMap removes a value from a reverse lookup map
func (r *ResourceStore) removeFromReverseMap(m map[string][]string, key, value string) {
	values := m[key]
	for i, v := range values {
		if v == value {
			m[key] = append(values[:i], values[i+1:]...)
			break
		}
	}

	if len(m[key]) == 0 {
		delete(m, key)
	}
}

// makeServerFileKey creates a server file key from listener key and hostname
func makeServerFileKey(listenerKey, hostname string) string {
	return fmt.Sprintf("%s:%s", listenerKey, hostname)
}

// parseServerFileKey splits a server file key into listener key and hostname
func parseServerFileKey(key string) (listenerKey, hostname string) {
	parts := strings.SplitN(key, ":", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return parts[0], ""
}

// sanitizeFilename replaces filesystem-unsafe characters
func sanitizeFilename(s string) string {
	replacer := strings.NewReplacer(
		"/", "-",
		":", "-",
		"*", "wildcard",
	)
	return replacer.Replace(s)
}

// routeFileKey creates a route file key from route key and listener key
func routeFileKey(routeKey, listenerKey string) string {
	return fmt.Sprintf("%s-%s", sanitizeFilename(routeKey), sanitizeFilename(listenerKey))
}

// onServiceAvailable is called when a service becomes available (has workloads)
// This regenerates all routes that reference this service
func (r *ResourceStore) onServiceAvailable(serviceKey string) {
	r.lock.Lock()
	defer r.lock.Unlock()

	log := slog.With("service", serviceKey)
	routeKeys, ok := r.serviceToRoutes[serviceKey]
	if !ok || len(routeKeys) == 0 {
		log.Info("service became available but no routes reference it")
		return
	}

	log.Info("service became available, regenerating routes", "route_count", len(routeKeys))

	// Regenerate all affected route files
	for _, routeKey := range routeKeys {
		if err := r.generateRouteFile(routeKey); err != nil {
			slog.Error("failed to regenerate route file for newly available service",
				"route", routeKey, "service", serviceKey, "error", err)
		}
	}

	// Reload NGINX to pick up the changes
	if err := r.nginxMgr.Reload(); err != nil {
		slog.Error("failed to reload nginx after service became available", "service", serviceKey, "error", err)
	}
}
