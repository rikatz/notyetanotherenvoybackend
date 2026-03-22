package store

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
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

// MatchWithMetadata wraps a RouteMatch with its route context for global sorting
type MatchWithMetadata struct {
	Match       *resourceapi.RouteMatch
	Route       *resourceapi.Route
	RouteKey    string
	MatchIndex  int // Index within the route's Matches array
}

// MatchSpecificity represents the Gateway API specificity score for a match
type MatchSpecificity struct {
	PathType        int // 0=exact, 1=prefix, 2=regex
	PathLength      int // Length of path (longer is more specific for prefix)
	HasMethod       bool
	HeaderCount     int
	QueryParamCount int
	RouteAge        int64 // Creation timestamp (older = higher priority on ties)
	RouteName       string // namespace/name for alphabetical tiebreaker
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
		r.updateRoute(route, affectedServerFiles)
	}

	// Phase 5: Regenerate affected configurations
	for serverFileKey := range affectedServerFiles {
		if err := r.generateServerFile(serverFileKey); err != nil {
			slog.Error("failed to generate server file", "key", serverFileKey, "error", err)
		}
	}

	// Phase 6: Reload NGINX if any changes were made
	if len(affectedServerFiles) > 0 {
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

	for _, name := range resourceNames {
		log := slog.With("resource", name)

		// Try each resource type
		if bind, ok := r.binds[name]; ok {
			r.removeBind(bind, affectedServerFiles)
		} else if listener, ok := r.listeners[name]; ok {
			r.removeListener(listener, affectedServerFiles)
		} else if route, ok := r.routes[name]; ok {
			r.removeRoute(route, affectedServerFiles)
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

	// Reload NGINX if any changes were made
	if len(affectedServerFiles) > 0 {
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
func (r *ResourceStore) updateRoute(route *resourceapi.Route, affectedServers map[string]struct{}) {
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
func (r *ResourceStore) removeListener(listener *resourceapi.Listener, affectedServers map[string]struct{}) {
	// Remove all routes using this listener
	if routeKeys, ok := r.listenerToRoutes[listener.Key]; ok {
		for _, routeKey := range routeKeys {
			if route, exists := r.routes[routeKey]; exists {
				for _, hostname := range route.Hostnames {
					serverKey := makeServerFileKey(listener.Key, hostname)
					affectedServers[serverKey] = struct{}{}
				}
				delete(r.routes, routeKey)
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
func (r *ResourceStore) removeRoute(route *resourceapi.Route, affectedServers map[string]struct{}) {
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

	// Sort routes by match specificity (most specific first)
	// This ensures that exact matches take precedence over prefix matches over regex
	slices.SortFunc(routeKeys, func(a, b string) int {
		return r.compareRouteSpecificity(r.routes[a], r.routes[b])
	})

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

	// Collect all matches from all routes and sort by Gateway API specificity
	var allMatches []MatchWithMetadata
	for _, routeKey := range routeKeys {
		route := r.routes[routeKey]
		if route == nil {
			continue
		}

		// If route has no matches, add a default match
		if len(route.Matches) == 0 {
			allMatches = append(allMatches, MatchWithMetadata{
				Match:      nil, // nil means default PathPrefix "/"
				Route:      route,
				RouteKey:   routeKey,
				MatchIndex: 0,
			})
		} else {
			for idx, match := range route.Matches {
				allMatches = append(allMatches, MatchWithMetadata{
					Match:      match,
					Route:      route,
					RouteKey:   routeKey,
					MatchIndex: idx,
				})
			}
		}
	}

	// Sort matches by Gateway API specificity (most specific first)
	slices.SortFunc(allMatches, func(a, b MatchWithMetadata) int {
		aSpec := r.getMatchSpecificity(a.Match, a.Route)
		bSpec := r.getMatchSpecificity(b.Match, b.Route)
		return r.compareMatchSpecificity(aSpec, bSpec)
	})

	// Group matches by path pattern
	pathGroups := make(map[string][]MatchWithMetadata)
	for _, m := range allMatches {
		pathKey := r.getPathKey(m.Match)
		pathGroups[pathKey] = append(pathGroups[pathKey], m)
	}

	// Generate location blocks for each path pattern
	for _, matches := range pathGroups {
		locationBlock := r.generateLocationBlockForMatches(matches)
		if locationBlock != nil {
			serverDirectives = append(serverDirectives, locationBlock)
		}
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

	log.Info("generated server file", "routes", len(routeKeys), "matches", len(allMatches), "paths", len(pathGroups))
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

		// Determine location pattern arguments
		locationArgs := r.buildLocationArgs(match)

		locationBlock := &crossplane.Directive{
			Directive: "location",
			Args:      locationArgs,
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

	// Process traffic policies for filters (redirects, rewrites, headers)
	var requestHeaderMod *resourceapi.HeaderModifier
	var responseHeaderMod *resourceapi.HeaderModifier
	var requestRedirect *resourceapi.RequestRedirect
	var urlRewrite *resourceapi.UrlRewrite

	for _, policy := range route.TrafficPolicies {
		if reqHdr := policy.GetRequestHeaderModifier(); reqHdr != nil {
			requestHeaderMod = reqHdr
		}
		if respHdr := policy.GetResponseHeaderModifier(); respHdr != nil {
			responseHeaderMod = respHdr
		}
		if redirect := policy.GetRequestRedirect(); redirect != nil {
			requestRedirect = redirect
		}
		if rewrite := policy.GetUrlRewrite(); rewrite != nil {
			urlRewrite = rewrite
		}
	}

	// Handle redirects first (they don't proxy to backends)
	if requestRedirect != nil {
		directives = append(directives, r.generateRedirectDirectives(requestRedirect, match)...)
		return directives
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

	// Add default proxy headers first (so user modifications can override them)
	// Only add Host header if not being rewritten by URLRewrite or HeaderModifier
	hostRewritten := (urlRewrite != nil && urlRewrite.Host != "") ||
		(requestHeaderMod != nil && hasHeaderModification(requestHeaderMod, "Host"))

	if !hostRewritten {
		directives = append(directives, &crossplane.Directive{
			Directive: "proxy_set_header",
			Args:      []string{"Host", "$host"},
		})
	}

	// Add other standard headers (check if being modified by user)
	if requestHeaderMod == nil || !hasHeaderModification(requestHeaderMod, "X-Real-IP") {
		directives = append(directives, &crossplane.Directive{
			Directive: "proxy_set_header",
			Args:      []string{"X-Real-IP", "$remote_addr"},
		})
	}
	if requestHeaderMod == nil || !hasHeaderModification(requestHeaderMod, "X-Forwarded-For") {
		directives = append(directives, &crossplane.Directive{
			Directive: "proxy_set_header",
			Args:      []string{"X-Forwarded-For", "$proxy_add_x_forwarded_for"},
		})
	}

	// Apply URL rewrite if specified (may override Host header)
	if urlRewrite != nil {
		directives = append(directives, r.generateURLRewriteDirectives(urlRewrite, match)...)
	}

	// Apply request header modifications (may override any headers)
	if requestHeaderMod != nil {
		directives = append(directives, r.generateRequestHeaderDirectives(requestHeaderMod)...)
	}

	// Apply response header modifications
	if responseHeaderMod != nil {
		directives = append(directives, r.generateResponseHeaderDirectives(responseHeaderMod)...)
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

	return directives
}

// generateRedirectDirectives creates NGINX return directive for HTTP redirects
func (r *ResourceStore) generateRedirectDirectives(redirect *resourceapi.RequestRedirect, match *resourceapi.RouteMatch) crossplane.Directives {
	var directives crossplane.Directives

	// Default status code is 302
	statusCode := redirect.Status
	if statusCode == 0 {
		statusCode = 302
	}

	// Build redirect URL components
	scheme := redirect.Scheme
	if scheme == "" {
		scheme = "$scheme" // Keep original scheme
	}

	host := redirect.Host
	if host == "" {
		host = "$host" // Keep original host
	}

	// Only omit port if it matches the standard port for the explicit scheme
	port := ""
	if redirect.Port > 0 {
		omitPort := (scheme == "http" && redirect.Port == 80) || (scheme == "https" && redirect.Port == 443)
		if !omitPort {
			port = fmt.Sprintf(":%d", redirect.Port)
		}
	}

	// Handle path replacement
	if redirect.GetPrefix() != "" {
		// Prefix replacement: strip matched prefix and prepend new prefix
		matchedPrefix := r.getMatchedPathPrefix(match)
		if matchedPrefix == "" {
			// Can't implement prefix replacement without knowing what prefix was matched
			// This happens with regex path matches - log warning and skip redirect
			slog.Warn("prefix redirect requires exact or prefix path match, skipping redirect",
				"redirect_prefix", redirect.GetPrefix(),
				"match_type", "regex or missing")
			// Fall through to use default path behavior instead
		} else {
			// Use rewrite directive for prefix replacement with redirect flag
			redirectFlag := "redirect" // 302
			if statusCode == 301 {
				redirectFlag = "permanent"
			}

			// Build redirect URL without path (we'll use rewrite to handle path)
			baseURL := fmt.Sprintf("%s://%s%s", scheme, host, port)

			// Rewrite to strip matched prefix and prepend new prefix, then redirect
			directives = append(directives, &crossplane.Directive{
				Directive: "rewrite",
				Args:      []string{fmt.Sprintf("^%s(.*)$", matchedPrefix), fmt.Sprintf("%s%s$1", baseURL, redirect.GetPrefix()), redirectFlag},
			})
			return directives
		}
	}

	// Handle full path replacement or default
	path := "$request_uri" // Default: keep original path
	if redirect.GetFull() != "" {
		path = redirect.GetFull()
	}

	redirectURL := fmt.Sprintf("%s://%s%s%s", scheme, host, port, path)

	directives = append(directives, &crossplane.Directive{
		Directive: "return",
		Args:      []string{fmt.Sprintf("%d", statusCode), redirectURL},
	})

	return directives
}

// generateURLRewriteDirectives creates NGINX rewrite directives for URL rewriting
func (r *ResourceStore) generateURLRewriteDirectives(urlRewrite *resourceapi.UrlRewrite, match *resourceapi.RouteMatch) crossplane.Directives {
	var directives crossplane.Directives

	// Host rewrite (set Host header)
	if urlRewrite.Host != "" {
		directives = append(directives, &crossplane.Directive{
			Directive: "proxy_set_header",
			Args:      []string{"Host", quoteHeaderValue(urlRewrite.Host)},
		})
	}

	// Path rewrite
	if urlRewrite.GetFull() != "" {
		// Full path replacement - replace entire path
		directives = append(directives, &crossplane.Directive{
			Directive: "rewrite",
			Args:      []string{"^.*$", urlRewrite.GetFull(), "break"},
		})
	} else if urlRewrite.GetPrefix() != "" {
		// Prefix replacement: replace matched prefix with new prefix
		// Need to know what prefix was matched to strip it correctly
		matchedPrefix := r.getMatchedPathPrefix(match)
		if matchedPrefix != "" {
			// Strip the matched prefix and prepend the new prefix
			// Example: ^/v1(.*)$ /v2$1 -> /v1/users becomes /v2/users
			directives = append(directives, &crossplane.Directive{
				Directive: "rewrite",
				Args:      []string{fmt.Sprintf("^%s(.*)$", matchedPrefix), urlRewrite.GetPrefix() + "$1", "break"},
			})
		} else {
			// No matched prefix (shouldn't happen in valid configs)
			// Fall back to prepending (preserves old buggy behavior as safety net)
			directives = append(directives, &crossplane.Directive{
				Directive: "rewrite",
				Args:      []string{"^(.*)$", urlRewrite.GetPrefix() + "$1", "break"},
			})
		}
	}

	return directives
}

// getMatchedPathPrefix extracts the path prefix from a RouteMatch for rewrite operations
func (r *ResourceStore) getMatchedPathPrefix(match *resourceapi.RouteMatch) string {
	if match == nil || match.Path == nil {
		return ""
	}

	switch pathMatch := match.Path.Kind.(type) {
	case *resourceapi.PathMatch_Exact:
		// Exact match - return the exact path
		return pathMatch.Exact
	case *resourceapi.PathMatch_PathPrefix:
		// Prefix match - return the prefix
		return pathMatch.PathPrefix
	case *resourceapi.PathMatch_Regex:
		// Regex match - can't reliably extract a prefix to replace
		// Gateway API spec doesn't support prefix rewrite with regex matches
		return ""
	default:
		return ""
	}
}

// generateRequestHeaderDirectives creates directives for request header modifications
func (r *ResourceStore) generateRequestHeaderDirectives(headerMod *resourceapi.HeaderModifier) crossplane.Directives {
	var directives crossplane.Directives

	// Remove headers first (by not passing them)
	for _, name := range headerMod.Remove {
		directives = append(directives, &crossplane.Directive{
			Directive: "proxy_set_header",
			Args:      []string{name, `""`},
		})
	}

	// Set headers (overwrite)
	for _, header := range headerMod.Set {
		directives = append(directives, &crossplane.Directive{
			Directive: "proxy_set_header",
			Args:      []string{header.Name, quoteHeaderValue(header.Value)},
		})
	}

	// Add headers (append to existing)
	for _, header := range headerMod.Add {
		varName := fmt.Sprintf("$http_%s", strings.ReplaceAll(strings.ToLower(header.Name), "-", "_"))
		tmpVar := fmt.Sprintf("$add_header_%s", strings.ReplaceAll(strings.ToLower(header.Name), "-", "_"))

		// Escape the header value for safe interpolation in NGINX strings
		// We need to escape backslashes and quotes for use within double-quoted NGINX strings
		escapedValue := strings.ReplaceAll(header.Value, `\`, `\\`)
		escapedValue = strings.ReplaceAll(escapedValue, `"`, `\"`)

		// Set temp variable to new value by default
		directives = append(directives, &crossplane.Directive{
			Directive: "set",
			Args:      []string{tmpVar, fmt.Sprintf(`"%s"`, escapedValue)},
		})

		// If original header exists, append to it
		directives = append(directives, &crossplane.Directive{
			Directive: "if",
			Args:      []string{fmt.Sprintf("(%s != \"\")", varName)},
			Block: crossplane.Directives{
				{
					Directive: "set",
					// Concatenate: existing value (via variable) + comma + new value
					Args: []string{tmpVar, fmt.Sprintf(`"%s,%s"`, varName, escapedValue)},
				},
			},
		})

		// Set the final header value
		directives = append(directives, &crossplane.Directive{
			Directive: "proxy_set_header",
			Args:      []string{header.Name, tmpVar},
		})
	}

	return directives
}

// generateResponseHeaderDirectives creates directives for response header modifications
func (r *ResourceStore) generateResponseHeaderDirectives(headerMod *resourceapi.HeaderModifier) crossplane.Directives {
	var directives crossplane.Directives

	// NGINX response header manipulation limitations:
	// - proxy_hide_header prevents upstream headers from reaching the client
	// - add_header adds headers (does NOT replace - multiple directives accumulate)
	// - True replacement requires ngx_headers_more (more_clear_headers/more_set_headers)
	//
	// Our strategy:
	// - Remove: proxy_hide_header (hides upstream value)
	// - Set: proxy_hide_header (hide upstream) + add_header (set new value)
	//   Note: This only replaces upstream headers, not headers added by other NGINX directives
	// - Add: add_header (append to existing)

	// Remove headers
	for _, name := range headerMod.Remove {
		directives = append(directives, &crossplane.Directive{
			Directive: "proxy_hide_header",
			Args:      []string{name},
		})
	}

	// Set headers (replace)
	// First hide the upstream header, then add our value
	for _, header := range headerMod.Set {
		directives = append(directives, &crossplane.Directive{
			Directive: "proxy_hide_header",
			Args:      []string{header.Name},
		})
		directives = append(directives, &crossplane.Directive{
			Directive: "add_header",
			Args:      []string{header.Name, quoteHeaderValue(header.Value), "always"},
		})
	}

	// Add headers (append)
	// add_header naturally accumulates multiple values for the same header
	for _, header := range headerMod.Add {
		directives = append(directives, &crossplane.Directive{
			Directive: "add_header",
			Args:      []string{header.Name, quoteHeaderValue(header.Value), "always"},
		})
	}

	return directives
}

// quoteHeaderValue escapes and quotes a header value for use in NGINX config
func quoteHeaderValue(value string) string {
	// Escape backslashes and quotes
	escaped := strings.ReplaceAll(value, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `"`, `\"`)
	return fmt.Sprintf(`"%s"`, escaped)
}

// hasHeaderModification checks if a header is being modified
func hasHeaderModification(headerMod *resourceapi.HeaderModifier, headerName string) bool {
	if headerMod == nil {
		return false
	}

	headerNameLower := strings.ToLower(headerName)

	for _, h := range headerMod.Set {
		if strings.ToLower(h.Name) == headerNameLower {
			return true
		}
	}

	for _, h := range headerMod.Add {
		if strings.ToLower(h.Name) == headerNameLower {
			return true
		}
	}

	for _, name := range headerMod.Remove {
		if strings.ToLower(name) == headerNameLower {
			return true
		}
	}

	return false
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
// Uses modifiers to enforce Gateway API matching precedence: exact > prefix > regex
// buildLocationArgs returns the arguments for a location directive
// The modifier and path are returned as separate elements to ensure correct NGINX syntax
func (r *ResourceStore) buildLocationArgs(match *resourceapi.RouteMatch) []string {
	if match == nil || match.Path == nil {
		return []string{"/"}
	}

	switch pathMatch := match.Path.Kind.(type) {
	case *resourceapi.PathMatch_Exact:
		// Exact match - highest priority, matches immediately
		return []string{"=", pathMatch.Exact}
	case *resourceapi.PathMatch_PathPrefix:
		// Prefix match with ^~ modifier - if selected as best prefix, prevents regex evaluation
		// This enforces Gateway API semantics where prefix should win over regex
		return []string{"^~", pathMatch.PathPrefix}
	case *resourceapi.PathMatch_Regex:
		// Regex match - lowest priority per Gateway API spec
		// Note: In NGINX, regex normally overrides prefix, but ^~ on prefix prevents this
		return []string{"~", pathMatch.Regex}
	default:
		return []string{"/"}
	}
}

// getMatchSpecificity computes the Gateway API specificity score for a match
// According to Gateway API spec, matches are prioritized by:
// 1. Exact path match
// 2. Prefix path match with largest number of characters
// 3. Method match
// 4. Largest number of header matches
// 5. Largest number of query param matches
func (r *ResourceStore) getMatchSpecificity(match *resourceapi.RouteMatch, route *resourceapi.Route) MatchSpecificity {
	spec := MatchSpecificity{}

	// Determine path type and length
	if match == nil || match.Path == nil {
		// Default is PathPrefix "/"
		spec.PathType = 1
		spec.PathLength = 1
	} else {
		switch pathMatch := match.Path.Kind.(type) {
		case *resourceapi.PathMatch_Exact:
			spec.PathType = 0 // Exact has highest priority
			spec.PathLength = len(pathMatch.Exact)
		case *resourceapi.PathMatch_PathPrefix:
			spec.PathType = 1 // Prefix has medium priority
			spec.PathLength = len(pathMatch.PathPrefix)
		case *resourceapi.PathMatch_Regex:
			spec.PathType = 2 // Regex has lowest priority (implementation-specific)
			spec.PathLength = 0
		}
	}

	// Method match
	if match != nil && match.Method != nil {
		spec.HasMethod = true
	}

	// Header matches
	if match != nil {
		spec.HeaderCount = len(match.Headers)
	}

	// Query param matches
	if match != nil {
		spec.QueryParamCount = len(match.QueryParams)
	}

	// Route metadata for tiebreakers
	if route != nil && route.Name != nil {
		// Use creation timestamp if available (older = higher priority)
		// For now, we'll use 0 since we don't have access to k8s metadata
		spec.RouteAge = 0
		spec.RouteName = fmt.Sprintf("%s/%s", route.Name.Namespace, route.Name.Name)
	}

	return spec
}

// compareMatchSpecificity compares two matches according to Gateway API precedence rules
// Returns negative if a is more specific, positive if b is more specific, 0 if equal
func (r *ResourceStore) compareMatchSpecificity(a, b MatchSpecificity) int {
	// 1. Exact path match wins
	if a.PathType != b.PathType {
		return a.PathType - b.PathType // Lower value = more specific
	}

	// 2. For prefix matches, longer path wins
	if a.PathType == 1 && b.PathType == 1 {
		if a.PathLength != b.PathLength {
			return b.PathLength - a.PathLength // Longer = more specific
		}
	}

	// 3. Method match present
	if a.HasMethod != b.HasMethod {
		if a.HasMethod {
			return -1
		}
		return 1
	}

	// 4. More header matches wins
	if a.HeaderCount != b.HeaderCount {
		return b.HeaderCount - a.HeaderCount
	}

	// 5. More query param matches wins
	if a.QueryParamCount != b.QueryParamCount {
		return b.QueryParamCount - a.QueryParamCount
	}

	// Tiebreakers: older route, then alphabetical by namespace/name
	if a.RouteAge != b.RouteAge {
		return int(a.RouteAge - b.RouteAge) // Older (smaller timestamp) wins
	}

	return strings.Compare(a.RouteName, b.RouteName)
}

// getPathKey returns a unique key for grouping matches by their path pattern
func (r *ResourceStore) getPathKey(match *resourceapi.RouteMatch) string {
	if match == nil || match.Path == nil {
		return "prefix:/"
	}

	switch pathMatch := match.Path.Kind.(type) {
	case *resourceapi.PathMatch_Exact:
		return fmt.Sprintf("exact:%s", pathMatch.Exact)
	case *resourceapi.PathMatch_PathPrefix:
		return fmt.Sprintf("prefix:%s", pathMatch.PathPrefix)
	case *resourceapi.PathMatch_Regex:
		return fmt.Sprintf("regex:%s", pathMatch.Regex)
	default:
		return "prefix:/"
	}
}

// generateLocationBlockForMatches generates a single location block that evaluates
// multiple matches with the same path pattern, sorted by specificity
func (r *ResourceStore) generateLocationBlockForMatches(matches []MatchWithMetadata) *crossplane.Directive {
	if len(matches) == 0 {
		return nil
	}

	// Use the first match's path for the location pattern
	locationArgs := r.buildLocationArgs(matches[0].Match)

	var locationDirectives crossplane.Directives

	// Add comment showing which routes/matches are in this location
	routeNames := make(map[string]bool)
	for _, m := range matches {
		if m.Route.Name != nil {
			routeName := fmt.Sprintf("%s/%s", m.Route.Name.Namespace, m.Route.Name.Name)
			routeNames[routeName] = true
		}
	}
	if len(routeNames) > 0 {
		var names []string
		for name := range routeNames {
			names = append(names, name)
		}
		slices.Sort(names)
		comment := fmt.Sprintf("Routes: %s", strings.Join(names, ", "))
		locationDirectives = append(locationDirectives, &crossplane.Directive{
			Directive: "#",
			Args:      []string{comment},
			Comment:   &comment,
		})
	}

	// If all matches in this location are simple (no filters, single backend),
	// we can use the simple backend selection approach
	// Otherwise, we need to generate full match evaluation with filters
	allSimple := true
	for _, m := range matches {
		if len(m.Route.TrafficPolicies) > 0 || len(m.Route.Backends) != 1 {
			allSimple = false
			break
		}
	}

	if allSimple {
		// Simple case: just backend selection
		locationDirectives = append(locationDirectives, r.generateSimpleBackendSelection(matches)...)
	} else {
		// Complex case: need to handle filters and weighted backends
		locationDirectives = append(locationDirectives, r.generateComplexMatchEvaluation(matches)...)
	}

	return &crossplane.Directive{
		Directive: "location",
		Args:      locationArgs,
		Block:     locationDirectives,
	}
}

// generateSimpleBackendSelection generates simple backend selection
// For path-only matches, directly proxy to the backend
// For matches with conditions, use the first match (already sorted by specificity)
func (r *ResourceStore) generateSimpleBackendSelection(matches []MatchWithMetadata) crossplane.Directives {
	var directives crossplane.Directives

	// Since all matches in this group share the same path and are simple (single backend, no filters),
	// and they're already sorted by specificity, we can just use the first matching backend
	if len(matches) > 0 {
		backend := r.getSimpleBackendName(matches[0])
		if backend != "" {
			// Add default proxy headers
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

			// Proxy pass directly to backend
			directives = append(directives, &crossplane.Directive{
				Directive: "proxy_pass",
				Args:      []string{fmt.Sprintf("http://%s", backend)},
			})

			return directives
		}
	}

	// No valid backend, return 404
	directives = append(directives, &crossplane.Directive{
		Directive: "return",
		Args:      []string{"404"},
	})

	return directives
}

// generateComplexMatchEvaluation generates full match evaluation with filters
// This creates nested if blocks that evaluate conditions and apply filters/backends
func (r *ResourceStore) generateComplexMatchEvaluation(matches []MatchWithMetadata) crossplane.Directives {
	var directives crossplane.Directives

	// Use a flag variable to track if a match was found
	directives = append(directives, &crossplane.Directive{
		Directive: "set",
		Args:      []string{"$matched", `"0"`},
	})

	for _, m := range matches {
		condition := r.buildMatchCondition(m.Match)

		// Get full directives for this match (includes filters and backend)
		matchDirectives := r.generateLocationDirectives(m.Route, m.Match)

		if condition == "" {
			// No conditions - execute if not already matched
			directives = append(directives, &crossplane.Directive{
				Directive: "if",
				Args:      []string{`($matched = "0")`},
				Block: append(crossplane.Directives{
					{
						Directive: "set",
						Args:      []string{"$matched", `"1"`},
					},
				}, matchDirectives...),
			})
		} else {
			// Has conditions - execute if conditions match and not already matched
			combinedCondition := fmt.Sprintf(`($matched = "0") && (%s)`, condition)
			directives = append(directives, &crossplane.Directive{
				Directive: "if",
				Args:      []string{combinedCondition},
				Block: append(crossplane.Directives{
					{
						Directive: "set",
						Args:      []string{"$matched", `"1"`},
					},
				}, matchDirectives...),
			})
		}
	}

	// If no match found, return 404
	directives = append(directives, &crossplane.Directive{
		Directive: "if",
		Args:      []string{`($matched = "0")`},
		Block: crossplane.Directives{
			{
				Directive: "return",
				Args:      []string{"404"},
			},
		},
	})

	return directives
}

// getSimpleBackendName returns the upstream name for a simple single-backend route
func (r *ResourceStore) getSimpleBackendName(m MatchWithMetadata) string {
	route := m.Route
	if route == nil || len(route.Backends) == 0 {
		return ""
	}

	backend := route.Backends[0]
	upstreamName, serviceKey := r.backendToUpstreamName(backend.Backend)
	if upstreamName == "" {
		return ""
	}

	// Ensure upstream file exists
	exists, err := r.addressStore.EnsureUpstreamFile(serviceKey)
	if err != nil || !exists {
		return ""
	}

	return upstreamName
}

// generateBackendSelectionLogic creates if-elif chain to select backend based on match conditions
// Matches are already sorted by specificity, so we evaluate in order
func (r *ResourceStore) generateBackendSelectionLogic(matches []MatchWithMetadata) crossplane.Directives {
	var directives crossplane.Directives

	// Initialize backend variable
	directives = append(directives, &crossplane.Directive{
		Directive: "set",
		Args:      []string{"$backend", `""`},
	})

	for _, m := range matches {
		// Build condition for this match (headers, method, query params)
		condition := r.buildMatchCondition(m.Match)

		// Determine backend for this match
		backend := r.getBackendForMatch(m)
		if backend == "" {
			// No valid backend, skip this match
			continue
		}

		if condition == "" {
			// No additional conditions (path-only match)
			// Set backend directly if not already set
			directives = append(directives, &crossplane.Directive{
				Directive: "if",
				Args:      []string{"($backend = \"\")"},
				Block: crossplane.Directives{
					{
						Directive: "set",
						Args:      []string{"$backend", fmt.Sprintf(`"%s"`, backend)},
					},
				},
			})
		} else {
			// Has additional conditions - set backend if conditions match AND backend not already set
			combinedCondition := fmt.Sprintf("($backend = \"\") && (%s)", condition)
			directives = append(directives, &crossplane.Directive{
				Directive: "if",
				Args:      []string{combinedCondition},
				Block: crossplane.Directives{
					{
						Directive: "set",
						Args:      []string{"$backend", fmt.Sprintf(`"%s"`, backend)},
					},
				},
			})
		}
	}

	// If no backend matched, return 404
	directives = append(directives, &crossplane.Directive{
		Directive: "if",
		Args:      []string{"($backend = \"\")"},
		Block: crossplane.Directives{
			{
				Directive: "return",
				Args:      []string{"404"},
			},
		},
	})

	return directives
}

// buildMatchCondition builds an NGINX condition string for non-path match criteria
// Returns empty string if no conditions (path-only match)
func (r *ResourceStore) buildMatchCondition(match *resourceapi.RouteMatch) string {
	if match == nil {
		return ""
	}

	var conditions []string

	// Method matching
	if match.Method != nil && match.Method.Exact != "" {
		conditions = append(conditions, fmt.Sprintf("$request_method = \"%s\"", match.Method.Exact))
	}

	// Header matching (all headers must match - AND logic)
	for _, header := range match.Headers {
		varName := fmt.Sprintf("$http_%s", strings.ReplaceAll(strings.ToLower(header.Name), "-", "_"))
		switch v := header.Value.(type) {
		case *resourceapi.HeaderMatch_Exact:
			conditions = append(conditions, fmt.Sprintf("%s = \"%s\"", varName, v.Exact))
		case *resourceapi.HeaderMatch_Regex:
			conditions = append(conditions, fmt.Sprintf("%s ~ \"%s\"", varName, v.Regex))
		}
	}

	// Query parameter matching (all params must match - AND logic)
	for _, query := range match.QueryParams {
		varName := fmt.Sprintf("$arg_%s", query.Name)
		switch v := query.Value.(type) {
		case *resourceapi.QueryMatch_Exact:
			conditions = append(conditions, fmt.Sprintf("%s = \"%s\"", varName, v.Exact))
		case *resourceapi.QueryMatch_Regex:
			conditions = append(conditions, fmt.Sprintf("%s ~ \"%s\"", varName, v.Regex))
		}
	}

	if len(conditions) == 0 {
		return ""
	}

	// Join with AND
	return strings.Join(conditions, " && ")
}

// getBackendForMatch returns the upstream name for a match
// Handles single backend, weighted backends, and redirects
func (r *ResourceStore) getBackendForMatch(m MatchWithMetadata) string {
	route := m.Route
	if route == nil {
		return ""
	}

	// Check for redirect filter - redirects don't use backends
	for _, policy := range route.TrafficPolicies {
		if policy.GetRequestRedirect() != nil {
			// TODO: Handle redirects in this new architecture
			// For now, skip redirect routes
			slog.Warn("redirect routes not yet supported in new match architecture", "route", m.RouteKey)
			return ""
		}
	}

	// Get backend references
	if len(route.Backends) == 0 {
		return ""
	}

	// For simplicity, use first backend for now
	// TODO: Handle weighted backends properly
	backend := route.Backends[0]
	upstreamName, serviceKey := r.backendToUpstreamName(backend.Backend)
	if upstreamName == "" {
		return ""
	}

	// Ensure upstream file exists
	exists, err := r.addressStore.EnsureUpstreamFile(serviceKey)
	if err != nil || !exists {
		return ""
	}

	return upstreamName
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

// compareRouteSpecificity compares two routes for sorting by specificity
// Returns -1 if route a is more specific than b, 1 if b is more specific, 0 if equal
// Specificity order: Exact path > Prefix path (longer first) > Regex path
func (r *ResourceStore) compareRouteSpecificity(a, b *resourceapi.Route) int {
	// Get the most specific match from each route
	aSpec := r.getRouteMatchSpecificity(a)
	bSpec := r.getRouteMatchSpecificity(b)

	// Compare by path match type first
	if aSpec.matchType != bSpec.matchType {
		return int(aSpec.matchType) - int(bSpec.matchType)
	}

	// If same match type, compare by path length (longer/more specific first)
	if aSpec.pathLength != bSpec.pathLength {
		return bSpec.pathLength - aSpec.pathLength
	}

	// If still equal, compare by number of match criteria (more specific first)
	return bSpec.criteriaCount - aSpec.criteriaCount
}

type routeSpecificity struct {
	matchType     pathMatchType
	pathLength    int
	criteriaCount int
}

type pathMatchType int

const (
	pathMatchExact  pathMatchType = 0 // Most specific
	pathMatchPrefix pathMatchType = 1
	pathMatchRegex  pathMatchType = 2 // Least specific
	pathMatchNone   pathMatchType = 3 // No path match
)

// getRouteMatchSpecificity calculates the specificity of a route's most specific match
func (r *ResourceStore) getRouteMatchSpecificity(route *resourceapi.Route) routeSpecificity {
	if len(route.Matches) == 0 {
		// No matches means match-all (least specific)
		return routeSpecificity{
			matchType:     pathMatchNone,
			pathLength:    0,
			criteriaCount: 0,
		}
	}

	// Find the most specific match
	mostSpecific := routeSpecificity{
		matchType:     pathMatchNone,
		pathLength:    0,
		criteriaCount: 0,
	}

	for _, match := range route.Matches {
		spec := routeSpecificity{}

		// Determine path match type and length
		if match.Path != nil {
			switch match.Path.Kind.(type) {
			case *resourceapi.PathMatch_Exact:
				spec.matchType = pathMatchExact
				spec.pathLength = len(match.Path.Kind.(*resourceapi.PathMatch_Exact).Exact)
			case *resourceapi.PathMatch_PathPrefix:
				spec.matchType = pathMatchPrefix
				spec.pathLength = len(match.Path.Kind.(*resourceapi.PathMatch_PathPrefix).PathPrefix)
			case *resourceapi.PathMatch_Regex:
				spec.matchType = pathMatchRegex
				spec.pathLength = len(match.Path.Kind.(*resourceapi.PathMatch_Regex).Regex)
			}
		} else {
			spec.matchType = pathMatchNone
		}

		// Count additional match criteria
		if match.Method != nil {
			spec.criteriaCount++
		}
		spec.criteriaCount += len(match.Headers)
		spec.criteriaCount += len(match.QueryParams)

		// Keep the most specific match
		if r.isMoreSpecific(spec, mostSpecific) {
			mostSpecific = spec
		}
	}

	return mostSpecific
}

// isMoreSpecific returns true if spec a is more specific than spec b
func (r *ResourceStore) isMoreSpecific(a, b routeSpecificity) bool {
	if a.matchType != b.matchType {
		return a.matchType < b.matchType
	}
	if a.pathLength != b.pathLength {
		return a.pathLength > b.pathLength
	}
	return a.criteriaCount > b.criteriaCount
}

// Helper functions

// addToReverseMap adds a value to a reverse lookup map
func (r *ResourceStore) addToReverseMap(m map[string][]string, key, value string) {
	if m[key] == nil {
		m[key] = []string{}
	}

	// Avoid duplicates
	if slices.Contains(m[key], value) {
		return
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
