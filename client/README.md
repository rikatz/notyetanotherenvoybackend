# Agentgateway xDS Client

> **⚠️ Demo/PoC**: This client is part of a demonstration repository. While functional, it is intended for educational purposes and prototyping custom backends.

This package provides an xDS (Discovery Service) client for connecting to the [agentgateway](https://github.com/agentgateway/agentgateway) control plane. It uses the Delta Discovery protocol to receive incremental updates of workload endpoints and Gateway API routing configuration.

## Overview

The client connects to agentgateway and subscribes to xDS resource types. When resources are created, updated, or removed, your custom handlers are called to process the changes. This enables you to build custom Gateway API backends using any data plane technology (NGINX, HAProxy, custom proxies, etc.).

**Key Features:**
- Delta Discovery protocol for efficient incremental updates
- JWT-based authentication using Kubernetes ServiceAccount tokens
- Type-safe resource handling with pluggable handlers
- Support for multiple resource types (Address, Resource)
- Automatic ACK/NACK handling

## Core Concepts

### ResourceTypeHandler

Implement this interface to handle updates for a specific xDS resource type:

```go
type ResourceTypeHandler interface {
    // Remove is called when resources are deleted
    Remove(ctx context.Context, resourceNames []string) error

    // Update is called when resources are created or updated
    Update(ctx context.Context, resources []*discovery.Resource) error
}
```

### ResourceTypeStore

Maps xDS TypeURLs to their handlers:

```go
type ResourceTypeStore map[string]ResourceTypeHandler
```

Common TypeURLs from agentgateway:
- `type.googleapis.com/istio.workload.Address` - Workload endpoints and services
- `type.googleapis.com/agentgateway.dev.resource.Resource` - Gateway API routing rules

## Testing

Run tests with `make test` from the repository root, or:

```bash
go test -v ./...
```

## Quick Start Demo

Here's a minimal example that logs xDS updates:

```go
package main

import (
    "context"
    "log/slog"
    "os"
    "strings"

    "github.com/rikatz/notyetanotherenvoybackend/client"
    discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
)

// Simple handler that just logs updates
type LoggingHandler struct {
    name string
}

func (h *LoggingHandler) Update(ctx context.Context, resources []*discovery.Resource) error {
    slog.Info("received update", "type", h.name, "count", len(resources))
    for _, res := range resources {
        slog.Info("resource updated", "name", res.Name)
    }
    return nil
}

func (h *LoggingHandler) Remove(ctx context.Context, resourceNames []string) error {
    slog.Info("received removal", "type", h.name, "resources", resourceNames)
    return nil
}

func main() {
    // Read JWT token (created with: kubectl create token <sa> --audience agentgateway)
    tokenBytes, err := os.ReadFile("xds-token")
    if err != nil {
        panic(err)
    }

    // Create handlers for each resource type
    handlers := client.ResourceTypeStore{
        "type.googleapis.com/istio.workload.Address":             &LoggingHandler{name: "Address"},
        "type.googleapis.com/agentgateway.dev.resource.Resource": &LoggingHandler{name: "Resource"},
    }

    // Create the xDS client
    xdsClient, err := client.NewADSClient(client.Config{
        ServerAddress:        "127.0.0.1:9978",
        Namespace:            "agentgateway-system",
        GatewayName:          "my-gateway",
        Token:                strings.TrimSpace(string(tokenBytes)),
        ResourceTypeHandlers: handlers,
    })
    if err != nil {
        panic(err)
    }

    // Start receiving updates (blocks)
    if err := xdsClient.Start(context.Background()); err != nil {
        panic(err)
    }
}
```

### Running the Demo

1. **Port-forward agentgateway:**
   ```bash
   kubectl port-forward -n agentgateway-system services/agentgateway 9978 &
   ```

2. **Create a ServiceAccount token:**
   ```bash
   # Token namespace and SA name determine which Gateway config you receive
   kubectl create token -n agentgateway-system my-gateway \
     --audience agentgateway --duration 43200s > xds-token
   ```

3. **Run your client:**
   ```bash
   go run main.go
   ```

Expected output:
```
INFO received update type=Address count=5
INFO resource updated name=default/echo-7f8c9d8b4f-abc12
INFO resource updated name=default/echo-7f8c9d8b4f-xyz89
INFO received update type=Resource count=2
INFO resource updated name=agentgateway-system/my-route
```

## Configuration

### ADSClient Config

```go
type Config struct {
    // ServerAddress: agentgateway xDS endpoint (format: "host:port")
    ServerAddress string

    // Namespace: Gateway resource namespace (must match JWT token)
    Namespace string

    // GatewayName: Gateway resource name (must match JWT ServiceAccount name)
    GatewayName string

    // Name: Optional unique client name (auto-generated if empty)
    Name string

    // Token: JWT token with "agentgateway" audience
    Token string

    // ResourceTypeHandlers: Map of TypeURL -> handler
    ResourceTypeHandlers ResourceTypeStore
}
```

### Authentication

The JWT token **must**:
- Have audience `agentgateway`
- Come from a ServiceAccount with name matching `GatewayName`
- Be in the namespace matching `Namespace`

agentgateway uses the token to determine which Gateway's configuration to send.

## Debug Store (debugstore/)

The `debugstore` package provides simple reference implementations:

- **`AddressStore`**: Logs workload endpoints, extracts IP addresses, stores in sync.Map
- **`ResourceStore`**: Logs Gateway API resources, stores in sync.Map
- **`Notify()` method**: Sends notifications via channel (single-consumer, debug only)

These are used by the [examples/](../examples/) TUI and dumper tools for visualizing xDS traffic.

## Reference Implementations

For complete backend examples, see:

- **[nginxbackend/](../nginxbackend/)**: NGINX backend reference implementation (demo/PoC)
  - Translates Address resources into NGINX upstream configuration
  - Converts Resource objects into NGINX routing rules
  - Manages NGINX lifecycle (start, reload, health checks)
  - Thread-safe with proper locking
  - **⚠️ Not production-ready** - for demonstration purposes only

## Resource Types

### Address (istio.workload.Address)

Contains:
- Workload endpoints (pod IPs, health status)
- Service definitions (port mappings)
- Network addresses (IPv4/IPv6 as byte slices - use `netip.AddrFromSlice` to decode)

**Example usage:** Build load balancer backend pools, update upstream servers.

### Resource (agentgateway.dev.resource.Resource)

Contains:
- Gateway API routing configuration
- Listener definitions
- HTTPRoute rules
- Backend references

**Example usage:** Generate proxy routing rules, configure virtual hosts.

## Protocol Details

The client implements xDS Delta Discovery:

1. **Initial subscription**: Sends `DeltaDiscoveryRequest` for each TypeURL
2. **Receive updates**: Server sends resources in `DeltaDiscoveryResponse`
   - `Resources`: New or updated resources
   - `RemovedResources`: Deleted resource names
3. **ACK**: Client acknowledges with matching `ResponseNonce`
4. **Handlers called**: Your `Update()` or `Remove()` methods process changes
5. **Repeat**: Continue receiving incremental updates

### Node Metadata

The client automatically sets:
```go
{
  "NAMESPACE": "<namespace>",
  "GATEWAY": "<gateway-name>",
  "NAME": "<unique-client-name>",
  "role": "<namespace>~<gateway-name>"
}
```

Node ID format: `agentgateway~<name>~<name>.<namespace>~<namespace>.svc.cluster.local`

## Error Handling

- Handler errors are logged but don't stop the stream
- `io.EOF` cleanly ends the stream
- Network errors terminate the client (TODO: add reconnection logic)
- Failed ACKs are logged

## Thread Safety

- The xDS client itself is **not** thread-safe (single receiver goroutine)
- Your handlers **should be** thread-safe if you access shared state
- Use `sync.Map`, mutexes, or channels for concurrent access

## Further Reading

- [agentgateway syncer model](https://github.com/agentgateway/agentgateway/blob/main/controller/pkg/kgateway/agentgatewaysyncer/README.md)
- [xDS Protocol Documentation](https://www.envoyproxy.io/docs/envoy/latest/api-docs/xds_protocol)
- [CLAUDE.md](../CLAUDE.md) - Project-wide implementation notes