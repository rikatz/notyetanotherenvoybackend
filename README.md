# Custom Gateway API Backend - Demo/PoC

> **⚠️ NOT PRODUCTION READY**: This is a demonstration and proof-of-concept repository showing how to consume the [agentgateway](https://github.com/agentgateway/agentgateway) control plane to build custom Gateway API backends. This code is for educational purposes only and should NOT be used in production environments.

This repository demonstrates how to build custom Gateway API implementations using the agentgateway control plane. Instead of deploying Envoy or another standard proxy, you can consume xDS (Discovery Service) resources and translate them into configurations for any data plane technology.

**What's included:**
- **NGINX backend** reference implementation (demo/PoC)
- **xDS client library** for connecting to agentgateway
- **Debug tools** (TUI and logger) for visualizing xDS traffic
- Complete demo environment with KinD cluster setup

## Repository Structure

### [client/](client/)
Core xDS client library using Delta Discovery protocol. Provides the foundation for building custom backends.

**Key features:**
- JWT authentication with Kubernetes ServiceAccount tokens
- Pluggable resource handlers
- Support for Address (workload endpoints) and Resource (routing rules) types

→ See [client/README.md](client/README.md) for API documentation and examples.

### [examples/](examples/)
Debug tools for exploring xDS communications:
- **adsgui** - Interactive TUI with syntax-highlighted JSON viewer
- **dump** - Simple logging-based debugger

→ See [examples/README.md](examples/README.md) for usage instructions.

### [nginxbackend/](nginxbackend/)
Reference NGINX backend implementation (demo/PoC). Translates xDS resources into NGINX configuration and manages the NGINX lifecycle.

**Demonstrates:**
- Address-to-upstream translation (workload endpoints → NGINX backends)
- Resource-to-routing translation (HTTPRoute → NGINX locations)
- Configuration reload and health checks

**⚠️ Not production-ready** - For demonstration purposes only.

→ See [nginxbackend/README.md](nginxbackend/README.md) for detailed documentation.

## Quick Start: NGINX Demo

Deploy a complete Gateway API setup with NGINX as the data plane:

**1. Create the cluster:**
```bash
make setup
```

**2. Deploy everything (app + NGINX gateway):**
```bash
make demo
```

This builds the NGINX gateway image, loads it into KinD, and deploys:
- Echo backend application
- NGINX Gateway with HTTP/HTTPS listeners
- HTTPRoute routing traffic to the echo service

**3. Test the deployment:**
```bash
# Get the gateway IP
GATEWAY_IP=$(kubectl get svc nginx-gateway -n agentgateway-system -o jsonpath='{.status.loadBalancer.ingress[0].ip}')

# Send a test request
curl -H "Host: foo.example.com" http://$GATEWAY_IP/
```

**4. Check the logs:**
```bash
# Controller logs (xDS updates, NGINX reloads)
kubectl logs -n agentgateway-system deployment/nginx-gateway -f

# NGINX access logs
kubectl exec -n agentgateway-system deployment/nginx-gateway -- tail -f /var/log/nginx/access.log
```

**Cleanup:**
```bash
make cleanup
```

## Using the xDS Client

Here's a minimal example of building your own backend:

```go
package main

import (
    "context"
    "github.com/rikatz/notyetanotherenvoybackend/client"
    discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
)

// Implement handlers for xDS resources
type MyHandler struct{}

func (h *MyHandler) Update(ctx context.Context, resources []*discovery.Resource) error {
    // Process new/updated resources
    return nil
}

func (h *MyHandler) Remove(ctx context.Context, resourceNames []string) error {
    // Handle deletions
    return nil
}

func main() {
    handlers := client.ResourceTypeStore{
        "type.googleapis.com/istio.workload.Address":             &MyHandler{},
        "type.googleapis.com/agentgateway.dev.resource.Resource": &MyHandler{},
    }

    client, _ := client.NewADSClient(client.Config{
        ServerAddress:        "127.0.0.1:9978",
        Namespace:            "agentgateway-system",
        GatewayName:          "my-gateway",
        Token:                "<jwt-token>",
        ResourceTypeHandlers: handlers,
    })

    client.Start(context.Background())
}
```

→ See [client/README.md](client/README.md) for complete examples and API documentation.

## Testing

Run tests with:
```bash
make test
```

## How It Works

1. **agentgateway control plane** watches Gateway API resources (Gateway, HTTPRoute, etc.)
2. **Control plane translates** them into xDS resources (Address, Resource)
3. **Your backend connects** via xDS client with JWT authentication
4. **Handlers receive updates** when configuration changes
5. **Backend translates** xDS into native configuration (NGINX, HAProxy, etc.)

```
Gateway API Resources → agentgateway → xDS Protocol → Your Backend → Data Plane
```

## Authentication

Backends authenticate using Kubernetes ServiceAccount tokens:

```bash
# Create token with agentgateway audience
kubectl create token -n agentgateway-system <gateway-name> \
  --audience agentgateway --duration 43200s > xds-token
```

The token's namespace and ServiceAccount name determine which Gateway's configuration you receive.

## Learn More

- [agentgateway syncer model](https://github.com/agentgateway/agentgateway/blob/main/controller/pkg/kgateway/agentgatewaysyncer/README.md)
- [xDS Protocol](https://www.envoyproxy.io/docs/envoy/latest/api-docs/xds_protocol)
- [CLAUDE.md](CLAUDE.md) - Development guide and implementation notes

## Environment Setup

The `setup.sh` script creates a KinD cluster with:
- Gateway API CRDs
- MetalLB for LoadBalancer support
- agentgateway control plane

Customize with environment variables:
- `CLUSTER_NAME` (default: notyetanotherenvoy)
- `GWAPI_VERSION` (default: v1.4.1)
- `METALLB_VERSION` (default: v0.15.3)
- `AGW_VERSION` (default: v2.2.0-main)

## License

See [LICENSE](LICENSE) for details

These are the files used to create my own Gateway API implementation. You can reuse 
to your own implementation, feel free to hack around it.

Some notes:

* Originally this implementation was done using kgateway, but the kgateway and agentgateway team 
decided to split the controlplane, and agentgateway now has its own control plane capable of 
having different backends
* The model is very well explained at [agentgateway repo](https://github.com/agentgateway/agentgateway/blob/b505b09ea5cd10d560b8728ab7b76873136fd03a/controller/pkg/kgateway/agentgatewaysyncer/README.md)

## No-op backend implementation

The no-op backend implementation will guide you through creating example manifests, 
connecting to the control plane and dumping the configuration.

This will demonstrate:

* How to get an authentication token
* How to connect to agentgateway control plane (and create sample manifests)
* How agentgateway represents the configuration sent via xDS

### Creating a sample cluster

You can create your own agentgateway environment using `./setup.sh` and this 
may get you a KinD cluster with agentgateway controller installed and running.

### Initial discoveries and getting an authentication token

Agentgateway is a Gateway API implementation. As so, we can create a new Gateway resource with:
```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata:
  name: agentgateway-ricardo1
  namespace: agentgateway-system
spec:
  gatewayClassName: agentgateway
  listeners:
  - protocol: HTTP
    port: 80
    name: http
    allowedRoutes:
      namespaces:
        from: All
```

When this manifest is applied, a new agentgateway proxy will be deployed (given
the `agentgateway` class). **This proxy will connect to control plane
at http://agentgateway.agentgateway-system.svc.cluster.local:9978**.

Before deploying the proxy, agentgateway creates a [ServiceAccount](https://kubernetes.io/docs/concepts/security/service-accounts/) on the same namespace as the 
Gateway, and with the same name. This service account has NO ACCESS to Kubernetes 
API, and it will be used to authenticate the workload against agentgateway controller.

We can create a new Token for this Service Account, and **IT MUST CONTAIN the agentgateway audience!**. You can control the duration if you need more or less time for the demo:

```shell
kubectl create token -n agentgateway-system agentgateway-ricardo1 --audience agentgateway --duration 43200s
eyJhbGciOiJSUzI1NiIsImtpZCI6IkxxVmhJc010ZGFpTE9JeXBaMGppWW43YUFKcVNTUC16ajNFRnVfR196LXMifQ......
```

**Save this authentication token on some file like `xds-token`.**

Decoding this JWT token may return you something like:
```json
{
  "header": {
    "alg": "RS256",
    "kid": "LqVhIsMtdaiLOIypZ0jiYn7aAJqSSP-zj3EFu_G_z-s"
  },
  "payload": {
    "aud": [
      "agentgateway"
    ],
    "exp": 1766804091,
    "iat": 1766760891,
    "iss": "https://kubernetes.default.svc.cluster.local",
    "jti": "470ff54b-4b0e-4a66-8d5f-d171220d1c51",
    "kubernetes.io": {
      "namespace": "agentgateway-system",
      "serviceaccount": {
        "name": "agentgateway-ricardo1",
        "uid": "5cc1cd3b-777b-45a4-9a2e-5b775c3cc841"
      }
    },
    "nbf": 1766760891,
    "sub": "system:serviceaccount:agentgateway-system:agentgateway-ricardo1"
  }
}
```

It is also important to note that the namespace and service account name are used
by agentgateway to decide which information should be sent (aka what Gateway is asking for
its own configuration).

### Connecting to control plane and debugging some communications

Because deploying a Gateway actually creates a Deployment, we want first to scale the
deployment to zero, to avoid concurrency on getting information with existing agentgateway
Pod. So: `kubectl scale deploy -n agentgateway-system agentgateway-ricardo1 --replicas=0`

Then, we can run a TUI program to debug the communications:

```shell
kubectl port-forward -n agentgateway-system services/agentgateway 9978 &
go run ./cmd/adsgui/main.go -token-file $(pwd)/xds-token -endpoint 127.0.0.1:9978 -gateway agentgateway-ricardo1 -namespace agentgateway-system
```

If you just want to fully dump the communication as log messages, use the program on 
`cmd/dump`, the flags are the same. Pass `-debug` on it to dump all of the JSON messages.



