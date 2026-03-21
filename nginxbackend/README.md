# NGINX Backend for Gateway API

> **⚠️ NOT PRODUCTION READY**: This is a demo/PoC implementation for educational purposes only. Do NOT use in production.

This directory contains a reference implementation of a custom Gateway API backend using **NGINX** as the data plane, powered by the [agentgateway](https://github.com/agentgateway/agentgateway) control plane.

## Project Context

This is part of a larger repository that demonstrates how to build custom Gateway API implementations by consuming xDS (Discovery Service) resources from the agentgateway control plane and translating them into backend-specific configurations.

The agentgateway control plane translates Kubernetes Gateway API resources (Gateway, HTTPRoute, etc.) into a specific xDS model that custom backends can consume. Instead of deploying Envoy or another standard proxy, this implementation uses **NGINX** as the data plane, automatically configuring it based on Gateway API resources.

## What This Implementation Does

The NGINX backend:

1. **Connects to agentgateway control plane** via xDS Delta Discovery protocol
2. **Consumes two primary resource types**:
   - `type.googleapis.com/istio.workload.Address` - Workload endpoints (pods) and services with health status
   - `type.googleapis.com/agentgateway.dev.resource.Resource` - Gateway API routing configuration
3. **Translates xDS resources into NGINX configuration** dynamically
4. **Manages NGINX lifecycle** - starts, configures, and reloads NGINX based on configuration changes
5. **Provides health probes** for Kubernetes readiness/liveness checks

### Key Components

- **[main.go](main.go)**: Entry point that initializes the xDS client, NGINX manager, and resource stores
- **[pkg/nginx/](pkg/nginx/)**: NGINX process manager and configuration generator
- **[pkg/store/](pkg/store/)**: Resource handlers for Address and Resource types
  - `AddressStore`: Tracks workload endpoints and services with health filtering
  - `ResourceStore`: Handles Gateway API routing rules
- **[pkg/probe/](pkg/probe/)**: HTTP health check server
- **[manifests/](manifests/)**: Kubernetes manifests for deploying the NGINX gateway

## Usage

### Prerequisites

Run the setup script from the repository root to create a KinD cluster with agentgateway:
```bash
make setup
```

This creates a cluster named `notyetanotherenvoy` by default. You can customize with `CLUSTER_NAME=myname make setup`.

### Quick Start - Full Demo

To run the complete NGINX backend demo:

```bash
make demo
```

This runs the following steps:
1. Sets up the KinD cluster with agentgateway (`make setup`)
2. Builds and loads the NGINX gateway Docker image (`make docker-load`)
3. Deploys the NGINX gateway with TLS certificates (`make demo-gateway`)
4. Deploys a demo echo application (`make demo-app`)

### Individual Makefile Targets

**Build the controller:**
```bash
make build
```
Compiles the nginx-controller binary to `nginx-controller`.

**Build Docker image:**
```bash
make docker-image
```
Builds the Docker image. Customize with environment variables:
- `REGISTRY`: Docker registry (default: example.com)
- `REPOSITORY`: Repository name (default: nginx-gateway)
- `TAG`: Image tag (default: auto-generated timestamp)

**Load image into KinD:**
```bash
make docker-load
```
Builds and loads the Docker image into the KinD cluster.

**Generate TLS certificates:**
```bash
make generate-certs
```
Creates TLS certificates for HTTPS listeners and stores them as a Kubernetes secret.
- Customize with `DEMO_HOSTNAME` (default: *.example.com)
- Customize with `GATEWAY_NAME` (default: nginx-gateway)

**Create authentication token:**
```bash
make token
```
Creates a ServiceAccount token for authenticating with the agentgateway control plane.
Saves to `xds-token` by default.

**Deploy NGINX gateway:**
```bash
make demo-gateway
```
Deploys the NGINX gateway with:
- GatewayClass pointing to the NGINX backend image
- Gateway resource with HTTP/HTTPS listeners
- HTTPRoute for routing traffic
- TLS certificates

**Deploy demo application:**
```bash
make demo-app
```
Deploys a simple echo server application as the backend service.

**Cleanup:**
```bash
make cleanup
```
Deletes the KinD cluster.

### Customization

Environment variables you can set:
- `CLUSTER_NAME`: KinD cluster name (default: notyetanotherenvoy)
- `GATEWAY_NAME`: Gateway resource name (default: nginx-gateway)
- `GATEWAY_NAMESPACE`: Gateway namespace (default: agentgateway-system)
- `DEMO_HOSTNAME`: TLS certificate hostname (default: *.example.com)
- `REGISTRY`, `REPOSITORY`, `TAG`: Docker image coordinates

Example:
```bash
CLUSTER_NAME=mytest GATEWAY_NAME=my-nginx make demo
```

## Testing the Deployment

After running `make demo`, the NGINX gateway is deployed with MetalLB providing external IP addresses.

### Get the Gateway IP Address

The Gateway service uses MetalLB LoadBalancer:

```bash
kubectl get gateway nginx-gateway -n agentgateway-system
```

Or get the service directly:

```bash
kubectl get svc nginx-gateway -n agentgateway-system
```

Look for the `EXTERNAL-IP` field. Example output:
```
NAME            CLASS   ADDRESS         PROGRAMMED   AGE
nginx-gateway   nginx   172.18.255.10   True         2m
```

### Test HTTP Traffic

Once you have the IP address, test the HTTP endpoint:

```bash
# Replace with your actual gateway IP
GATEWAY_IP=$(kubectl get svc nginx-gateway -n agentgateway-system -o jsonpath='{.status.loadBalancer.ingress[0].ip}')

# Test HTTP request
curl -H "Host: foo.example.com" http://$GATEWAY_IP/
```

Expected response from the echo server:
```
Request served by echo-<pod-id>

HTTP/1.1 GET /

Host: foo.example.com
...
```

### Test HTTPS Traffic

For HTTPS, you'll need to accept the self-signed certificate:

```bash
curl -k -H "Host: foo.example.com" https://$GATEWAY_IP/
```

Or with verbose output to see TLS details:

```bash
curl -kv -H "Host: foo.example.com" https://$GATEWAY_IP/
```

### Check Logs

**NGINX gateway controller logs:**

```bash
kubectl logs -n agentgateway-system deployment/nginx-gateway -f
```

This shows:
- xDS connection status
- Resource updates from agentgateway
- NGINX configuration changes
- Errors and warnings

**NGINX access logs (from within the pod):**

```bash
kubectl exec -n agentgateway-system deployment/nginx-gateway -- tail -f /var/log/nginx/access.log
```

**NGINX error logs:**

```bash
kubectl exec -n agentgateway-system deployment/nginx-gateway -- tail -f /var/log/nginx/error.log
```

**Echo application logs:**

```bash
kubectl logs deployment/echo -f
```

### Debugging

**Check Gateway status:**

```bash
kubectl describe gateway nginx-gateway -n agentgateway-system
```

**Check HTTPRoute status:**

```bash
kubectl get httproute -n agentgateway-system
kubectl describe httproute foo -n agentgateway-system
```

**Check NGINX configuration:**

```bash
kubectl exec -n agentgateway-system deployment/nginx-gateway -- cat /etc/nginx/nginx.conf
```

**Check workload endpoints:**

```bash
kubectl exec -n agentgateway-system deployment/nginx-gateway -- cat /etc/nginx/upstreams.conf
```

## Development

### Running Locally

To test the controller locally (outside Kubernetes):

1. Port-forward the agentgateway service:
   ```bash
   kubectl port-forward -n agentgateway-system services/agentgateway 9978 &
   ```

2. Create a token:
   ```bash
   make token
   ```

3. Run the controller:
   ```bash
   go run ./main.go -token-file ../xds-token -endpoint 127.0.0.1:9978 -debug
   ```

The controller will:
- Extract gateway name and namespace from the JWT token
- Connect to the agentgateway control plane
- Start NGINX and configure it based on xDS updates

### Important Notes

- The JWT token **must contain the `agentgateway` audience**
- The token's namespace and serviceaccount name determine which Gateway configuration is received
- Before running custom clients, scale down the official gateway deployment to avoid conflicts:
  ```bash
  kubectl scale deploy -n agentgateway-system nginx-gateway --replicas=0
  ```

## Architecture Details

For detailed information about the xDS client, resource types, and implementation patterns, see the main [CLAUDE.md](../CLAUDE.md) file.

**⚠️ NOT PRODUCTION READY**: This is a demonstration and proof-of-concept implementation for educational purposes only. This code should **NOT** be used in production environments. It lacks security hardening, comprehensive error handling, and has not been tested at scale.
