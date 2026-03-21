# xDS Debug Examples

This directory contains demo applications that consume the xDS client to visualize and debug communications with the agentgateway control plane. These tools are useful for understanding how agentgateway translates Gateway API resources into xDS configuration.

## Overview

Both examples connect to the agentgateway control plane via xDS Delta Discovery protocol and display real-time updates of:
- **Address resources** (`type.googleapis.com/istio.workload.Address`) - Workload endpoints, services, and health status
- **Resource objects** (`type.googleapis.com/agentgateway.dev.resource.Resource`) - Gateway API routing configuration

These tools use debug-oriented stores that simply log or display messages, unlike the NGINX backend demo which translates them into actual configuration.

## Available Tools

### 1. adsgui - Terminal UI (TUI)

Interactive split-pane TUI application built with [bubbletea](https://github.com/charmbracelet/bubbletea).

**Features:**
- **Left pane**: Chronological list of xDS messages (UPDATE/REMOVE operations)
- **Right pane**: JSON viewer with syntax highlighting
- Navigate messages with arrow keys
- Real-time updates as configuration changes

**Best for:** Interactive exploration, watching configuration changes as you modify Gateway API resources.

### 2. dump - Simple Logger

Lightweight logging-based debugging tool.

**Features:**
- Logs xDS messages to stdout with structured logging (slog)
- Use `-debug` flag to see full JSON payloads
- Simpler output for scripting or CI pipelines

**Best for:** Quick debugging, capturing logs for analysis, non-interactive environments.

## Usage

### Prerequisites

Ensure you have a KinD cluster with agentgateway running:
```bash
make setup
```

### Running the TUI

```bash
make tui
```

This will:
1. Deploy the echo demo application (`make demo-app`)
2. Deploy a debug Gateway with TLS certificates (`make deploy-debug-gateway`)
3. Port-forward the agentgateway service to localhost:9978
4. Launch the interactive TUI

**Keyboard controls:**
- `↑/↓` or `j/k` - Navigate message list
- `PgUp/PgDn` - Scroll JSON viewer
- `q` or `Ctrl+C` - Quit

### Running the Dumper

```bash
make dumper
```

This performs the same setup as `make tui` but runs the simple logging tool instead.

**Output example:**
```
INFO Starting xDS client
DEBUG Address update received resources=3
DEBUG Resource update received resources=1
```

With `-debug` flag (already included in the Makefile), you'll see full JSON:
```json
{
  "address": "10.244.0.5",
  "workloadName": "echo-abc123",
  "namespace": "default",
  ...
}
```

### Running Manually

Both tools can be run directly with `go run`:

**TUI:**
```bash
# Port-forward agentgateway first
kubectl port-forward -n agentgateway-system services/agentgateway 9978 &

# Run TUI
go run ./examples/adsgui/main.go \
  -token-file examples/xds-token \
  -endpoint 127.0.0.1:9978 \
  -gateway debug-gateway \
  -namespace agentgateway-system
```

**Dumper:**
```bash
go run ./examples/dump/main.go \
  -token-file examples/xds-token \
  -endpoint 127.0.0.1:9978 \
  -gateway debug-gateway \
  -namespace agentgateway-system \
  -debug
```

### Common Flags

Both tools support the same flags:

| Flag | Default | Description |
|------|---------|-------------|
| `-endpoint` | `127.0.0.1:9978` | agentgateway xDS server address |
| `-token-file` | (required) | Path to JWT authentication token file |
| `-gateway` | `agentgateway-ricardo1` | Gateway resource name |
| `-namespace` | `agentgateway-system` | Gateway namespace |
| `-debug` | `false` | Enable debug logging (dump only) |

**TUI-specific flags:**
| Flag | Default | Description |
|------|---------|-------------|
| `-theme` | `dracula` | Syntax highlighting theme |

## What to Watch For

### Testing Configuration Changes

While running either tool, try modifying Gateway API resources to see xDS updates in real-time:

**Add a new HTTPRoute:**
```bash
kubectl apply -f - <<EOF
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: test-route
  namespace: agentgateway-system
spec:
  parentRefs:
  - name: debug-gateway
  hostnames:
  - "test.example.com"
  rules:
  - backendRefs:
    - name: echo
      port: 3000
EOF
```

You should see a **Resource UPDATE** message with the new HTTPRoute configuration.

**Scale the backend application:**
```bash
kubectl scale deployment echo --replicas=3
```

You should see **Address UPDATE** messages as new pod workloads are added.

**Delete the route:**
```bash
kubectl delete httproute test-route -n agentgateway-system
```

You should see a **Resource REMOVE** message.

### Understanding the Output

**Address messages** contain:
- Workload information (pod IPs, UIDs, health status)
- Service definitions (ports, target ports)
- Network addresses (IPv4/IPv6)

**Resource messages** contain:
- Gateway API routing rules
- Listener configurations
- Backend references

## Architecture

Both tools use:
- **[client/](../client/)**: Core xDS client using Delta Discovery protocol
- **[client/debugstore/](../client/debugstore/)**: Debug-oriented resource handlers
  - `AddressStore`: Stores and displays Address resources
  - `ResourceStore`: Stores and displays Resource objects

Unlike the NGINX backend demo (`nginxbackend/`), these debug stores don't generate configuration—they simply log or display the raw xDS messages for inspection.

## Cleanup

Stop the tools with `Ctrl+C` or `q` (TUI only).

The port-forward process is automatically killed when using Makefile targets. If running manually, kill it with:
```bash
pkill kubectl
```

To clean up the cluster:
```bash
make cleanup
```

## Troubleshooting

**"Failed to read token file":**
- Ensure you've created the authentication token:
  ```bash
  make GATEWAY_NAME=debug-gateway TOKEN_OUTPUT=examples/xds-token token
  ```

**"Connection refused":**
- Verify port-forward is running:
  ```bash
  kubectl port-forward -n agentgateway-system services/agentgateway 9978
  ```
- Check agentgateway is running:
  ```bash
  kubectl get pods -n agentgateway-system
  ```

**"No messages appearing":**
- Ensure the Gateway resource exists:
  ```bash
  kubectl get gateway debug-gateway -n agentgateway-system
  ```
- Check that the token's namespace and service account match the Gateway
- Deploy a demo application to generate workload updates:
  ```bash
  make demo-app
  ```
