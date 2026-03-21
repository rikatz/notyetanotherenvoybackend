# CLAUDE.md

Instructions for Claude Code when working with this repository.

## ⚠️ Critical: This is Demo/PoC Code

**NEVER suggest using this code in production.** Always mention this is for educational/demo purposes only when discussing the implementation. This applies to ALL code in this repository - client, examples, and nginxbackend.

## Repository Overview

Demo repository showing how to build custom Gateway API backends using [agentgateway](https://github.com/agentgateway/agentgateway) control plane via xDS protocol.

**Structure:**
- `client/` - xDS client library (see [client/README.md](client/README.md))
- `examples/` - Debug tools: TUI and logger (see [examples/README.md](examples/README.md))
- `nginxbackend/` - NGINX backend demo (see [nginxbackend/README.md](nginxbackend/README.md))
- `Makefile` - Primary interface for all operations
- `setup.sh` - KinD cluster setup script

**Key concept:** agentgateway translates Gateway API resources → xDS → your backend consumes and translates to native config.

## Working with This Repository

### Always Use Makefile First

When helping users run, test, or deploy, **always suggest Makefile targets first:**

- `make setup` - Create KinD cluster
- `make demo` - Full demo (build + deploy everything)
- `make tui` / `make dumper` - Run debug tools
- `make token` - Generate auth token
- `make cleanup` - Delete cluster

Only show manual `go run` or `kubectl` commands if users explicitly ask or Makefile doesn't cover their use case.

### Go Workspace

This repo uses Go workspaces with 4 modules: root, client/, examples/, nginxbackend/. If you see module errors, suggest `go work sync`.

### Authentication Pattern

All xDS clients require JWT tokens with:
- Audience: `agentgateway` (NOT "kgateway")
- Namespace + ServiceAccount name → determines which Gateway config is received
- Generate with: `make token` or `kubectl create token -n <ns> <sa> --audience agentgateway`

### Common User Tasks

**Run demo end-to-end:**
```bash
make demo
GATEWAY_IP=$(kubectl get svc nginx-gateway -n agentgateway-system -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
curl -H "Host: foo.example.com" http://$GATEWAY_IP/
```

**Debug xDS traffic:** Use `make tui` (interactive) or `make dumper` (logs)

**Check logs:** `kubectl logs -n agentgateway-system deployment/nginx-gateway -f`

## Code Organization & Key Files

### xDS Client Library (`client/`)

**Core:** [client.go](client/client.go) - Delta Discovery xDS client

**Key interface:** `ResourceTypeHandler` - implement `Update()` and `Remove()` methods for each xDS TypeURL

**Debug stores:** `client/debugstore/` - Simple stores that log updates (used by examples)

See [client/README.md](client/README.md) for API details and code examples.

### Backend Implementations

**Debug tools:** `examples/adsgui` (TUI) and `examples/dump` (logger)
- Use `client/debugstore` to visualize xDS traffic
- Run with `make tui` or `make dumper`

**NGINX backend:** `nginxbackend/`
- Translates Address → NGINX upstreams, Resource → NGINX locations
- Stores in `nginxbackend/pkg/store/` use mutex for thread-safety
- Main entrypoint: `nginxbackend/main.go`

See component READMEs for implementation details.

## Important Patterns & Gotchas

### xDS Protocol Specifics

**Resource TypeURLs:**
- `type.googleapis.com/istio.workload.Address` - Workload endpoints (IPs as byte slices, use `netip.AddrFromSlice`)
- `type.googleapis.com/agentgateway.dev.resource.Resource` - Gateway API routing config

**Protocol flow:**
1. Send `DeltaDiscoveryRequest` per TypeURL
2. Receive updates in `Resources` field, removals in `RemovedResources`
3. ACK with matching `ResponseNonce` and `TypeUrl`
4. Repeat

**Critical:** Service deletion → workload Update (not Remove) with service reference removed. Handle in Update path.

### Implementation Patterns

**When writing resource handlers:**
- Implement `ResourceTypeHandler` interface (Update + Remove methods)
- Use `anypb.UnmarshalTo` to decode resources
- Use `slog` for logging with contextual fields
- Thread safety: use `sync.Mutex` or `sync.Map` for shared state
- Filter unhealthy workloads (check `WorkloadStatus_HEALTHY`)

**Notification channels:** `Notify()` pattern in debugstore is single-consumer only, NOT thread-safe

**Workload lifecycle:** When removed: update ServiceMap, delete from WorkloadMap, regenerate config

### Common Issues

**Module errors:** Run `go work sync`

**Token auth fails:** Ensure audience is `agentgateway` (not "kgateway"), namespace+SA match Gateway

**Port conflicts:** Makefile handles this, but manual runs may need: `kubectl scale deploy -n agentgateway-system <name> --replicas=0`

**Makefile PID files:** Port-forward PIDs saved to `/tmp/agw-pf.pid`, cleaned up automatically

## When Helping Users

1. **Always mention demo/PoC status** when discussing code
2. **Prefer Makefile** over manual commands
3. **Point to READMEs** for detailed docs rather than explaining everything
4. **Reference agentgateway docs** for protocol details: https://github.com/agentgateway/agentgateway/blob/main/controller/pkg/kgateway/agentgatewaysyncer/README.md
5. **For bugs/improvements:** Remind this is demo code, suggest they fork/extend for real use

## Quick Reference

**Makefile targets:** `setup`, `demo`, `tui`, `dumper`, `token`, `cleanup`

**Key files:**
- `client/client.go` - xDS client
- `nginxbackend/main.go` - NGINX backend entrypoint
- `examples/adsgui/main.go`, `examples/dump/main.go` - Debug tools

**Detailed docs:**
- [README.md](README.md) - Overview & quick start
- [client/README.md](client/README.md) - Client API & examples
- [examples/README.md](examples/README.md) - Debug tools usage
- [nginxbackend/README.md](nginxbackend/README.md) - NGINX backend details
