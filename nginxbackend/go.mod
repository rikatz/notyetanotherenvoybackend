module github.com/rikatz/notyetanotherenvoybackend/nginxbackend

go 1.26.1

replace github.com/rikatz/notyetanotherenvoybackend/client => ../client

require (
	github.com/agentgateway/agentgateway/api v0.0.0-20260320213410-b505b09ea5cd
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc
	github.com/envoyproxy/go-control-plane/envoy v1.37.1-0.20260313105501-0df655d8a214
	github.com/golang-jwt/jwt/v5 v5.3.1
	github.com/nginxinc/nginx-go-crossplane v0.4.86
	github.com/rikatz/notyetanotherenvoybackend/client v0.0.0-00010101000000-000000000000
	google.golang.org/protobuf v1.36.11
	istio.io/istio v0.0.0-20260321172513-28c83112477b
)

require (
	github.com/cncf/xds/go v0.0.0-20260202195803-dba9d589def2 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.3.3 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/jstemmer/go-junit-report v1.0.0 // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240409071808-615f978279ca // indirect
	golang.org/x/mod v0.38.0 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	golang.org/x/tools v0.48.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260526163538-3dc84a4a5aaa // indirect
	google.golang.org/grpc v1.83.2 // indirect
)
