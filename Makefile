NOW := $(shell date +%s)
REGISTRY ?= example.com
REPOSITORY ?= nginx-gateway
TAG ?= v$(NOW)
IMAGE_NAME ?= $(REGISTRY)/$(REPOSITORY):$(TAG)
CLUSTER_NAME ?= notyetanotherenvoy
GATEWAY_NAME ?= nginx-gateway
GATEWAY_NAMESPACE ?= agentgateway-system
DEMO_HOSTNAME ?= *.example.com
CERT_OUTPUT ?= certs/
TOKEN_OUTPUT ?= xds-token
TOKEN_DURATION ?= 43200s
ECHO_NAMESPACE ?= default


## Development
.PHONY: build
build:
	CGO_ENABLED=0 go build -o nginx-controller ./nginxbackend/main.go

.PHONY: test
test:
	cd client && go test -v ./...

.PHONY: docker-image
docker-image:
	docker build -t $(IMAGE_NAME) .

.PHONY: docker-load
docker-load: docker-image
	kind load docker-image --name=$(CLUSTER_NAME) $(IMAGE_NAME)


## Demo Helpers
## Setup environment
.PHONY: setup
setup:
	CLUSTER_NAME=$(CLUSTER_NAME) ./setup.sh

.PHONY: generate-certs
generate-certs:
	mkdir -p $(CERT_OUTPUT)
	openssl req -x509 -newkey rsa:4096 -sha256 -days 3650 \
    	-nodes -keyout $(CERT_OUTPUT)/tls.key -out $(CERT_OUTPUT)/tls.crt \
    	-subj "/CN=$(DEMO_HOSTNAME)" \
    	-addext "subjectAltName=DNS:$(DEMO_HOSTNAME)"
	kubectl delete --ignore-not-found secret $(GATEWAY_NAME) -n $(GATEWAY_NAMESPACE)
	kubectl create secret tls $(GATEWAY_NAME) -n $(GATEWAY_NAMESPACE) \
		--cert=$(CERT_OUTPUT)/tls.crt --key=$(CERT_OUTPUT)/tls.key

.PHONY: token
token:
	kubectl create token -n $(GATEWAY_NAMESPACE) $(GATEWAY_NAME) --audience agentgateway --duration $(TOKEN_DURATION) > $(TOKEN_OUTPUT)

## TUI/Dumper targets
.PHONY: deploy-debug-gateway
deploy-debug-gateway:
	$(MAKE) CERT_OUTPUT=examples/certs GATEWAY_NAME=debug-gateway generate-certs
	kubectl apply --server-side -f examples/manifests/gateway.yaml
	$(MAKE) GATEWAY_NAME=debug-gateway TOKEN_OUTPUT=examples/xds-token token

.PHONY: tui
tui: demo-app deploy-debug-gateway
	@echo "Starting port-forward (PID will be saved to /tmp/agw-pf.pid)"
	@kubectl port-forward -n agentgateway-system services/agentgateway 9978 > /dev/null 2>&1 & echo $$! > /tmp/agw-pf.pid
	@sleep 2
	@go run ./examples/adsgui/main.go -token-file examples/xds-token -endpoint 127.0.0.1:9978 -gateway debug-gateway -namespace $(GATEWAY_NAMESPACE); \
	if [ -f /tmp/agw-pf.pid ]; then kill $$(cat /tmp/agw-pf.pid) 2>/dev/null || true; rm -f /tmp/agw-pf.pid; fi

.PHONY: dumper
dumper: demo-app deploy-debug-gateway
	@echo "Starting port-forward (PID will be saved to /tmp/agw-pf.pid)"
	@kubectl port-forward -n agentgateway-system services/agentgateway 9978 > /dev/null 2>&1 & echo $$! > /tmp/agw-pf.pid
	@sleep 2
	@go run ./examples/dump/main.go -debug -token-file examples/xds-token -endpoint 127.0.0.1:9978 -gateway debug-gateway -namespace $(GATEWAY_NAMESPACE); \
	if [ -f /tmp/agw-pf.pid ]; then kill $$(cat /tmp/agw-pf.pid) 2>/dev/null || true; rm -f /tmp/agw-pf.pid; fi

## Demo application - Using NGINX Backend
.PHONY: demo-app
demo-app:
	kubectl delete --ignore-not-found deploy echo -n $(ECHO_NAMESPACE)
	kubectl delete --ignore-not-found service echo -n $(ECHO_NAMESPACE)
	kubectl create deploy echo --image=registry.k8s.io/gateway-api/echo-basic:v20251204-v1.4.1 -n $(ECHO_NAMESPACE)
	kubectl create service clusterip echo --tcp=3000 -n $(ECHO_NAMESPACE)

.PHONY: demo-gateway
demo-gateway:
	export REGISTRY=$(REGISTRY) REPOSITORY=$(REPOSITORY) TAG=$(TAG); \
	envsubst < nginxbackend/manifests/gateway/parameters.yaml | kubectl apply --server-side -f -
	kubectl apply --server-side -f nginxbackend/manifests/gateway/gatewayclass.yaml
	$(MAKE) generate-certs
	kubectl apply --server-side -f nginxbackend/manifests/gateway/gateway.yaml
	kubectl apply --server-side -f nginxbackend/manifests/app/route.yaml

.PHONY: demo
demo: setup docker-load demo-gateway demo-app

.PHONY: cleanup
cleanup:
	kind delete cluster --name=$(CLUSTER_NAME)