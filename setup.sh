#!/usr/bin/env bash
#

CLUSTER_NAME="${CLUSTER_NAME:-notyetanotherenvoy}"
GWAPI_VERSION="${GWAPI_VERSION:-v1.5.0}"
METALLB_VERSION="${METALLB_VERSION:-v0.15.3}"
AGENTGATEWAY_VERSION="${AGENTGATEWAY_VERSION:-v1.0.0}"  

create_kind() {
  kind create cluster --name="${CLUSTER_NAME}"
}

deploy_crds() {
  kubectl get crd gateways.gateway.networking.k8s.io &> /dev/null || \
  kubectl apply --server-side -f https://github.com/kubernetes-sigs/gateway-api/releases/download/"${GWAPI_VERSION}"/experimental-install.yaml
}

deploy_metallb() {
  kubectl apply -f https://raw.githubusercontent.com/metallb/metallb/"${METALLB_VERSION}"/config/manifests/metallb-native.yaml
  kubectl wait --timeout=5m deploy -n metallb-system controller --for=condition=Available
  kubectl apply -f - <<EOF
apiVersion: metallb.io/v1beta1
kind: IPAddressPool
metadata:
  namespace: metallb-system
  name: kube-services
spec:
  addresses:
  - 172.18.200.100-172.18.200.150
---
apiVersion: metallb.io/v1beta1
kind: L2Advertisement
metadata:
  name: kube-services
  namespace: metallb-system
spec:
  ipAddressPools:
  - kube-services
EOF
}

create_kind
deploy_metallb
deploy_crds

helm upgrade -i --create-namespace \
  --namespace agentgateway-system \
  --version "${AGENTGATEWAY_VERSION}" agentgateway-crds oci://cr.agentgateway.dev/charts/agentgateway-crds

helm upgrade -i agentgateway oci://cr.agentgateway.dev/charts/agentgateway \
  --namespace agentgateway-system \
  --version "${AGENTGATEWAY_VERSION}" \
  --set controller.image.pullPolicy=Always \
  --set controller.extraEnv.KGW_ENABLE_GATEWAY_API_EXPERIMENTAL_FEATURES=true

kubectl wait --timeout=5m deploy -n agentgateway-system agentgateway --for=condition=Available
