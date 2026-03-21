// This is a very simple NGINX backend for agentgateway model
// THIS SHOULD NOT BE USED IN PRODUCTION!!

package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"slices"
	"strings"

	"github.com/golang-jwt/jwt/v5"

	"github.com/rikatz/notyetanotherenvoybackend/client"
	"github.com/rikatz/notyetanotherenvoybackend/nginxbackend/pkg/nginx"
	"github.com/rikatz/notyetanotherenvoybackend/nginxbackend/pkg/probe"
	"github.com/rikatz/notyetanotherenvoybackend/nginxbackend/pkg/store"
)

var (
	agentgatewayEndpoint string
	tokenFile            string
	debug                bool
)

func main() {
	flag.StringVar(&agentgatewayEndpoint, "endpoint", "127.0.0.1:9978", "endpoint controller to connect to")
	flag.StringVar(&tokenFile, "token-file", "", "file containing the token")
	flag.BoolVar(&debug, "debug", false, "put in debug mode")
	flag.Parse()

	opts := &slog.HandlerOptions{}
	if debug {
		opts.Level = slog.LevelDebug
	}
	handler := slog.NewTextHandler(os.Stdout, opts)
	slog.SetDefault(slog.New(handler))

	tokenBytes, err := os.ReadFile(tokenFile)
	if err != nil {
		fatal("Failed to read token file", err)
	}

	gatewayname, namespace, err := gatewayAndNamespaceFromJWT(string(tokenBytes))
	if err != nil {
		fatal("Failed to decode the service token", err)
	}

	nginxMgr, err := nginx.NewManager("", "")
	if err != nil {
		fatal("Failed to create NGINX manager", err)
	}

	// Create context for managing NGINX and xDS client lifecycle
	ctx := context.Background()

	// Start NGINX before initializing stores and xDS client
	slog.Info("Starting NGINX process")
	if err := nginxMgr.Start(ctx); err != nil {
		fatal("Failed to start NGINX", err)
	}
	slog.Info("NGINX started successfully")

	// Initialize the address store
	addressStore, err := store.NewAddressStore(nginxMgr)
	if err != nil {
		fatal("Failed to create address store", err)
	}

	// Initialize the resource store (needs reference to addressStore for upstream coordination)
	resourceStore, err := store.NewResourceStore(nginxMgr, addressStore)
	if err != nil {
		fatal("Failed to create resource store", err)
	}

	resourceHandlerStore := client.ResourceTypeStore{
		"type.googleapis.com/istio.workload.Address":             addressStore,
		"type.googleapis.com/agentgateway.dev.resource.Resource": resourceStore,
	}

	token := strings.TrimSpace(string(tokenBytes))

	client, err := client.NewADSClient(client.Config{
		ServerAddress:        agentgatewayEndpoint,
		Namespace:            namespace,
		GatewayName:          gatewayname,
		Token:                token,
		ResourceTypeHandlers: resourceHandlerStore,
	})
	if err != nil {
		fatal("error initiating the client", err)
	}

	slog.Info("starting probe server")
	go probe.StartProbeServer()
	slog.Info("Starting xDS client")
	if err := client.Start(ctx); err != nil {
		fatal("error running client", err)
	}
}

func fatal(msg string, err error) {
	slog.Error(msg, "error", err)
	os.Exit(1)
}

func gatewayAndNamespaceFromJWT(tokenString string) (string, string, error) {
	type K8sMetadata struct {
		Namespace      string `json:"namespace"`
		ServiceAccount struct {
			Name string `json:"name"`
		} `json:"serviceaccount"`
	}

	type GatewayClaims struct {
		K8sDetails K8sMetadata `json:"kubernetes.io"`
		jwt.RegisteredClaims
	}

	var claims GatewayClaims

	_, _, err := jwt.NewParser().ParseUnverified(tokenString, &claims)
	if err != nil {
		return "", "", err
	}

	if !slices.Contains(claims.Audience, "agentgateway") {
		return "", "", errors.New("JWT token does not contain 'agentgateway' audience")
	}

	namespace := claims.K8sDetails.Namespace
	gatewayName := claims.K8sDetails.ServiceAccount.Name

	if namespace == "" || gatewayName == "" {
		return "", "", errors.New("jwt token does not contain the right namespace and SA name")
	}

	return gatewayName, namespace, nil

}
