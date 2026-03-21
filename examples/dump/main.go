package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"strings"

	"github.com/rikatz/notyetanotherenvoybackend/client"
	"github.com/rikatz/notyetanotherenvoybackend/client/debugstore"
)

var (
	agentgatewayEndpoint string
	tokenFile            string
	namespace            string
	gatewayname          string
	debug                bool
)

func main() {
	flag.StringVar(&agentgatewayEndpoint, "endpoint", "127.0.0.1:9978", "endpoint controller to connect to")
	flag.StringVar(&tokenFile, "token-file", "", "file containing the token")
	flag.StringVar(&namespace, "namespace", "agentgateway-system", "namespace where the gateway is running")
	flag.StringVar(&gatewayname, "gateway", "agentgateway-ricardo1", "gateway name")
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

	resourceHandlerStore := client.ResourceTypeStore{
		"type.googleapis.com/istio.workload.Address":             debugstore.NewAddressStore(),
		"type.googleapis.com/agentgateway.dev.resource.Resource": debugstore.NewResourceStore(),
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

	if err := client.Start(context.Background()); err != nil {
		fatal("error running client", err)
	}
}

func fatal(msg string, err error) {
	slog.Error(msg, "error", err)
	os.Exit(1)
}
