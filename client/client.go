// This is the xDS Client for agentgateway syncer
// It relies on the DeltaDiscovery method, but uses the specific
// agentgateway typeURL for discovery of the Workloads and Resources.
// See more at https://github.com/agentgateway/agentgateway/blob/b505b09ea5cd10d560b8728ab7b76873136fd03a/controller/pkg/kgateway/agentgatewaysyncer/README.md
package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"time"

	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

// ResourceTypeHandler defines an interface for dealing with a resource of a
// specific type that arrives via xDS client.
// This handler should cover removal of resource(s) and update (update can be a creation, an update, etc)
type ResourceTypeHandler interface {
	Remove(ctx context.Context, resourceNames []string) error
	Update(ctx context.Context, resources []*discovery.Resource) error
}

// ResourceTypeStore defines a map of typeURLs and the handler
// that should be called for each type.
type ResourceTypeStore map[string]ResourceTypeHandler

type Config struct {
	// ServerAddress defines the xDS address to be used, and MUST be composed of address:port
	ServerAddress string
	// Namespace is the namespace where the gateway is running and should match
	// the JWT "sub" and "kubernetes.io" attributes
	Namespace string
	// GatewayName is the name of the proxy/deployment, and should match the ServiceAccount name
	// and attributes
	GatewayName string
	// Name is a unique name for this client. It must be set to allow the server to
	// send the xDS updates to all the different subscribers on a unique way.
	// If not set, we will generate a random one derived from GatewayName
	Name string
	// Token is the JWT Token to be used when authenticating against agentgateway.
	// It can be refreshed later with the "Refresh()" method in case the token
	// has a watcher for its expiration and renewal. This client DOES NOT refresh
	// the token automatically
	Token string
	// ResourceTypeHandlers specifies a map where the keys are the "TypeURL" to be
	// requested to the discovery server, and the value is a function to be called
	// once this resource is found.
	ResourceTypeHandlers ResourceTypeStore
	// InitialReconnectDelay is the initial delay before reconnection attempts
	// Default: 1 second
	InitialReconnectDelay time.Duration
	// MaxReconnectDelay is the maximum delay between reconnection attempts
	// Default: 30 seconds
	MaxReconnectDelay time.Duration
	// DialTimeout is the timeout for establishing the initial gRPC connection
	// Default: 10 seconds
	DialTimeout time.Duration
}

// ADSClient represents our aggregated discovery service client
type ADSClient struct {
	client               discovery.AggregatedDiscoveryServiceClient
	conn                 *grpc.ClientConn
	token                string
	nodeInfo             *core.Node
	resourceTypeHandlers ResourceTypeStore
	config               Config
}

// NewADSClient creates a new aggregated Discovery Service Client that will be
// used by our backends.
func NewADSClient(cfg Config) (*ADSClient, error) {
	if cfg.Namespace == "" || cfg.GatewayName == "" || cfg.Token == "" || cfg.ServerAddress == "" {
		return nil, errors.New("serveraddress, namespace, gatewayname and token are required")
	}

	_, _, err := net.SplitHostPort(cfg.ServerAddress)
	if err != nil {
		return nil, fmt.Errorf("invalid server address: %w", err)
	}

	// Set default timeouts and delays
	if cfg.InitialReconnectDelay == 0 {
		cfg.InitialReconnectDelay = time.Second
	}
	if cfg.MaxReconnectDelay == 0 {
		cfg.MaxReconnectDelay = 30 * time.Second
	}
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 10 * time.Second
	}

	roleName := fmt.Sprintf("%s~%s", cfg.Namespace, cfg.GatewayName)

	if cfg.Name == "" {
		// Use higher entropy random suffix to avoid collisions
		cfg.Name = fmt.Sprintf("%s-%d", cfg.GatewayName, rand.IntN(1000000))
	}

	meta, err := structpb.NewStruct(map[string]any{
		"NAMESPACE": cfg.Namespace,
		"GATEWAY":   cfg.GatewayName,
		"NAME":      cfg.Name,
		"role":      roleName,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create client metadata: %w", err)
	}

	nodeName := fmt.Sprintf("agentgateway~%s~%s.%s~%s.svc.cluster.local", cfg.Name, cfg.Name, cfg.Namespace, cfg.Namespace)

	node := &core.Node{
		Id:       nodeName,
		Metadata: meta,
	}

	slog.Info("dialing xDS server", "server", cfg.ServerAddress, "timeout", cfg.DialTimeout)
	conn, err := grpc.NewClient(cfg.ServerAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("error dialing xDS server: %w", err)
	}

	return &ADSClient{
		client:               discovery.NewAggregatedDiscoveryServiceClient(conn),
		conn:                 conn,
		token:                cfg.Token,
		nodeInfo:             node,
		resourceTypeHandlers: cfg.ResourceTypeHandlers,
		config:               cfg,
	}, nil
}

// Close closes the gRPC connection
func (a *ADSClient) Close() error {
	if a.conn != nil {
		return a.conn.Close()
	}
	return nil
}

func (a *ADSClient) Start(ctx context.Context) error {
	defer func() {
		if err := a.Close(); err != nil {
			slog.Error("error while closing the connection", "error", err)
		}
	}()

	// Reconnection loop with exponential backoff
	attempt := 0
	reconnectDelay := a.config.InitialReconnectDelay

	for {
		// Check if context is canceled before attempting connection
		select {
		case <-ctx.Done():
			slog.Info("context canceled, stopping client")
			return ctx.Err()
		default:
		}

		if attempt > 0 {
			slog.Info("reconnecting to xDS server", "attempt", attempt, "delay", reconnectDelay)
			select {
			case <-time.After(reconnectDelay):
			case <-ctx.Done():
				return ctx.Err()
			}

			// Exponential backoff
			reconnectDelay *= 2
			if reconnectDelay > a.config.MaxReconnectDelay {
				reconnectDelay = a.config.MaxReconnectDelay
			}
		}

		err := a.runStream(ctx)
		if err == nil || ctx.Err() != nil {
			return err
		}

		// Check if error is permanent or transient
		if isPermanentError(err) {
			return err
		}

		slog.Warn("stream error, will retry", "error", err, "attempt", attempt)
		attempt++
	}
}

// runStream handles a single stream lifecycle
func (a *ADSClient) runStream(ctx context.Context) error {
	ctxSrv := metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+a.token))

	stream, err := a.client.DeltaAggregatedResources(ctxSrv)
	if err != nil {
		return fmt.Errorf("error starting the stream: %w", err)
	}

	// Send subscriptions for all resource types
	for resType := range a.resourceTypeHandlers {
		req := &discovery.DeltaDiscoveryRequest{
			Node:    a.nodeInfo,
			TypeUrl: resType,
		}

		if err := stream.Send(req); err != nil {
			return fmt.Errorf("error subscribing to %s: %w", resType, err)
		}

		slog.Info("listening for AgentGateway Delta updates", "typeURL", resType)
	}

	// Main receive loop
	for {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		resp, err := stream.Recv()
		if err == io.EOF {
			slog.Info("stream ended by server")
			return nil
		}
		if err != nil {
			return fmt.Errorf("stream recv error: %w", err)
		}

		log := slog.With("typeURL", resp.TypeUrl, "nonce", resp.Nonce)
		log.Info("Received Delta Update", "version", resp.SystemVersionInfo,
			"resources", len(resp.Resources), "removed", len(resp.RemovedResources))

		callbackHandler, ok := a.resourceTypeHandlers[resp.TypeUrl]
		if !ok {
			log.Warn("received unknown typeURL, skipping")
			// Still send ACK to acknowledge we received it
			if err := a.sendAck(stream, resp, nil); err != nil {
				return fmt.Errorf("failed to send ACK for unknown type: %w", err)
			}
			continue
		}

		if callbackHandler == nil {
			return fmt.Errorf("handler for typeURL %s is nil, THIS IS A BUG", resp.TypeUrl)
		}

		// Process removals
		var handlerErr error
		if len(resp.RemovedResources) > 0 {
			if err := callbackHandler.Remove(ctx, resp.RemovedResources); err != nil {
				log.Error("error removing resources", "error", err)
				handlerErr = err
			}
		}

		// Process updates
		if len(resp.Resources) > 0 && handlerErr == nil {
			if err := callbackHandler.Update(ctx, resp.Resources); err != nil {
				log.Error("error updating resources", "error", err)
				handlerErr = err
			}
		}

		// Send ACK or NACK based on handler result
		if err := a.sendAck(stream, resp, handlerErr); err != nil {
			return fmt.Errorf("failed to send ACK/NACK: %w", err)
		}
	}
}

// sendAck sends an ACK (or NACK if err is not nil) for the given response
func (a *ADSClient) sendAck(stream discovery.AggregatedDiscoveryService_DeltaAggregatedResourcesClient,
	resp *discovery.DeltaDiscoveryResponse, handlerErr error) error {

	req := &discovery.DeltaDiscoveryRequest{
		Node:          a.nodeInfo,
		TypeUrl:       resp.TypeUrl,
		ResponseNonce: resp.Nonce,
	}

	// If handler failed, send NACK with error details
	if handlerErr != nil {
		req.ErrorDetail = &status.Status{
			Code:    int32(codes.Internal),
			Message: fmt.Sprintf("handler error: %v", handlerErr),
		}
		slog.Warn("sending NACK", "typeURL", resp.TypeUrl, "error", handlerErr)
	}

	return stream.Send(req)
}

// isPermanentError determines if an error should stop reconnection attempts
func isPermanentError(err error) bool {
	if err == nil {
		return false
	}

	// Check for gRPC status codes that are permanent
	if st, ok := grpcstatus.FromError(err); ok {
		switch st.Code() {
		case codes.Unauthenticated, codes.PermissionDenied, codes.InvalidArgument:
			return true
		}
	}

	// Context cancellation is permanent
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	return false
}
