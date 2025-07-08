package bazelazblob

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const authHeaderName = "Authorization"

func NewProxy(backend CacheClient) *Proxy {
	return &Proxy{
		backend: backend,
	}
}

func RegisterProxyServer(srv *grpc.Server, p *Proxy) {
	remoteexecution.RegisterActionCacheServer(srv, p)
	remoteexecution.RegisterContentAddressableStorageServer(srv, p)
	remoteexecution.RegisterCapabilitiesServer(srv, p)
	bytestream.RegisterByteStreamServer(srv, p)
}

type CacheClient interface {
	remoteexecution.ActionCacheClient
	remoteexecution.ContentAddressableStorageClient
	remoteexecution.CapabilitiesClient
	// bazel uses bytestream API for large blobs
	bytestream.ByteStreamClient
}

type CacheServer interface {
	remoteexecution.ActionCacheServer
	remoteexecution.ContentAddressableStorageServer
	remoteexecution.CapabilitiesServer
	// bazel uses bytestream API for large blobs
	bytestream.ByteStreamServer
}

var (
	_ CacheServer = (*Proxy)(nil)
)

type Proxy struct {
	backend CacheClient
}

func (p *Proxy) GetCapabilities(ctx context.Context, req *remoteexecution.GetCapabilitiesRequest) (*remoteexecution.ServerCapabilities, error) {
	return p.backend.GetCapabilities(ctx, req)
}

func (p *Proxy) GetActionResult(ctx context.Context, req *remoteexecution.GetActionResultRequest) (*remoteexecution.ActionResult, error) {
	return p.backend.GetActionResult(ctx, req)
}

func (p *Proxy) UpdateActionResult(ctx context.Context, req *remoteexecution.UpdateActionResultRequest) (*remoteexecution.ActionResult, error) {
	return p.backend.UpdateActionResult(ctx, req)
}

func (p *Proxy) FindMissingBlobs(ctx context.Context, req *remoteexecution.FindMissingBlobsRequest) (*remoteexecution.FindMissingBlobsResponse, error) {
	resp, err := p.backend.FindMissingBlobs(ctx, req)
	return resp, err
}

func (p *Proxy) BatchUpdateBlobs(ctx context.Context, req *remoteexecution.BatchUpdateBlobsRequest) (*remoteexecution.BatchUpdateBlobsResponse, error) {
	return p.backend.BatchUpdateBlobs(ctx, req)
}

func (p *Proxy) BatchReadBlobs(ctx context.Context, req *remoteexecution.BatchReadBlobsRequest) (*remoteexecution.BatchReadBlobsResponse, error) {
	return p.backend.BatchReadBlobs(ctx, req)
}

func (p *Proxy) GetTree(req *remoteexecution.GetTreeRequest, srv remoteexecution.ContentAddressableStorage_GetTreeServer) error {
	ctx := srv.Context()
	client, err := p.backend.GetTree(ctx, req)
	if err != nil {
		return err
	}

	for {
		resp, err := client.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := srv.Send(resp); err != nil {
			return err
		}
	}
}

func (p *Proxy) Read(req *bytestream.ReadRequest, srv bytestream.ByteStream_ReadServer) error {
	ctx := srv.Context()
	client, err := p.backend.Read(ctx, req)
	if err != nil {
		return err
	}

	for {
		if err := ctx.Err(); err != nil {
			if err2 := client.CloseSend(); err2 != nil {
				slog.WarnContext(ctx, "failed to close client send stream", "error", err2)
			}
			return fmt.Errorf("context error: %w", err)
		}

		resp, err := client.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		if err := srv.Send(resp); err != nil {
			return err
		}
	}
}

// Write implements ByteStream Write
func (p *Proxy) Write(srv bytestream.ByteStream_WriteServer) error {
	ctx := srv.Context()
	client, err := p.backend.Write(ctx)
	if err != nil {
		return err
	}

	for {
		if err := ctx.Err(); err != nil {
			if err2 := client.CloseSend(); err2 != nil {
				slog.WarnContext(ctx, "failed to close client send stream", "error", err2)
			}
			return fmt.Errorf("context error: %w", err)
		}

		req, err := srv.Recv()
		if err == io.EOF {
			// Client finished sending, close our send side
			if closeErr := client.CloseSend(); closeErr != nil {
				slog.WarnContext(ctx, "failed to close client send stream", "error", closeErr)
			}
			break
		}
		if err != nil {
			return fmt.Errorf("failed to receive request from client: %w", err)
		}
		if err := client.Send(req); err != nil {
			return fmt.Errorf("failed to send request to backend: %w", err)
		}
	}

	resp, err := client.CloseAndRecv()
	if err != nil {
		return err
	}

	return srv.SendAndClose(resp)
}

// QueryWriteStatus implements ByteStream QueryWriteStatus
func (p *Proxy) QueryWriteStatus(ctx context.Context, req *bytestream.QueryWriteStatusRequest) (*bytestream.QueryWriteStatusResponse, error) {
	return p.backend.QueryWriteStatus(ctx, req)
}

type TokenProvider interface {
	GetToken(ctx context.Context, opts policy.TokenRequestOptions) (azcore.AccessToken, error)
}

type cacheTokenProvider struct {
	provider TokenProvider
	mu       sync.Mutex
	tkn      azcore.AccessToken
}

func (p *cacheTokenProvider) GetToken(ctx context.Context, opts policy.TokenRequestOptions) (azcore.AccessToken, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// If the token is still valid, return it
	if p.tkn.ExpiresOn.After(time.Now().Add(5 * time.Minute)) {
		return p.tkn, nil
	}

	// Otherwise, request a new token
	tkn, err := p.provider.GetToken(ctx, opts)
	if err != nil {
		return azcore.AccessToken{}, err
	}

	p.tkn = tkn
	return tkn, nil
}

func UnaryClientAuthInterceptor(p TokenProvider) grpc.UnaryClientInterceptor {
	p = &cacheTokenProvider{
		provider: p,
	}
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if method == remoteexecution.Capabilities_GetCapabilities_FullMethodName {
			// GetCapabilities does not require authentication, so we skip the token retrieval
			return invoker(ctx, method, req, reply, cc, opts...)
		}

		tkn, err := p.GetToken(ctx, tokenOpt)
		if err != nil {
			return err
		}

		md, ok := metadata.FromOutgoingContext(ctx)
		if !ok {
			md = metadata.MD{}
		}

		md.Set(authHeaderName, "Bearer "+tkn.Token)
		ctx = metadata.NewOutgoingContext(ctx, md)

		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

var tokenOpt = policy.TokenRequestOptions{
	Scopes: []string{"https://storage.azure.com/.default"},
}

func StreamClientAuthInterceptor(p TokenProvider) grpc.StreamClientInterceptor {
	p = &cacheTokenProvider{
		provider: p,
	}
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		tkn, err := p.GetToken(ctx, tokenOpt)
		if err != nil {
			return nil, fmt.Errorf("error getting auth token: %w", err)
		}

		md, ok := metadata.FromOutgoingContext(ctx)
		if !ok {
			md = metadata.MD{}
		}

		md.Set(authHeaderName, "Bearer "+tkn.Token)
		ctx = metadata.NewOutgoingContext(ctx, md)

		return streamer(ctx, desc, cc, method, opts...)
	}
}

type ProxyClient struct {
	remoteexecution.ActionCacheClient
	remoteexecution.ContentAddressableStorageClient
	remoteexecution.CapabilitiesClient
	bytestream.ByteStreamClient
}

func ProxyBackendFromGRPC(client grpc.ClientConnInterface) *ProxyClient {
	return &ProxyClient{
		ActionCacheClient:               remoteexecution.NewActionCacheClient(client),
		ContentAddressableStorageClient: remoteexecution.NewContentAddressableStorageClient(client),
		CapabilitiesClient:              remoteexecution.NewCapabilitiesClient(client),
		ByteStreamClient:                bytestream.NewByteStreamClient(client),
	}
}
