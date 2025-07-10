package bazelazblob

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/bazelbuild/remote-apis/build/bazel/semver"
	"golang.org/x/sync/semaphore"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	digestFunc      = remoteexecution.DigestFunction_SHA256
	hashKeyLength   = 64
	emptySHA256Hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

var (
	digestFuncString = strings.ToLower(digestFunc.String())
	emptyBuf         = []byte{}
)

type AzureBlobAccountConfig struct {
	// ServuceURL is the Azure Blob Storage service URL, e.g. "https://<account>.blob.core.windows.net"
	ServiceURL string
	// Container is the name of the Azure Blob Storage container
	Container string
	// Prefix is an optional path prefix for blobs in the container
	Prefix string

	// azClientOpts is used to pass additional options to the Azure Blob Storage client.
	// This is used by tests to inject custom options such as custom HTTP client with
	// its own CA pool for self-signed certificates.
	azClientOpts []func(*azblob.ClientOptions)
}

func (cfg AzureBlobAccountConfig) Equal(other AzureBlobAccountConfig) bool {
	return cfg.ServiceURL == other.ServiceURL &&
		cfg.Container == other.Container &&
		cfg.Prefix == other.Prefix
}

// AzureBlobServerConfig holds the configuration for the Azure Blob Storage server
// This supports having separate configurations for Content Addressable Storage (CAS) and Action Cache (AC)
// which enables you to have different performance/access characteristics for each (each one in hot storage and one in cold, for example).
type AzureBlobServerConfig struct {
	// CAS is the Azure blobn store configuration for Bazel Content Addressable Storage
	CAS AzureBlobAccountConfig
	// AC is the Azure blob store configuration for Bazel Action Cache
	AC AzureBlobAccountConfig
}

// AzureBlobServer implements the Bazel Remote Execution API using Azure Blob Storage
// Only the things required for implementing remote caching are implemented.
type AzureBlobServer struct {
	cfg AzureBlobServerConfig

	// client is used for testing purposes only.
	// When client is set, `createBlobClient` will return that instead of creating one from
	// the metadata in the request.
	client BlobClient
}

func RegisterAzureBlobServer(srv *grpc.Server, azb *AzureBlobServer) {
	remoteexecution.RegisterActionCacheServer(srv, azb)
	remoteexecution.RegisterContentAddressableStorageServer(srv, azb)
	remoteexecution.RegisterCapabilitiesServer(srv, azb)
	bytestream.RegisterByteStreamServer(srv, azb)
}

// NewAzureBlobServer creates a new Azure Blob Storage server
func NewAzureBlobServer(cfg AzureBlobServerConfig) (*AzureBlobServer, error) {
	if cfg.CAS.ServiceURL == "" {
		return nil, fmt.Errorf("CAS ServiceURL is required")
	}
	if cfg.CAS.Container == "" {
		return nil, fmt.Errorf("CAS Container is required")
	}
	if cfg.AC.ServiceURL == "" {
		return nil, fmt.Errorf("AC ServiceURL is required")
	}
	if cfg.AC.Container == "" {
		return nil, fmt.Errorf("AC Container is required")
	}
	if !strings.Contains(cfg.CAS.ServiceURL, "://") {
		// Assume storage account name if no scheme is provided
		cfg.CAS.ServiceURL = fmt.Sprintf("https://%s.blob.core.windows.net", cfg.CAS.ServiceURL)
	}
	if !strings.Contains(cfg.AC.ServiceURL, "://") {
		// Assume storage account name if no scheme is provided
		cfg.AC.ServiceURL = fmt.Sprintf("https://%s.blob.core.windows.net", cfg.AC.ServiceURL)
	}

	if cfg.CAS.Equal(cfg.AC) && cfg.CAS.Prefix == "" {
		// If CAS and AC are the same storage account, use default prefixes
		cfg.CAS.Prefix = "cas"
		cfg.AC.Prefix = "ac"
	}
	return &AzureBlobServer{
		cfg: cfg,
	}, nil
}

// SetClient allows setting a custom BlobClient to use
// It is only safe to call this before any requests are made to the server.
func (s *AzureBlobServer) SetClient(client BlobClient) {
	s.client = client
}

func (s *AzureBlobServer) Config() AzureBlobServerConfig {
	return s.cfg
}

// staticTokenCredential implements azcore.TokenCredential with a static token
type staticTokenCredential struct {
	token string
}

func (c *staticTokenCredential) GetToken(ctx context.Context, options policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{
		Token: c.token,
		// Note: We don't have expiry info from the gRPC metadata, so we assume it's valid
		// In a real implementation, you might want to parse the JWT to get the expiry
	}, nil
}

func validateDigest(d *remoteexecution.Digest) error {
	if d == nil {
		return status.Error(codes.InvalidArgument, "digest cannot be nil")
	}
	if d.Hash == "" {
		return status.Error(codes.InvalidArgument, "digest hash cannot be empty")
	}
	if d.SizeBytes < 0 {
		return status.Error(codes.InvalidArgument, "digest size cannot be negative")
	}
	if len(d.Hash) != hashKeyLength {
		return status.Errorf(codes.InvalidArgument, "digest hash must be %d characters long", hashKeyLength)
	}

	if d.SizeBytes == 0 {
		if d.Hash == emptySHA256Hash {
			// Special case for empty digest
			return nil
		}
		return status.Error(codes.InvalidArgument, "invalid size for non-empty digest")
	}

	return nil
}

// createBlobClient creates an Azure Blob client using the token from gRPC metadata
func (s *AzureBlobServer) createBlobClient(ctx context.Context, cfg *AzureBlobAccountConfig) (BlobClient, error) {
	if s.client != nil {
		return s.client, nil
	}

	// Extract auth token from gRPC metadata
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "no metadata found")
	}

	authHeaders := md.Get(authHeaderName)
	if len(authHeaders) == 0 {
		return nil, status.Error(codes.Unauthenticated, "no authorization header")
	}

	authHeader := authHeaders[0]
	if !strings.HasPrefix(authHeader, "Bearer ") {
		return nil, status.Error(codes.Unauthenticated, "invalid authorization header format")
	}

	token := strings.TrimPrefix(authHeader, "Bearer ")
	if len(token) == 0 {
		return nil, status.Error(codes.Unauthenticated, "empty token in authorization header")
	}

	// Create token credential from the extracted token
	tokenCredential := &staticTokenCredential{token: token}

	// Create blob client
	var opt *azblob.ClientOptions
	if len(cfg.azClientOpts) > 0 {
		opt = &azblob.ClientOptions{}
		for _, o := range cfg.azClientOpts {
			o(opt)
		}
	}
	client, err := azblob.NewClient(cfg.ServiceURL, tokenCredential, opt)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create blob client: %v", err)
	}

	return &azblobClient{az: client}, nil
}

func isBlobNotFoundError(err error) bool {
	if err == nil {
		return false
	}

	if responseErr, ok := err.(*azcore.ResponseError); ok {
		return responseErr.StatusCode == 404
	}

	return strings.Contains(err.Error(), "BlobNotFound") ||
		strings.Contains(err.Error(), "404")
}

func (s *AzureBlobServer) GetActionResult(ctx context.Context, req *remoteexecution.GetActionResultRequest) (*remoteexecution.ActionResult, error) {
	client, err := s.createBlobClient(ctx, &s.cfg.AC)
	if err != nil {
		return nil, err
	}

	downloader := client.Downloader(s.cfg.AC.Container, s.cfg.AC.Prefix)
	rc, err := downloader.Download(ctx, req.ActionDigest.Hash, req.ActionDigest.SizeBytes, 0, 0)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	dt, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("error reading action result: %w", err)
	}

	var result remoteexecution.ActionResult
	if err := proto.Unmarshal(dt, &result); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to unmarshal action result: %v", err)
	}

	// If the client requested to inline stdout/stderr or output files,
	// we need to fetch those blobs and inline them into the result.
	//
	// Note: this is all best effort and not a required part of the API.
	// It does make it so the client doesn't need to make additional
	// requests to fetch these blobs if they are small enough.

	const maxInlineSize = 64 * 1024 // 64 KiB
	var fetchDigests []*remoteexecution.Digest
	if req.InlineStderr {
		if result.StderrDigest != nil && result.StderrDigest.SizeBytes <= maxInlineSize {
			fetchDigests = append(fetchDigests, result.StderrDigest)
		}
	}
	if req.InlineStdout {
		if result.StdoutDigest != nil && result.StdoutDigest.SizeBytes <= maxInlineSize {
			fetchDigests = append(fetchDigests, result.StdoutDigest)
		}
	}

	var digestToFile map[string]*remoteexecution.OutputFile
	if len(result.OutputFiles) > 0 {
		// create an index of paths so we don't have nested loops
		idx := make(map[string]struct{}, len(req.InlineOutputFiles))
		for _, p := range req.InlineOutputFiles {
			idx[p] = struct{}{}
		}

		digestToFile = make(map[string]*remoteexecution.OutputFile, len(req.InlineOutputFiles))
		for _, f := range result.OutputFiles {
			if _, ok := idx[f.Path]; ok && f.Digest != nil && f.Digest.SizeBytes <= maxInlineSize {
				fetchDigests = append(fetchDigests, f.Digest)
				digestToFile[f.Digest.Hash] = f
			}
		}
	}

	if len(fetchDigests) > 0 {
		downloader := client.Downloader(s.cfg.CAS.Container, s.cfg.CAS.Prefix)
		blobsResult, err := s.batchReadBlobs(ctx, downloader, &remoteexecution.BatchReadBlobsRequest{
			Digests: fetchDigests,
		})
		// This is best effort to inline requested blobs
		// no need to return an error here
		if err != nil {
			slog.ErrorContext(ctx, "failed to fetch inline blobs", "error", err)
		}

		stderrHash := ""
		if result.StderrDigest != nil {
			stderrHash = result.StderrDigest.Hash
		}

		stdoutHash := ""
		if result.StdoutDigest != nil {
			stdoutHash = result.StdoutDigest.Hash
		}

		if blobsResult != nil {
			for _, resp := range blobsResult.Responses {
				if resp.Status.Code != int32(codes.OK) {
					slog.ErrorContext(ctx, "failed to fetch blob", "digest", resp.Digest)
					continue
				}

				switch resp.Digest.Hash {
				case stderrHash:
					result.StderrRaw = resp.Data
				case stdoutHash:
					result.StdoutRaw = resp.Data
				default:
					f, ok := digestToFile[resp.Digest.Hash]
					if !ok {
						slog.DebugContext(ctx, "unexpected blob digest", "digest", resp.Digest.Hash)
						continue
					}
					f.Contents = resp.Data
				}
			}
		}
	}

	return &result, nil
}

func (s *AzureBlobServer) UpdateActionResult(ctx context.Context, req *remoteexecution.UpdateActionResultRequest) (*remoteexecution.ActionResult, error) {
	client, err := s.createBlobClient(ctx, &s.cfg.AC)
	if err != nil {
		return nil, err
	}

	// Marshal the action result
	dt, err := proto.Marshal(req.ActionResult)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to marshal action result: %v", err)
	}

	uploader := client.Uploader(s.cfg.AC.Container, s.cfg.AC.Prefix)
	err = uploader.Upload(ctx, req.ActionDigest.Hash, req.ActionDigest.SizeBytes, bytes.NewReader(dt), 0)
	if err != nil {
		return nil, err
	}
	return req.ActionResult, nil
}

func (s *AzureBlobServer) FindMissingBlobs(ctx context.Context, req *remoteexecution.FindMissingBlobsRequest) (*remoteexecution.FindMissingBlobsResponse, error) {
	// FindMissingBlobs is used by bazel to check if blobs are present in the CAS.
	// It sends us a list of digests to check and we return any digests that are not in the CAS.

	client, err := s.createBlobClient(ctx, &s.cfg.CAS)
	if err != nil {
		return nil, err
	}
	downloader := client.Downloader(s.cfg.CAS.Container, s.cfg.CAS.Prefix)

	const maxConcurrency = 20
	sem := semaphore.NewWeighted(maxConcurrency)

	ch := make(chan *remoteexecution.Digest, len(req.BlobDigests))
	var wg sync.WaitGroup

	for _, digest := range req.BlobDigests {
		if err := ctx.Err(); err != nil {
			return nil, status.FromContextError(err).Err()
		}

		if err := validateDigest(digest); err != nil {
			return nil, err
		}

		if digest.Hash == emptySHA256Hash {
			// empty hash is a special case and we never consider it missing
			continue
		}

		wg.Add(1)
		go func() {
			defer wg.Done()

			if err := sem.Acquire(ctx, 1); err != nil {
				// Context was cancelled or deadline exceeded
				return
			}
			defer sem.Release(1)

			exists, err := downloader.Exists(ctx, digest.Hash, digest.SizeBytes)
			if err != nil {
				// Maybe this should trigger an error return?
				// Seems like an error on one blob shouldn't stop the whole operation
				slog.ErrorContext(ctx, "failed to check blob existence", "hash", digest.Hash, "size", digest.SizeBytes, "error", err)
			}
			if !exists {
				ch <- digest
			}
		}()
	}

	wg.Wait()
	close(ch)

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	digests := make([]*remoteexecution.Digest, 0, len(req.BlobDigests))
	for digest := range ch {
		digests = append(digests, digest)
	}

	return &remoteexecution.FindMissingBlobsResponse{
		MissingBlobDigests: digests,
	}, nil
}

func (s *AzureBlobServer) BatchUpdateBlobs(ctx context.Context, req *remoteexecution.BatchUpdateBlobsRequest) (*remoteexecution.BatchUpdateBlobsResponse, error) {
	client, err := s.createBlobClient(ctx, &s.cfg.CAS)
	if err != nil {
		return nil, err
	}

	resultChan := make(chan *remoteexecution.BatchUpdateBlobsResponse_Response, len(req.Requests))

	const maxConcurrency = 10
	sem := semaphore.NewWeighted(maxConcurrency)

	uploader := client.Uploader(s.cfg.CAS.Container, s.cfg.CAS.Prefix)

	for _, r := range req.Requests {
		if err := ctx.Err(); err != nil {
			return nil, status.FromContextError(err).Err()
		}

		if err := validateDigest(r.Digest); err != nil {
			return nil, err
		}
		if r.Digest.Hash == emptySHA256Hash {
			// Special case for empty digest
			resultChan <- &remoteexecution.BatchUpdateBlobsResponse_Response{
				Digest: r.Digest,
				Status: status.New(codes.OK, "").Proto(),
			}
			continue
		}
		go func() {
			if len(r.Data) != int(r.Digest.SizeBytes) {
				resultChan <- &remoteexecution.BatchUpdateBlobsResponse_Response{
					Digest: r.Digest,
					Status: status.Newf(codes.InvalidArgument, "data size mismatch: expected %d, got %d", r.Digest.SizeBytes, len(r.Data)).Proto(),
				}
			}

			hasher := sha256.New()
			hasher.Write(r.Data)
			computedHash := hex.EncodeToString(hasher.Sum(nil))

			if computedHash != r.Digest.Hash {
				resultChan <- &remoteexecution.BatchUpdateBlobsResponse_Response{
					Digest: r.Digest,
					Status: status.Newf(codes.InvalidArgument, "hash mismatch: expected %s, got %s", r.Digest.Hash, computedHash).Proto(),
				}
				return
			}

			// Acquire semaphore
			if err := sem.Acquire(ctx, 1); err != nil {
				resultChan <- &remoteexecution.BatchUpdateBlobsResponse_Response{
					Digest: r.Digest,
					Status: status.FromContextError(ctx.Err()).Proto(),
				}
				return
			}
			defer sem.Release(1)

			err = uploader.Upload(ctx, r.Digest.Hash, r.Digest.SizeBytes, bytes.NewReader(r.Data), 0)
			if err != nil {
				s, ok := status.FromError(err)
				if !ok {
					s = status.New(codes.Internal, fmt.Sprintf("failed to upload blob %s: %v", r.Digest.Hash, err))
				}
				resultChan <- &remoteexecution.BatchUpdateBlobsResponse_Response{
					Digest: r.Digest,
					Status: s.Proto(),
				}
				return
			}

			resultChan <- &remoteexecution.BatchUpdateBlobsResponse_Response{
				Digest: r.Digest,
				Status: status.New(codes.OK, "").Proto(),
			}
		}()
	}

	// Collect results
	responses := make([]*remoteexecution.BatchUpdateBlobsResponse_Response, 0, len(req.Requests))
	for range len(req.Requests) {
		result := <-resultChan
		responses = append(responses, result)
	}

	return &remoteexecution.BatchUpdateBlobsResponse{
		Responses: responses,
	}, nil
}

func (s *AzureBlobServer) BatchReadBlobs(ctx context.Context, req *remoteexecution.BatchReadBlobsRequest) (*remoteexecution.BatchReadBlobsResponse, error) {
	client, err := s.createBlobClient(ctx, &s.cfg.CAS)
	if err != nil {
		return nil, err
	}
	return s.batchReadBlobs(ctx, client.Downloader(s.cfg.CAS.Container, s.cfg.CAS.Prefix), req)
}

func (s *AzureBlobServer) batchReadBlobs(ctx context.Context, downloader Downloader, req *remoteexecution.BatchReadBlobsRequest) (*remoteexecution.BatchReadBlobsResponse, error) {
	const maxConcurrency = 10
	sem := semaphore.NewWeighted(maxConcurrency)

	resultChan := make(chan *remoteexecution.BatchReadBlobsResponse_Response, len(req.Digests))
	for _, d := range req.Digests {
		if err := ctx.Err(); err != nil {
			return nil, status.FromContextError(err).Err()
		}

		go func() {
			if err := validateDigest(d); err != nil {
				resultChan <- &remoteexecution.BatchReadBlobsResponse_Response{
					Digest: d,
					Status: status.New(codes.InvalidArgument, fmt.Sprintf("invalid digest: %v", err)).Proto(),
				}
				return
			}

			if d.Hash == emptySHA256Hash {
				resultChan <- &remoteexecution.BatchReadBlobsResponse_Response{
					Digest: d,
					Data:   emptyBuf, // Empty data for empty digest
					Status: status.New(codes.OK, "").Proto(),
				}
				return
			}
			if err := sem.Acquire(ctx, 1); err != nil {
				resultChan <- &remoteexecution.BatchReadBlobsResponse_Response{
					Digest: d,
					Status: status.FromContextError(ctx.Err()).Proto(),
				}
				return
			}
			defer sem.Release(1)

			rc, err := downloader.Download(ctx, d.Hash, d.SizeBytes, 0, 0)
			if err != nil {
				resultChan <- &remoteexecution.BatchReadBlobsResponse_Response{
					Digest: d,
					Status: status.Newf(codes.Internal, "download failed: %v", err).Proto(),
				}
				return
			}
			defer rc.Close()

			dt, err := io.ReadAll(rc)
			if err != nil {
				resultChan <- &remoteexecution.BatchReadBlobsResponse_Response{
					Digest: d,
					Status: status.Newf(codes.Internal, "error reading blob data: %v", err).Proto(),
				}
				return
			}

			resultChan <- &remoteexecution.BatchReadBlobsResponse_Response{
				Digest: d,
				Data:   dt,
				Status: status.New(codes.OK, "").Proto(),
			}
		}()
	}

	// Collect results
	responses := make([]*remoteexecution.BatchReadBlobsResponse_Response, 0, len(req.Digests))
	for range len(req.Digests) {
		result := <-resultChan
		responses = append(responses, result)
	}

	return &remoteexecution.BatchReadBlobsResponse{
		Responses: responses,
	}, nil
}

func (s *AzureBlobServer) GetTree(req *remoteexecution.GetTreeRequest, srv remoteexecution.ContentAddressableStorage_GetTreeServer) error {
	return status.Errorf(codes.Unimplemented, "GetTree is not implemented since this is for caching only")
}

func (s *AzureBlobServer) GetCapabilities(ctx context.Context, req *remoteexecution.GetCapabilitiesRequest) (*remoteexecution.ServerCapabilities, error) {
	return &remoteexecution.ServerCapabilities{
		CacheCapabilities: &remoteexecution.CacheCapabilities{
			DigestFunctions: []remoteexecution.DigestFunction_Value{
				digestFunc,
			},
			ActionCacheUpdateCapabilities: &remoteexecution.ActionCacheUpdateCapabilities{
				UpdateEnabled: true,
			},
			MaxBatchTotalSizeBytes:      4 * 1024 * 1024,
			SymlinkAbsolutePathStrategy: remoteexecution.SymlinkAbsolutePathStrategy_DISALLOWED,
			SupportedBatchUpdateCompressors: []remoteexecution.Compressor_Value{
				remoteexecution.Compressor_IDENTITY, // No compression
			},
			SupportedCompressors: []remoteexecution.Compressor_Value{
				remoteexecution.Compressor_IDENTITY, // No compression
			},
		},
		LowApiVersion:  &semver.SemVer{Major: 2, Minor: 0, Patch: 0},
		HighApiVersion: &semver.SemVer{Major: 2, Minor: 0, Patch: 0},
	}, nil
}

// Read implements ByteStream Read (downloads)
func (s *AzureBlobServer) Read(req *bytestream.ReadRequest, srv bytestream.ByteStream_ReadServer) error {
	ctx := srv.Context()

	hash, size, err := parseByteStreamResourceName(req.ResourceName)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid resource name: %v", err)
	}
	expected := size - req.ReadOffset
	if expected < 0 {
		return status.Errorf(codes.OutOfRange, "read offset %d is greater than size %d", req.ReadOffset, size)
	}

	if req.ReadLimit > 0 && req.ReadLimit < expected {
		expected = req.ReadLimit
	}

	client, err := s.createBlobClient(ctx, &s.cfg.CAS)
	if err != nil {
		return err
	}

	downloader := client.Downloader(s.cfg.CAS.Container, s.cfg.CAS.Prefix)
	rc, err := downloader.Download(ctx, hash, size, req.ReadOffset, req.ReadLimit)
	if err != nil {
		return err
	}
	defer rc.Close()

	w := streamWriterPool.Get().(*byteStreamWriter)
	defer func() {
		w.Reset()
		streamWriterPool.Put(w)
	}()
	w.srv = srv

	_, err = copyBuffer(w, &io.LimitedReader{R: rc, N: expected})
	if err != nil {
		return err
	}
	return err
}

// Write implements ByteStream Write (uploads)
func (s *AzureBlobServer) Write(srv bytestream.ByteStream_WriteServer) error {
	ctx := srv.Context()

	// Upload to Azure Blob Storage
	client, err := s.createBlobClient(ctx, &s.cfg.CAS)
	if err != nil {
		return err
	}

	uploader := client.Uploader(s.cfg.CAS.Container, s.cfg.CAS.Prefix)

	size, err := s.writeStream(ctx, uploader, srv)
	if err != nil {
		return err
	}

	// Send response
	return srv.SendAndClose(&bytestream.WriteResponse{
		CommittedSize: size,
	})
}

// QueryWriteStatus implements ByteStream QueryWriteStatus
func (s *AzureBlobServer) QueryWriteStatus(ctx context.Context, req *bytestream.QueryWriteStatusRequest) (*bytestream.QueryWriteStatusResponse, error) {
	// Parse resource name to get digest
	hash, size, err := parseByteStreamResourceName(req.ResourceName)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid resource name: %v", err)
	}

	if hash == emptySHA256Hash {
		// Special case for empty digest
		return &bytestream.QueryWriteStatusResponse{
			CommittedSize: 0,
			Complete:      true,
		}, nil
	}

	client, err := s.createBlobClient(ctx, &s.cfg.CAS)
	if err != nil {
		return nil, err
	}

	downloader := client.Downloader(s.cfg.CAS.Container, s.cfg.CAS.Prefix)

	// We do not support querrying write status or resumable uploads, so either the blob exists or it doesn't.
	// If it exists, we return the committed size and complete status.
	// If it doesn't exist, we return 0 committed size and complete false.
	exists, err := downloader.Exists(ctx, hash, size)
	if err != nil {
		return nil, err
	}

	if exists {
		return &bytestream.QueryWriteStatusResponse{
			CommittedSize: size,
			Complete:      true,
		}, nil
	}

	u := client.Uploader(s.cfg.CAS.Container, s.cfg.CAS.Prefix)
	info, err := u.Status(ctx, hash, size)
	if err != nil {
		return nil, err
	}

	return &bytestream.QueryWriteStatusResponse{
		CommittedSize: info.AvailableBytes,
	}, nil
}
