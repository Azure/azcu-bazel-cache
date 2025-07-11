package bazelazblob

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/signal"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/cpuguy83/go-docker"
	"github.com/cpuguy83/go-docker/container"
	"github.com/cpuguy83/go-docker/container/containerapi"
	"github.com/cpuguy83/go-docker/errdefs"
	"github.com/cpuguy83/go-docker/image"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	azuriteImageRef    = "mcr.microsoft.com/azure-storage/azurite:3.34.0"
	azuriteBlobPortKey = "10000/tcp"
)

var baseCtx = context.Background()

func TestMain(m *testing.M) {
	var cancel context.CancelFunc
	baseCtx, cancel = signal.NotifyContext(baseCtx, os.Interrupt)
	defer cancel()

	os.Exit(m.Run())
}

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	t.Parallel()

	ctx := baseCtx
	client := docker.NewClient()

	accountName := "some_account"

	serviceURL, certRdr := runAzurite(ctx, t, client, accountName)
	cert, err := io.ReadAll(certRdr)
	if err != nil {
		t.Fatal("Failed to read cert:", err)
	}

	clientOpts := []func(*azblob.ClientOptions){
		clientOptWithHTTPClient(t, cert),
	}
	cfg := AzureBlobServerConfig{
		CAS: AzureBlobAccountConfig{
			ServiceURL:   serviceURL,
			Container:    "blobcas",
			Prefix:       "cas",
			azClientOpts: clientOpts,
		},
		AC: AzureBlobAccountConfig{
			ServiceURL:   serviceURL,
			Container:    "blobac",
			Prefix:       "ac",
			azClientOpts: clientOpts,
		},
	}

	// Create the blob containers needed for the test.
	// The bazelazblob server will not create these automatically, they must pre-exist.
	token := fakeJWT(accountName)
	azAuth := &staticTokenCredential{token: token}
	createBlobContainers(ctx, t, cert, azAuth, serviceURL, cfg.CAS.Container, cfg.AC.Container)

	dialer := setupTestServers(ctx, t, cfg, azAuth)

	cc, err := grpc.NewClient("passthrough://", grpc.WithContextDialer(dialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal("Failed to create gRPC client:", err)
	}
	defer cc.Close()

	makeDigest := func(data []byte) *remoteexecution.Digest {
		digestBytes := sha256.Sum256(data)
		return &remoteexecution.Digest{
			Hash:      hex.EncodeToString(digestBytes[:]),
			SizeBytes: int64(len(data)),
		}
	}

	casClient := remoteexecution.NewContentAddressableStorageClient(cc)
	uploadBlobs := func(t *testing.T, blobs ...[]byte) []*remoteexecution.Digest {
		requests := make([]*remoteexecution.BatchUpdateBlobsRequest_Request, 0, len(blobs))

		for _, blob := range blobs {
			requests = append(requests, &remoteexecution.BatchUpdateBlobsRequest_Request{
				Data:   blob,
				Digest: makeDigest(blob),
			})
		}

		resp, err := casClient.BatchUpdateBlobs(ctx, &remoteexecution.BatchUpdateBlobsRequest{
			Requests: requests,
		})
		require.NoError(t, err)

		out := make([]*remoteexecution.Digest, len(resp.Responses))
		for _, resp := range resp.Responses {
			require.Equal(t, int32(codes.OK), resp.Status.Code, resp.Status.Message)

			// Insert the digesets into the output slice ordering it the same as
			// the input blob requests.
			// This makes it easier to match the uploaded blobs since the responses
			// may not be in the same order as the requests.
			for i, request := range requests {
				if request.Digest.Hash == resp.Digest.Hash {
					out[i] = request.Digest
					break
				}
			}
		}

		return out
	}

	t.Run("ActionCache", func(t *testing.T) {
		client := remoteexecution.NewActionCacheClient(cc)

		t.Run("empty digest always succeeds", func(t *testing.T) {
			req := &remoteexecution.GetActionResultRequest{
				ActionDigest: &remoteexecution.Digest{
					Hash: emptySHA256Hash,
				},
			}
			res, err := client.GetActionResult(ctx, req)
			require.NoError(t, err, "GetActionResult should succeed for empty digest")
			expectRes := &remoteexecution.ActionResult{}
			proto.Unmarshal([]byte{}, expectRes)
			require.Equal(t, expectRes, res)
		})

		t.Run("missing digest returns not found error", func(t *testing.T) {
			digestBytes := sha256.Sum256([]byte("some data"))
			req := &remoteexecution.GetActionResultRequest{
				ActionDigest: &remoteexecution.Digest{
					Hash: hex.EncodeToString(digestBytes[:]),
				},
			}
			_, err := client.GetActionResult(ctx, req)
			require.NotNil(t, err, "GetActionResult should return an error for missing digest")

			st, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.NotFound, st.Code(), "Expected NotFound error for missing digest: %v", st.Message())
		})

		t.Run("store and retrieve action result", func(t *testing.T) {
			dt := []byte("some data to store in action result")
			stdoutDt := []byte("some stdout data")
			stderrDt := []byte("some stderr data")
			uploaded := uploadBlobs(t, dt, stdoutDt, stderrDt)

			actionResult := &remoteexecution.ActionResult{
				OutputFiles: []*remoteexecution.OutputFile{
					{
						Path:   "output.txt",
						Digest: uploaded[0],
					},
				},
				StdoutDigest: uploaded[1],
				StderrDigest: uploaded[2],
			}

			acDt, err := proto.Marshal(actionResult)
			require.NoError(t, err, "Failed to marshal action result")

			_, err = client.UpdateActionResult(ctx, &remoteexecution.UpdateActionResultRequest{
				ActionDigest: makeDigest(acDt),
				ActionResult: actionResult,
			})
			require.NoError(t, err)

			actual, err := client.GetActionResult(ctx, &remoteexecution.GetActionResultRequest{
				ActionDigest:      makeDigest(acDt),
				InlineOutputFiles: []string{"output.txt"},
				InlineStdout:      true,
				InlineStderr:      true,
			})
			require.NoError(t, err, "GetActionResult should succeed for stored action result")
			require.Len(t, actual.OutputFiles, 1, "Expected one output file in action result")
			require.Equal(t, "output.txt", actual.OutputFiles[0].Path, "Expected output file path to match")
			assert.Equal(t, uploaded[0].Hash, actual.OutputFiles[0].Digest.Hash, "Expected output file hash to match")
			assert.Equal(t, uploaded[0].SizeBytes, actual.OutputFiles[0].Digest.SizeBytes, "Expected output file size to match")
			assert.Equal(t, string(dt), string(actual.OutputFiles[0].Contents), "Expected output file content to match")

			assert.Equal(t, uploaded[1].Hash, actual.StdoutDigest.Hash, "Expected stdout digest to match")
			assert.Equal(t, uploaded[1].SizeBytes, actual.StdoutDigest.SizeBytes, "Expected stdout size to match")
			assert.Equal(t, string(stdoutDt), string(actual.StdoutRaw), "Expected stdout content to match")

			assert.Equal(t, uploaded[2].Hash, actual.StderrDigest.Hash, "Expected stderr digest to match")
			assert.Equal(t, uploaded[2].SizeBytes, actual.StderrDigest.SizeBytes, "Expected stderr size to match")
			assert.Equal(t, string(stderrDt), string(actual.StderrRaw), "Expected stderr content to match")

			// Again but without requesting inlining, which means the response should
			// *not* contain any of the actual contents.
			actual, err = client.GetActionResult(ctx, &remoteexecution.GetActionResultRequest{
				ActionDigest: makeDigest(acDt),
			})
			require.NoError(t, err, "GetActionResult should succeed for stored action result")
			require.Len(t, actual.OutputFiles, 1, "Expected one output file in action result")
			require.Equal(t, "output.txt", actual.OutputFiles[0].Path, "Expected output file path to match")

			assert.Equal(t, uploaded[0].Hash, actual.OutputFiles[0].Digest.Hash, "Expected output file hash to match")
			assert.Equal(t, uploaded[0].SizeBytes, actual.OutputFiles[0].Digest.SizeBytes, "Expected output file size to match")
			assert.Equal(t, "", string(actual.OutputFiles[0].Contents), "output files should not be inlined")

			assert.Equal(t, uploaded[1].Hash, actual.StdoutDigest.Hash, "Expected stdout digest to match")
			assert.Equal(t, uploaded[1].SizeBytes, actual.StdoutDigest.SizeBytes, "Expected stdout size to match")
			assert.Equal(t, "", string(actual.StdoutRaw), "Stdout should not be inlined")

			assert.Equal(t, uploaded[2].Hash, actual.StderrDigest.Hash, "Expected stderr digest to match")
			assert.Equal(t, uploaded[2].SizeBytes, actual.StderrDigest.SizeBytes, "Expected stderr size to match")
			assert.Equal(t, "", string(actual.StderrRaw), "Stderr should not be inlined")
		})
	})

	t.Run("ContentAddressableStorage", func(t *testing.T) {
		dt0 := []byte("some data to store in CAS")
		dt1 := []byte("some other data to store in CAS")

		missingDt0 := []byte("this data is not uploaded")
		missingDt0Digest := makeDigest(missingDt0)
		missingDt1 := []byte("this data is also not uploaded")
		missingDt1Digest := makeDigest(missingDt1)

		digests := uploadBlobs(t, dt0, dt1)

		missing, err := casClient.FindMissingBlobs(ctx, &remoteexecution.FindMissingBlobsRequest{
			BlobDigests: []*remoteexecution.Digest{
				missingDt0Digest,
				missingDt1Digest,
				digests[0],
				digests[1],
			},
		})
		require.NoError(t, err)
		require.Len(t, missing.MissingBlobDigests, 2, "Expected two missing blobs")

		missingHashes := make([]string, 0, len(missing.MissingBlobDigests))
		for _, d := range missing.MissingBlobDigests {
			missingHashes = append(missingHashes, d.Hash)
		}
		assert.Contains(t, missingHashes, missingDt0Digest.Hash, "Expected first missing blob to be reported")
		assert.Contains(t, missingHashes, missingDt1Digest.Hash, "Expected second missing blob to be reported")
		assert.NotContains(t, missingHashes, digests[0].Hash, "Expected first uploaded blob to not be reported as missing")
		assert.NotContains(t, missingHashes, digests[1].Hash, "Expected second uploaded blob to not be reported as missing")

		uploadBlobs(t, missingDt0)
		missing, err = casClient.FindMissingBlobs(ctx, &remoteexecution.FindMissingBlobsRequest{
			BlobDigests: []*remoteexecution.Digest{
				missingDt0Digest,
				missingDt1Digest,
				digests[0],
				digests[1],
			},
		})
		require.NoError(t, err)
		require.Len(t, missing.MissingBlobDigests, 1, "Expected two missing blobs")

		missingHashes = make([]string, 0, len(missing.MissingBlobDigests))
		for _, d := range missing.MissingBlobDigests {
			missingHashes = append(missingHashes, d.Hash)
		}
		assert.NotContains(t, missingHashes, missingDt0Digest.Hash, "Expected first missing blob to be reported")
		assert.Contains(t, missingHashes, missingDt1Digest.Hash, "Expected second missing blob to be reported")
		assert.NotContains(t, missingHashes, digests[0].Hash, "Expected first uploaded blob to not be reported as missing")
		assert.NotContains(t, missingHashes, digests[1].Hash, "Expected second uploaded blob to not be reported as missing")

		resp, err := casClient.BatchReadBlobs(ctx, &remoteexecution.BatchReadBlobsRequest{
			Digests: digests,
		})
		require.NoError(t, err, "BatchReadBlobs should succeed for uploaded digests")
		require.Len(t, resp.Responses, len(digests), "Expected two responses for uploaded digests")

		hashDt := make(map[string][]byte, len(resp.Responses))
		for _, r := range resp.Responses {
			require.Equal(t, int32(codes.OK), r.Status.Code, "Expected OK status for uploaded blob")
			require.NotNil(t, r.Data, "Expected data to be present for uploaded blob")
			hashDt[r.Digest.Hash] = r.Data
		}

		assert.Equal(t, dt0, hashDt[digests[0].Hash], "Expected first uploaded blob data to match")
		assert.Equal(t, dt1, hashDt[digests[1].Hash], "Expected second uploaded blob data to match")
	})

	t.Run("ByteStream", func(t *testing.T) {
		dir := t.TempDir()
		f, err := os.CreateTemp(dir, "test-blob-")
		require.NoError(t, err, "Failed to create temp file for blob upload")
		defer f.Close()

		hasher := sha256.New()

		// Just some data to fill up the file.
		dt := bytes.Repeat([]byte("a"), 512*1024) // 512 KB
		var size int64
		for range 1024 { // Write 1024 times to make it 512MB
			w := io.MultiWriter(f, hasher)
			n, err := w.Write(dt)
			require.NoError(t, err)
			size += int64(n)
		}
		_, err = f.Seek(0, io.SeekStart)
		require.NoError(t, err)

		hash := hex.EncodeToString(hasher.Sum(nil))
		name := fmt.Sprintf("uploads/%s/blobs/%s/%d", "test-uuid", hash, size)

		bsClient := bytestream.NewByteStreamClient(cc)
		writer, err := bsClient.Write(ctx)
		require.NoError(t, err)
		defer writer.CloseSend()

		var msg bytestream.WriteRequest

		var offset int64
		buf := make([]byte, 32*1024)
		breakAfterOffset := int64(100 * 1024 * 1024) // 100 MB

		checkStat := func(completed bool, size int64) {
			stat, err := bsClient.QueryWriteStatus(ctx, &bytestream.QueryWriteStatusRequest{
				ResourceName: name,
			})

			require.NoError(t, err)
			require.Equal(t, size, stat.CommittedSize)
			require.Equal(t, completed, stat.Complete)
		}

		checkMissing := func(expectedMissing bool) {
			missing, err := casClient.FindMissingBlobs(ctx, &remoteexecution.FindMissingBlobsRequest{
				BlobDigests: []*remoteexecution.Digest{
					{Hash: hash, SizeBytes: size},
				},
			})
			require.NoError(t, err)

			if expectedMissing {
				require.Len(t, missing.MissingBlobDigests, 1, "Expected one missing blob after partial upload")
				require.Equal(t, hash, missing.MissingBlobDigests[0].Hash, "Expected missing blob to match uploaded blob")
			} else {
				require.Len(t, missing.MissingBlobDigests, 0, "Expected no missing blobs after full upload")
			}
		}

		writeChunk := func(rdr io.Reader, offset int64) (int64, bool) {
			n, err := rdr.Read(buf)
			if err != nil && !errors.Is(err, io.EOF) {
				t.Fatalf("Failed to read from file: %v", err)
			}

			if n > 0 {
				msg.Reset()
				msg.ResourceName = name
				msg.WriteOffset = offset
				msg.Data = buf[:n]

				err := writer.Send(&msg)
				require.NoError(t, err, "Failed to send WriteRequest, offset: %d, total: %d", offset, size)
				offset += int64(n)
			}

			return offset, err != io.EOF
		}

		// Write part of the file then break the upload to check resuming behavior.
		for {
			if offset > breakAfterOffset {
				break
			}

			var more bool
			offset, more = writeChunk(f, offset)
			if !more {
				break
			}
		}

		resp, err := writer.CloseAndRecv()
		require.NoError(t, err)
		require.Equal(t, offset, resp.CommittedSize)
		checkStat(false, resp.CommittedSize)

		// The blob should *not* be available yet
		checkMissing(true)

		writer, err = bsClient.Write(ctx)
		require.NoError(t, err)

		// Now finish the upload
		for {
			var more bool
			offset, more = writeChunk(f, offset)
			if !more {
				break
			}
		}

		resp, err = writer.CloseAndRecv()
		require.NoError(t, err)
		require.Equal(t, offset, resp.CommittedSize)
		checkStat(true, resp.CommittedSize)

		// The blob should now be available
		checkMissing(false)
	})
}

func setupTestServers(ctx context.Context, t *testing.T, cfg AzureBlobServerConfig, auth azcore.TokenCredential) func(context.Context, string) (net.Conn, error) {
	var srvListener, proxyListener pipeListener
	t.Cleanup(func() {
		srvListener.Close()
		proxyListener.Close()
	})

	srv, err := NewAzureBlobServer(cfg)
	if err != nil {
		t.Fatal(err)
	}

	grpcAzblobServ := grpc.NewServer()
	RegisterAzureBlobServer(grpcAzblobServ, srv)
	t.Cleanup(grpcAzblobServ.Stop)

	dialOpts := []grpc.DialOption{
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return srvListener.Dialer(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStreamInterceptor(StreamClientAuthInterceptor(auth)),
		grpc.WithUnaryInterceptor(UnaryClientAuthInterceptor(auth)),
	}

	// passthrough:// is a special scheme that prevents the gRPC client from
	// trying to resolve the address, which is necessary since we are using a
	// pipe listener.
	cc, err := grpc.NewClient("passthrough://", dialOpts...)
	if err != nil {
		t.Fatal("Failed to create gRPC client:", err)
	}
	t.Cleanup(func() { cc.Close() })

	proxy := NewProxy(ProxyBackendFromGRPC(cc))

	grpcProxyServ := grpc.NewServer()
	RegisterProxyServer(grpcProxyServ, proxy)
	t.Cleanup(grpcProxyServ.Stop)

	var eg errgroup.Group
	go func() {
		<-ctx.Done()
		grpcProxyServ.Stop()
		grpcAzblobServ.Stop()
	}()

	eg.Go(func() error { return grpcAzblobServ.Serve(&srvListener) })
	eg.Go(func() error { return grpcProxyServ.Serve(&proxyListener) })

	return func(ctx context.Context, _ string) (net.Conn, error) {
		return proxyListener.Dialer(ctx)
	}
}

func clientOptWithHTTPClient(t *testing.T, cert []byte) func(*azblob.ClientOptions) {
	return func(opts *azblob.ClientOptions) {
		opts.Transport = newTestHTTPClient(t, cert)
	}
}

func createBlobContainers(ctx context.Context, t *testing.T, cert []byte, auth azcore.TokenCredential, serviceURL string, containers ...string) {
	azblobClient, err := azblob.NewClient(serviceURL, auth, &azblob.ClientOptions{ClientOptions: azcore.ClientOptions{
		Transport: newTestHTTPClient(t, cert),
	}})
	if err != nil {
		t.Fatal("Failed to create AzBlob client:", err)
	}

	for _, container := range containers {
		if _, err := azblobClient.CreateContainer(ctx, container, nil); err != nil {
			t.Fatalf("Failed to create container %q: %v", container, err)
		}
		t.Log("created container:", container)
	}
}

func newTestHTTPClient(t *testing.T, cert []byte) *http.Client {
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(cert) {
		t.Fatal("Failed to append cert to CA pool")
	}

	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs: caPool,
			},
		},
	}
}

func createCertsDir(t *testing.T, cert, key *bytes.Buffer) io.Reader {
	t.Helper()

	buf := bytes.NewBuffer(nil)
	tw := tar.NewWriter(buf)

	certsDir := &tar.Header{
		Name:     "certs/",
		Mode:     0755,
		Typeflag: tar.TypeDir,
	}
	if err := tw.WriteHeader(certsDir); err != nil {
		t.Fatal("Failed to write certs dir header:", err)
	}

	certFile := &tar.Header{
		Name:     "certs/server.crt",
		Mode:     0600,
		Size:     int64(cert.Len()),
		Typeflag: tar.TypeReg,
	}
	if err := tw.WriteHeader(certFile); err != nil {
		t.Fatal("Failed to write cert header:", err)
	}
	_, err := io.Copy(tw, cert)
	if err != nil {
		t.Fatal("Failed to write cert data:", err)
	}

	keyFile := &tar.Header{
		Name:     "certs/server.key",
		Mode:     0600,
		Size:     int64(key.Len()),
		Typeflag: tar.TypeReg,
	}

	if err := tw.WriteHeader(keyFile); err != nil {
		t.Fatal("Failed to write key header:", err)
	}

	_, err = io.Copy(tw, key)
	if err != nil {
		t.Fatal("Failed to write key data:", err)
	}

	if err := tw.Flush(); err != nil {
		t.Fatal("Failed to flush tar writer:", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal("Failed to close tar writer:", err)
	}
	return buf
}

func runAzurite(ctx context.Context, t *testing.T, client *docker.Client, accountName string) (string, io.Reader) {
	csvc := client.ContainerService()

	cert := bytes.NewBuffer(nil)
	key := bytes.NewBuffer(nil)

	hcfg := containerapi.HostConfig{
		AutoRemove: true,
		PortBindings: containerapi.PortMap{
			azuriteBlobPortKey: []containerapi.PortBinding{{HostIP: "127.0.0.1"}},
		},
	}

	cfg := containerapi.Config{
		Image: azuriteImageRef,
		Cmd: []string{
			"azurite",
			"--oauth", "basic",
			"--blobHost", "0.0.0.0",
			"--blobPort", "10000",
			"-l", "/data",
			"--cert", "/certs/server.crt",
			"--key", "/certs/server.key",
		},
		Env: []string{
			fmt.Sprintf("AZURITE_ACCOUNTS=%s:some_key", accountName),
		},
	}

	retCert := bytes.NewBuffer(nil)
	createTLSCerts(t, io.MultiWriter(retCert, cert), key)

	createOpts := []container.CreateOption{
		container.WithCreateHostConfig(hcfg),
		container.WithCreateConfig(cfg),
		container.WithCreateAttachStderr,
	}

	c, err := csvc.Create(ctx, azuriteImageRef, createOpts...)
	if err != nil {
		if !errdefs.IsNotFound(err) {
			t.Fatalf("Failed to create container: %v", err)
		}

		ref, err := image.ParseRef(azuriteImageRef)
		if err != nil {
			t.Fatalf("Failed to parse image reference: %v", err)
		}
		if err := client.ImageService().Pull(ctx, ref); err != nil {
			t.Fatal("Failed to pull Azurite image:", err)
		}

		c, err = csvc.Create(ctx, azuriteImageRef, createOpts...)
		if err != nil {
			t.Fatal(err)
		}
	}

	err = c.Upload(ctx, "/", createCertsDir(t, cert, key))
	if err != nil {
		t.Fatal("Failed to upload certs to container:", err)
	}

	t.Cleanup(func() {
		if t.Failed() {
			r, w := io.Pipe()

			go func() {
				defer w.Close()

				err := c.Logs(ctx, func(cfg *container.LogReadConfig) {
					cfg.Stdout = w
				})
				if err != nil {
					t.Log("error reading container logs", err)
				}
			}()

			scanner := bufio.NewScanner(r)
			for scanner.Scan() {
				line := scanner.Text()
				if line != "" {
					t.Log(line)
				}
			}
		}

		err := csvc.Remove(ctx, c.ID(), container.WithRemoveForce)
		if err != nil && !errdefs.IsNotFound(err) {
			t.Log(err)
		}
	})

	stderr, err := c.StderrPipe(ctx)
	if err != nil {
		t.Fatalf("Failed to get stderr pipe: %v", err)
	}
	go func() {
		defer stderr.Close()

		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			line := scanner.Text()
			if line != "" {
				t.Log(line)
			}
		}
	}()

	if err := c.Start(ctx); err != nil {
		csvc.Remove(ctx, c.ID(), container.WithRemoveForce)
		t.Fatal(err)
	}

	info, err := c.Inspect(ctx)
	if err != nil {
		t.Fatalf("Failed to inspect container: %v", err)
	}

	portInfo := info.NetworkSettings.Ports[azuriteBlobPortKey][0]
	t.Logf("Azurite container %s is running on %s:%s", c.ID(), portInfo.HostIP, portInfo.HostPort)
	serviceURL := fmt.Sprintf("https://%s:%s/%s", portInfo.HostIP, portInfo.HostPort, accountName)
	t.Log(serviceURL)

	return serviceURL, retCert
}

func createTLSCerts(t *testing.T, cert, key io.Writer) {
	t.Helper()

	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}

	// Self-signed cert template
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "localhost",
		},
		NotBefore: time.Now(),
		NotAfter:  time.Now().Add(365 * 24 * time.Hour), // valid for 1 year

		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
		IPAddresses: []net.IP{
			net.ParseIP("127.0.0.1"),
		},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		log.Fatal(err)
	}

	if err := pem.Encode(cert, &pem.Block{Type: "CERTIFICATE", Bytes: certDER}); err != nil {
		t.Fatal(err)
	}

	if err := pem.Encode(key, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)}); err != nil {
		t.Fatal(err)
	}
}

func fakeJWT(accountName string) string {
	header := `{"alg":"none","typ":"JWT"}`
	now := time.Now().Unix()
	// NOTE: azurite validates that aud and iss is within a set of known values
	// aud: https://github.com/Azure/Azurite/blob/e20f8673cc0817fe5f1648ef51a3dc61c617b6bf/src/blob/utils/constants.ts#L150-L169
	// iss: https://github.com/Azure/Azurite/blob/e20f8673cc0817fe5f1648ef51a3dc61c617b6bf/src/common/utils/constants.ts#L42-L54
	//
	// iat, nbf, and exp must be set:
	// https://github.com/Azure/Azurite/blob/e20f8673cc0817fe5f1648ef51a3dc61c617b6bf/src/blob/authentication/BlobTokenAuthenticator.ts#L161-L163
	payload := fmt.Sprintf(`{
		"aud": "https://%s.blob.core.windows.net/",
		"iss": "https://sts.windows.net/",
		"sub": "test-user",
		"iat": %d,
		"nbf": %d,
		"exp": %d
	}`, accountName, now, now, now+3600)

	encode := func(s string) string {
		return base64.RawURLEncoding.EncodeToString([]byte(s))
	}

	// <-- Include a non-empty dummy signature here
	return fmt.Sprintf("%s.%s.%s", encode(header), encode(payload), "x")
}
