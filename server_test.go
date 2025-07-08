package bazelazblob

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestValidateDigest(t *testing.T) {
	tests := []struct {
		name        string
		hash        string
		size        int64
		expectedErr bool
	}{
		{
			name:        "valid digest",
			hash:        "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
			size:        1024,
			expectedErr: false,
		},
		{
			name:        "empty hash",
			hash:        "",
			size:        1024,
			expectedErr: true,
		},
		{
			name:        "invalid hash length",
			hash:        "abcdef123456",
			size:        1024,
			expectedErr: true,
		},
		{
			name:        "negative size",
			hash:        "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
			size:        -1,
			expectedErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := &remoteexecution.Digest{Hash: tc.hash, SizeBytes: tc.size}
			err := validateDigest(d)

			if tc.expectedErr && err == nil {
				t.Fatalf("expected error, got nil")
			}

			if !tc.expectedErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// testServer wraps the AzureBlobServer with a real gRPC server for testing
type testServer struct {
	server   *grpc.Server
	listener *pipeListener
	azbs     *AzureBlobServer
}

func newTestServer(t *testing.T) (*testServer, *testBlobClient) {
	var l pipeListener
	t.Cleanup(func() {
		l.Close()
	})

	client := &testBlobClient{
		content: make(map[string][]byte),
	}
	azbs := &AzureBlobServer{
		client: client,
	}

	s := grpc.NewServer()
	RegisterAzureBlobServer(s, azbs)
	chErr := make(chan error, 1)
	t.Cleanup(func() {
		s.Stop()
		require.NoError(t, <-chErr, "Error serving gRPC server")
	})

	go func() {
		chErr <- s.Serve(&l)
	}()

	return &testServer{
		server:   s,
		listener: &l,
		azbs:     azbs,
	}, client
}

func (ts *testServer) newClient(ctx context.Context, t *testing.T) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient("passthrough://", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return ts.listener.Dialer(ctx)
	}))
	t.Cleanup(func() {
		conn.Close()
	})
	require.NoError(t, err)
	return conn
}

func TestByteStreamWriteRead(t *testing.T) {
	ctx := context.Background()
	ts, _ := newTestServer(t)

	conn := ts.newClient(ctx, t)
	bsClient := bytestream.NewByteStreamClient(conn)

	testDataBase := "hello world"
	testData := []byte(strings.Repeat(testDataBase, 100))

	sum := sha256.Sum256(testData)
	hash := hex.EncodeToString(sum[:])
	size := int64(len(testData))
	resourceName := fmt.Sprintf("uploads/test-uuid/blobs/%s/%d", hash, size)

	writeClient, err := bsClient.Write(ctx)
	require.NoError(t, err)

	var total int64
	for offset := 0; offset < len(testData); offset += 10 {
		chunkSize := min(10, len(testData)-offset)
		chunk := testData[offset : offset+chunkSize]

		req := &bytestream.WriteRequest{
			ResourceName: resourceName,
			Data:         chunk,
			WriteOffset:  total,
			FinishWrite:  total+int64(offset) == size,
		}
		err = writeClient.Send(req)
		require.NoError(t, err)
		total += int64(chunkSize)
	}

	resp, err := writeClient.CloseAndRecv()
	require.NoError(t, err)
	assert.Equal(t, resp.CommittedSize, size)
}

func TestByteStreamRead(t *testing.T) {
	ctx := context.Background()
	ts, tc := newTestServer(t)

	conn := ts.newClient(ctx, t)

	bsClient := bytestream.NewByteStreamClient(conn)

	testData := []byte("hello world test data")
	digest := sha256.Sum256(testData)
	encoded := hex.EncodeToString(digest[:])
	size := int64(len(testData))

	tc.content[encoded] = testData

	readResourceName := fmt.Sprintf("blobs/%s/%d", encoded, size)

	t.Run("with offset", func(t *testing.T) {
		readReq := &bytestream.ReadRequest{
			ResourceName: readResourceName,
			ReadOffset:   6, // Skip "hello "
			ReadLimit:    5, // Read "world"
		}

		readClient, err := bsClient.Read(ctx, readReq)
		if err != nil {
			t.Fatalf("Failed to create read client: %v", err)
		}

		var receivedData []byte
		for {
			resp, err := readClient.Recv()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			receivedData = append(receivedData, resp.Data...)
		}

		expected := "world"
		assert.Equal(t, expected, string(receivedData))
	})

	t.Run("without offset", func(t *testing.T) {
		readReq := &bytestream.ReadRequest{
			ResourceName: readResourceName,
		}

		readClient, err := bsClient.Read(ctx, readReq)
		if err != nil {
			t.Fatalf("Failed to create read client: %v", err)
		}

		var receivedData []byte
		for {
			resp, err := readClient.Recv()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			receivedData = append(receivedData, resp.Data...)
		}

		assert.Equal(t, string(testData), string(receivedData))
	})
}

func TestFindMissingBlobs(t *testing.T) {
	ctx := context.Background()
	ts, tc := newTestServer(t)

	conn := ts.newClient(ctx, t)
	casClient := remoteexecution.NewContentAddressableStorageClient(conn)

	testData := []byte("existing blob data")
	digest := sha256.Sum256(testData)
	encoded := hex.EncodeToString(digest[:])
	tc.content[encoded] = testData

	missingData := []byte("missing blob data")
	missingDigest := sha256.Sum256(missingData)
	missingEncoded := hex.EncodeToString(missingDigest[:])

	req := &remoteexecution.FindMissingBlobsRequest{
		BlobDigests: []*remoteexecution.Digest{
			{Hash: encoded, SizeBytes: int64(len(testData))},
			{Hash: missingEncoded, SizeBytes: int64(len(missingData))},
		},
	}

	resp, err := casClient.FindMissingBlobs(ctx, req)
	require.NoError(t, err)
	require.Len(t, resp.MissingBlobDigests, 1, "Expected 1 missing blob")
	require.Equal(t, resp.MissingBlobDigests[0].Hash, missingEncoded, "Unexpected missing blob hash")
}
