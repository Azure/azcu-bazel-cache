package bazelazblob

import (
	"bytes"
	"context"
	"io"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type testBlobClient struct {
	mu      sync.Mutex
	content map[string][]byte // digest -> data
}

func (c *testBlobClient) Uploader(container, prefix string) Uploader {
	return c
}

func (c *testBlobClient) Downloader(container, prefix string) Downloader {
	return c
}

func (c *testBlobClient) Upload(ctx context.Context, id BlobID, contentSize int64, r io.Reader, offset int64) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.content[id.Hash] = data
	c.mu.Unlock()
	return nil
}

func (c *testBlobClient) Download(ctx context.Context, id BlobID, offset, count int64) (io.ReadCloser, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	data, exists := c.content[id.Hash]
	if !exists {
		return nil, status.Error(codes.NotFound, "blob not found")
	}

	rdr := bytes.NewReader(data)
	if offset == 0 && count == 0 {
		return io.NopCloser(rdr), nil
	}

	section := io.NewSectionReader(bytes.NewReader(data), offset, count)
	return io.NopCloser(section), nil
}

func (c *testBlobClient) Exists(ctx context.Context, id BlobID) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, exists := c.content[id.Hash]
	return exists, nil
}

func (c *testBlobClient) Status(ctx context.Context, id BlobID) (BlobInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return BlobInfo{
		AvailableBytes: int64(len(c.content[id.Hash])),
	}, nil
}
