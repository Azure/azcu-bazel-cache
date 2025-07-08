package bazelazblob

import (
	"context"
	"io"
)

type BlobClient interface {
	Uploader(container, prefix string) Uploader
	Downloader(container, prefix string) Downloader
}

type Uploader interface {
	Upload(ctx context.Context, expectedHash string, size int64, rdr io.Reader, offset int64) error
	Status(ctx context.Context, hash string, size int64) (BlobInfo, error)
}

type BlobInfo struct {
	AvailableBytes int64
	// AvailbleIndex is the index of the last continuous block of data
	AvailableIndex int
	Blocks         []BlockInfo
}

type BlockInfo struct {
	Offset int64
	Size   int64
	ID     string
}

type Downloader interface {
	Download(ctx context.Context, hash string, size, offset, count int64) (io.ReadCloser, error)
	Exists(ctx context.Context, hash string, size int64) (bool, error)
}
