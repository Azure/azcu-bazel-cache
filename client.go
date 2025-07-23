package bazelazblob

import (
	"context"
	"io"
)

type BlobClient interface {
	Uploader(container, prefix string) Uploader
	Downloader(container, prefix string) Downloader
}

type BlobID struct {
	Hash      string
	SizeBytes int64
}

func newBlobID(hash string, sizeBytes int64) BlobID {
	return BlobID{
		Hash:      hash,
		SizeBytes: sizeBytes,
	}
}

type Uploader interface {
	Upload(ctx context.Context, id BlobID, contentSize int64, rdr io.Reader, offset int64) error
	Status(ctx context.Context, id BlobID) (BlobInfo, error)
}

type BlobInfo struct {
	AvailableBytes int64
	// AvailableIndex is the index of the last continuous block of data
	AvailableIndex int
	Blocks         []BlockInfo
}

type BlockInfo struct {
	Offset int64
	Size   int64
	ID     string
}

type Downloader interface {
	Download(ctx context.Context, id BlobID, offset, count int64) (io.ReadCloser, error)
	Exists(ctx context.Context, id BlobID) (bool, error)
}
