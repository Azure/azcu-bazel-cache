package bazelazblob

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path"
	"slices"
	"strconv"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type azblobClient struct {
	az *azblob.Client
}

func (c *azblobClient) Uploader(container, prefix string) Uploader {
	return &azblobUploader{az: c.az, container: container, prefix: prefix}
}

func (c *azblobClient) Downloader(container, prefix string) Downloader {
	return &azblobDownloader{az: c.az, container: container, prefix: prefix}
}

type azblobUploader struct {
	az        *azblob.Client
	container string
	prefix    string
}

type bufReadCloser struct {
	*bytes.Reader
}

func (b *bufReadCloser) Close() error {
	// No-op for bufio.Reader, as it does not hold any resources that need to be released
	return nil
}

func (b *azblobUploader) concurrentUpload(ctx context.Context, sem *semaphore.Weighted, group *errgroup.Group, blockID string, dt []byte, client *blockblob.Client) error {
	if err := sem.Acquire(ctx, 1); err != nil {
		return status.FromContextError(ctx.Err()).Err()
	}

	// Make an internal copy so the caller can reuse their buffer.
	bufPtr := bufPool.Get().(*[]byte)

	buf := *bufPtr
	n := copy(buf, dt)

	bufReader := &bufReadCloser{Reader: bytes.NewReader(buf[:n])}
	group.Go(func() error {
		defer func() {
			bufPool.Put(bufPtr)
			sem.Release(1)
		}()
		return b.doUpload(ctx, client, bufReader, blockID)
	})

	return nil
}

func (b *azblobUploader) doUpload(ctx context.Context, client *blockblob.Client, rdr io.ReadSeekCloser, blockID string) error {
	err := retryWithBackoff(ctx, defaultRetryConfig, func() error {
		if _, err := rdr.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("failed to seek reader to start: %w", err)
		}
		_, err := client.StageBlock(ctx, blockID, rdr, nil)
		return err
	})
	if err != nil {
		return fmt.Errorf("failed to stage block %s: %w", blockID, err)
	}
	return nil
}

const azBlockPrefix = "offset-"

type blockReader struct {
	rdr    io.Reader
	nr     int64
	offset int64
	size   int64
	buf    []byte

	n       int
	err     error
	blockID string
}

func (br *blockReader) Next() bool {
	if br.err != nil {
		return false
	}

	// Block ID's must be unique (for the *blob*) and base64 encoded.
	// We use the offset as a unique identifier here.
	br.blockID = offsetToBlockID(br.offset + br.TotalRead())

	trunc := min(len(br.buf), int(br.size-br.offset-br.nr))
	n, err := io.ReadFull(br.rdr, br.buf[:trunc])
	if n > 0 {
		br.nr += int64(n)
		br.n = n
	}
	br.err = err
	return n > 0
}

func (br *blockReader) Bytes() []byte {
	if br.n == 0 {
		return nil
	}
	return br.buf[:br.n]
}

func (br *blockReader) Err() error {
	if errors.Is(br.err, io.EOF) {
		// If we hit EOF, we consider it a successful read.
		return nil
	}
	return br.err
}

func (br *blockReader) TotalRead() int64 {
	return br.nr
}

func (br *blockReader) BlockID() string {
	return br.blockID
}

// validateChunks ensures that the provided block IDs are valid and sequential,
// and that their sizes match the expected total size.
//
// It is expected that the block IDs are sorted.
func validateChunks(blockIDs []string, chunks map[string]*BlockInfo, expectedSize int64) error {
	var nextOffset int64
	for _, id := range blockIDs {
		offset, err := blockIDToOffset(id)
		if err != nil {
			return fmt.Errorf("failed to parse block ID %q: %w", id, err)
		}

		if offset < 0 || offset >= expectedSize {
			return fmt.Errorf("invalid block ID %q with offset %d, expected offset in range [0, %d)", id, offset, expectedSize)
		}

		if offset != nextOffset {
			return fmt.Errorf("gap in block IDs: expected offset %d, got %d from block ID %q", nextOffset, offset, id)
		}

		chunk, ok := chunks[id]
		if !ok {
			panic(fmt.Sprintf("missing size for block ID %q", id))
		}
		nextOffset += chunk.Size
	}

	if nextOffset != expectedSize {
		return fmt.Errorf("size mismatch: expected %d bytes, got %d bytes from block IDs", expectedSize, nextOffset)
	}
	return nil
}

func (u *azblobUploader) Upload(ctx context.Context, hash string, size int64, rdr io.Reader, offset int64) error {
	if offset != 0 {
		return status.Errorf(codes.InvalidArgument, "offset must be 0 for initial upload, resumable upload not supported: got offset %d", offset)
	}

	if hash == emptySHA256Hash {
		return nil
	}

	// This function breaks the upload into blocks, each of which is uploaded separately.
	// The block ID is based on the offset, which is used to ensure that the blocks
	// are unique and can be sorted. The block ID is base64 encoded to ensure it
	// is a valid string for Azure Blob Storage.
	// The block ID is of the form "offset-<offset>", where <offset>.
	// This allows us to both get some deduplication and support resuming uploads.
	name := path.Join(u.prefix, digestFuncString, hash, strconv.FormatInt(size, 10))

	const (
		maxConcurrency     = 16
		largeFileThreshold = 100 * 1024 * 1024 // 100MB
	)

	bufPtr := bufPool.Get().(*[]byte)
	defer bufPool.Put(bufPtr)
	buf := *bufPtr

	blockSize := min(int64(len(buf)), size)
	blockIDs := make([]string, 0, size/blockSize)

	origOffset := offset

	client := u.az.ServiceClient().NewContainerClient(u.container).NewBlockBlobClient(name)
	bufReader := &bufReadCloser{Reader: bytes.NewReader(nil)}

	var chunks map[string]*BlockInfo

	// Only bother with checking the status if the file is large enough or if we have an offset.
	// Otherwise this is just an extra unnecessary round trip to the server.
	if size > largeFileThreshold || offset > 0 {
		info, err := u.status(ctx, client)
		if err != nil {
			if !isBlobNotFoundError(err) {
				return fmt.Errorf("error querying blob status: %w", err)
			}
		}
		if info.AvailableBytes < offset {
			return status.Errorf(codes.InvalidArgument, "offset %d is greater than available bytes %d for blob %s", offset, info.AvailableBytes, name)
		}

		chunks = make(map[string]*BlockInfo, len(info.Blocks))
		for _, block := range info.Blocks {
			chunks[block.ID] = &block
			if block.Offset < offset {
				blockIDs = append(blockIDs, block.ID)
			}
		}
	}

	concurrency := 1
	var (
		sem   *semaphore.Weighted
		group *errgroup.Group
		// Use separate context for errgroup so that the main context is not cancelled
		// when group.Wait() is called since we still need to use that to commit the block list.
		groupCtx = ctx
	)
	if size > largeFileThreshold {
		concurrency = min(maxConcurrency, int((size-offset)/blockSize))
		// errgroup supports setting a limit on the number of concurrent goroutines
		// However, need use a separate semaphore to help limit stuff before we get to the errgroup
		sem = semaphore.NewWeighted(int64(concurrency))
		group, groupCtx = errgroup.WithContext(ctx)

		// Acquire a semaphore for the main goroutine to ensure we don't start too many goroutines at once
		if err := sem.Acquire(ctx, 1); err != nil {
			return status.FromContextError(ctx.Err()).Err()
		}
		defer sem.Release(1)
	}

	br := &blockReader{
		rdr:    rdr,
		offset: origOffset,
		size:   size,
		buf:    buf,
	}

	for br.Next() {
		dt := br.Bytes()
		blockID := br.BlockID()

		chunk, exists := chunks[blockID]
		if exists && chunk.Size == int64(len(dt)) {
			// If the block already exists and has the same size, we can skip uploading it.
			slog.DebugContext(ctx, "skipping existing block", "blockID", blockID, "size", len(dt))
			continue
		}

		blockIDs = append(blockIDs, blockID)
		if concurrency > 1 {
			if err := u.concurrentUpload(groupCtx, sem, group, blockID, dt, client); err != nil {
				return err
			}
			chunks[blockID] = &BlockInfo{
				Offset: br.offset + br.TotalRead() - int64(len(dt)),
				Size:   int64(len(dt)),
				ID:     blockID,
			}
			continue
		}

		bufReader.Reset(dt)
		if err := u.doUpload(ctx, client, bufReader, blockID); err != nil {
			return err
		}
		if chunks != nil {
			chunks[blockID] = &BlockInfo{
				Offset: br.offset + br.TotalRead() - int64(len(dt)),
				Size:   int64(len(dt)),
				ID:     blockID,
			}
		}
	}

	if err := br.Err(); err != nil {
		s, ok := status.FromError(err)
		if ok {
			return s.Err()
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return status.Errorf(codes.InvalidArgument, "failed to read data for block %s: %v", br.BlockID(), err)
		}
		return status.Errorf(codes.Internal, "failed to read data for block %s: %v", br.BlockID(), err)
	}

	if group != nil {
		if err := group.Wait(); err != nil {
			return err
		}
	}

	if nr := br.TotalRead(); nr != size-origOffset {
		return status.Errorf(codes.InvalidArgument, "size mismatch: expected %d, got %d", size, nr)
	}

	sortBlockIDs(blockIDs)

	// if chunks is not set there isn't much use in validating them our block ID's since those would
	// already be consistent.
	if chunks != nil {
		if err := validateChunks(blockIDs, chunks, size); err != nil {
			return fmt.Errorf("failed to validate chunks, this is probably a bug in the remote cache implementation: %w", err)
		}
	}
	err := retryWithBackoff(ctx, defaultRetryConfig, func() error {
		_, err := client.CommitBlockList(ctx, blockIDs, nil)
		return err
	})
	if err != nil {
		return err
	}
	return nil
}

func sortBlockIDs(blockIDs []string) error {
	var err error
	slices.SortFunc(blockIDs, func(a, b string) int {
		var (
			aOffset, bOffset int64
		)
		aOffset, err = blockIDToOffset(a)
		if err != nil {
			return -1 // Sort invalid IDs to the end
		}
		bOffset, err = blockIDToOffset(b)
		if err != nil {
			return 1 // Sort invalid IDs to the end
		}
		if aOffset < bOffset {
			return -1
		}
		if aOffset > bOffset {
			return 1
		}
		return 0
	})
	return err
}

func offsetToBlockID(offset int64) string {
	// We use a fixed prefix to ensure the block ID is unique and can be sorted.
	// The prefix is used to ensure that the block IDs are valid base64 strings.
	return base64.StdEncoding.EncodeToString(fmt.Appendf(nil, "%s%012d", azBlockPrefix, offset))
}

func blockIDToOffset(blockID string) (int64, error) {
	decoded, err := base64.StdEncoding.DecodeString(blockID)
	if err != nil {
		return 0, fmt.Errorf("failed to decode block ID %q: %w", blockID, err)
	}
	decodedStr := string(decoded)
	var offset int64
	_, err = fmt.Sscanf(decodedStr, azBlockPrefix+"%12d", &offset)
	if err != nil {
		return 0, fmt.Errorf("failed to parse offset from block ID %q: %w", decodedStr, err)
	}
	return offset, nil
}

func (u *azblobUploader) status(ctx context.Context, client *blockblob.Client) (info BlobInfo, _ error) {
	var gap bool

	err := retryWithBackoff(ctx, defaultRetryConfig, func() error {
		resp, err := client.GetBlockList(ctx, blockblob.BlockListTypeUncommitted, nil)
		if err != nil {
			return err
		}

		if len(resp.UncommittedBlocks) == 0 {
			return nil
		}

		blocks := resp.UncommittedBlocks
		slices.SortFunc(blocks, func(a, b *blockblob.Block) int {
			var (
				aOffset, bOffset int64
			)
			aOffset, err = blockIDToOffset(*a.Name)
			if err != nil {
				return -1
			}

			bOffset, err = blockIDToOffset(*b.Name)
			if err != nil {
				return -1
			}

			if aOffset < bOffset {
				return -1
			}
			if aOffset > bOffset {
				return 1
			}
			return 0
		})

		if err != nil {
			return fmt.Errorf("failed to sort blocks: %w", err)
		}

		var i int
		for _, block := range blocks {
			name := *block.Name
			offset, err := blockIDToOffset(name)
			if err != nil {
				slog.WarnContext(ctx, "failed to parse block ID, skipping", "blockID", name, "error", err)
				continue
			}

			info.Blocks = append(info.Blocks, BlockInfo{
				Offset: offset,
				Size:   *block.Size,
				ID:     name,
			})

			if offset != info.AvailableBytes {
				if !gap {
					gap = true
					// We have a gap in the blocks, we cannot return more than what we already proccessed.
					slog.DebugContext(ctx, "gap in block offsets, stopping", "expectedOffset", info.AvailableBytes, "foundOffset", offset, "blockID", name)
				}
				continue
			}

			info.AvailableBytes += *block.Size
			info.AvailableIndex = i
			i++
		}
		return nil
	})
	return info, err
}

func (u *azblobUploader) Status(ctx context.Context, hash string, size int64) (info BlobInfo, err error) {
	if hash == emptySHA256Hash {
		return BlobInfo{AvailableBytes: 0}, nil
	}

	name := path.Join(u.prefix, digestFuncString, hash, strconv.FormatInt(size, 10))
	c := u.az.ServiceClient().NewContainerClient(u.container).NewBlockBlobClient(name)

	info, err = u.status(ctx, c)
	if isBlobNotFoundError(err) {
		return BlobInfo{}, status.Error(codes.NotFound, err.Error())
	}
	return info, err
}

type azblobDownloader struct {
	az        *azblob.Client
	container string
	prefix    string
}

func (d *azblobDownloader) Download(ctx context.Context, hash string, size, offset, count int64) (io.ReadCloser, error) {
	if hash == emptySHA256Hash {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}

	name := path.Join(d.prefix, digestFuncString, hash, strconv.FormatInt(size, 10))

	var opts *azblob.DownloadStreamOptions
	if offset > 0 || count > 0 {
		opts = &azblob.DownloadStreamOptions{
			Range: azblob.HTTPRange{
				Offset: offset,
				Count:  count,
			},
		}
	}

	var rc io.ReadCloser

	err := retryWithBackoff(ctx, defaultRetryConfig, func() error {
		resp, err := d.az.DownloadStream(ctx, d.container, name, opts)
		if err != nil {
			return err
		}
		rc = resp.Body
		return nil
	})

	if err != nil && isBlobNotFoundError(err) {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return rc, err
}

func (d *azblobDownloader) Exists(ctx context.Context, hash string, size int64) (bool, error) {
	if hash == emptySHA256Hash {
		return true, nil // Empty hash is always considered existing.
	}

	name := path.Join(d.prefix, digestFuncString, hash, strconv.FormatInt(size, 10))
	c := d.az.ServiceClient().NewContainerClient(d.container).NewBlobClient(name)

	var exists bool
	err := retryWithBackoff(ctx, defaultRetryConfig, func() error {
		_, err := c.GetProperties(ctx, nil)
		if err == nil {
			exists = true
			return nil
		}
		if isBlobNotFoundError(err) {
			return nil
		}
		return err
	})

	return exists, err
}

func NewAzBlobClient(client *azblob.Client) BlobClient {
	return &azblobClient{az: client}
}
