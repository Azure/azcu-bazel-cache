package bazelazblob

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"

	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	streamBufferSize = 1024 * 1024 // 1MB
)

var (
	streamReaderPool = &sync.Pool{
		New: func() any {
			return &byteStreamReader{msg: &bytestream.WriteRequest{}}
		},
	}

	streamWriterPool = &sync.Pool{
		New: func() any {
			return &byteStreamWriter{msg: &bytestream.ReadResponse{}}
		},
	}

	bufPool = &sync.Pool{
		New: func() any {
			buf := make([]byte, streamBufferSize)
			return &buf
		},
	}
)

// writeStream writes the data from the bytestream server to Azure Blob Storage.
// The provided emptyDigest is used to store the digest of the uploaded blob.
func (s *AzureBlobServer) writeStream(ctx context.Context, client Uploader, srv bytestream.ByteStream_WriteServer) (int64, error) {
	// The way the bytestream API works is it sends a stream of messages
	// where the first message contains metadata about the resource being uploaded,
	// and subsequent messages contain the actual data.
	// The first message contains the resource name, which is used to determine the
	// blob name in Azure Blob Storage, that includes the hash and size of the blob.
	// The information is used to create a content addressable blob name in Azure Blob Storage.
	bsr := streamReaderPool.Get().(*byteStreamReader)
	defer func() {
		bsr.Reset()
		streamReaderPool.Put(bsr)
	}()

	bsr.srv = srv
	if err := srv.RecvMsg(bsr.msg); err != nil {
		if errors.Is(err, io.EOF) {
			return -1, status.Error(codes.InvalidArgument, io.ErrUnexpectedEOF.Error())
		}
		return -1, status.Errorf(codes.Internal, "failed to receive message: %v", err)
	}

	bsr.buf = bsr.msg.Data

	hash, size, err := parseByteStreamResourceName(bsr.msg.ResourceName)
	if err != nil {
		return -1, status.Errorf(codes.InvalidArgument, "failed to parse resource name: %v", err)
	}

	err = client.Upload(ctx, hash, size, bsr, bsr.msg.WriteOffset)
	if err != nil {
		return -1, err
	}
	return size, nil
}

// Format: "uploads/{uuid}/blobs/{hash}/{size}" or "blobs/{hash}/{size}"
func parseByteStreamResourceName(resourceName string) (string, int64, error) {
	kind, remain, ok := strings.Cut(resourceName, "/")
	if !ok {
		return "", -1, fmt.Errorf("invlaid resource name format: %s", resourceName)
	}

	var hashSize string
	switch kind {
	case "blobs":
		hashSize = remain
	case "uploads":
		_, hashSize, ok = strings.Cut(remain, "/blobs/")
		if !ok {
			return "", -1, fmt.Errorf("invalid resource name format: %s", resourceName)
		}
	default:
		return "", -1, fmt.Errorf("unknown resource kind %q for resource name: %s", kind, resourceName)
	}

	hash, sizeStr, ok := strings.Cut(hashSize, "/")
	if !ok {
		return "", -1, fmt.Errorf("invalid resource name format: %s", resourceName)
	}

	size, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		return "", -1, fmt.Errorf("could not parse blob size from %q, resource name %q: %w", sizeStr, resourceName, err)
	}
	return hash, size, nil
}

// copyBuffer is similar to io.Copy but it tries to read the full buffer size
// from the reader before writing to the writer and tries to optimize based on the expected size.
// This helps prevent excessive chunking when writing.
func copyBuffer(w io.Writer, rdr *io.LimitedReader) (int64, error) {
	bufPtr := bufPool.Get().(*[]byte)
	defer bufPool.Put(bufPtr)
	buf := *bufPtr

	if rdr.N <= 0 {
		panic("copyBuffer called with non-positive size")
	}

	var total int64
	for {
		// If there's nothing left to read, return early.
		// io.ReadFull (as used below) will return 0 bytes read without an error
		// if the we are requesting 0 bytes.
		if rdr.N == 0 {
			return total, nil // No more data to read
		}

		// Use ReadFull here to try and get as much data as possible before making a write call.
		// Otherwise we could end up with many small writes if the reader returns small chunks.
		trunc := min(int64(len(buf)), rdr.N)
		nr, err := io.ReadFull(rdr, buf[:trunc])
		if nr > 0 {
			total += int64(nr)
		}

		if err != nil {
			if errors.Is(err, io.EOF) {
				return total, nil
			}
			return total, err
		}

		nw, err := w.Write(buf[:nr])
		if err != nil {
			return total, fmt.Errorf("failed to write data: %w", err)
		}
		if nw != nr {
			return total, fmt.Errorf("short write: expected %d, got %d", nr, nw)
		}
	}
}

type byteStreamWriter struct {
	srv bytestream.ByteStream_ReadServer
	msg *bytestream.ReadResponse
}

func (w *byteStreamWriter) Write(p []byte) (n int, err error) {
	w.msg.Reset()
	w.msg.Data = p
	if err := w.srv.SendMsg(w.msg); err != nil {
		return 0, status.Errorf(codes.Internal, "failed to send data: %v", err)
	}
	return len(p), nil
}

func (w *byteStreamWriter) Reset() {
	w.msg.Reset()
	w.srv = nil
}

type byteStreamReader struct {
	srv bytestream.ByteStream_WriteServer
	msg *bytestream.WriteRequest
	buf []byte
}

func (r *byteStreamReader) Read(p []byte) (int, error) {
	if len(r.buf) > 0 {
		n := copy(p, r.buf)
		r.buf = r.buf[n:]
		return n, nil
	}

	r.msg.Reset()
	err := r.srv.RecvMsg(r.msg)
	if err != nil {
		return 0, err
	}

	n := copy(p, r.msg.Data)
	if n < len(r.msg.Data) {
		// If the data didn't fit into p, buffer the remaining data for the next read
		r.buf = r.msg.Data[n:]
	}

	return n, nil
}

func (s *byteStreamReader) Reset() {
	s.srv = nil
	s.msg.Reset()
	s.buf = nil
}
