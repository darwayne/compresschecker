package compresschecker

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"sync"
	"sync/atomic"
)

// NewReadChecker generates a ReadChecker that allows callers
// to understand if a given io.Reader is compressed or not.
//
// Callers should use the returned type for future read operations
// since this operation will read the first few bytes of the reader
// to understand the compression type
func NewReadChecker(r io.Reader) *ReadChecker {
	result := readCheckerPool.Get().(*ReadChecker)
	result.Reset(r)
	return result
}

// ReadChecker allows you to check if an io.Reader is compressed or not
type ReadChecker struct {
	io.Reader
	reader *bufio.Reader
	myType CompressionType
	closed int32
	err    error
}

func (r *ReadChecker) IsCompressed() bool {
	return r.myType != None
}

func (r *ReadChecker) Err() error {
	return r.err
}

func (r *ReadChecker) Reset(rr io.Reader) *ReadChecker {
	r.err = nil
	atomic.StoreInt32(&r.closed, 0)
	r.reader = bufioReaderPool.Get().(*bufio.Reader)
	r.reader.Reset(rr)
	info, err := r.reader.Peek(maxSize)
	if err != nil && !errors.Is(err, io.EOF) {
		r.err = err
		return r
	}

	r.myType = FormatOfBytes(info)

	r.Reader = r.reader
	return r
}
func (r *ReadChecker) Check() (CompressionType, error) {
	return r.myType, r.err
}

func (r *ReadChecker) CompressionType() CompressionType {
	return r.myType
}

func (r *ReadChecker) Close() error {
	if atomic.LoadInt32(&r.closed) == 1 {
		return nil
	}
	atomic.StoreInt32(&r.closed, 1)
	bufioReaderPool.Put(r.reader)
	readCheckerPool.Put(r)
	return nil
}

type CompressionType int

const (
	None CompressionType = iota
	Snappy
	Gzip
	Zstd
	Zip
	Parquet
	Avro
	X7zip
	BZip2
	Xz
	Rar
)

func (c CompressionType) IsCompressed() bool {
	return c != None
}

func (c CompressionType) String() string {
	switch c {
	case None:
		return "None"
	case Snappy:
		return "Snappy"
	case Gzip:
		return "Gzip"
	case Zstd:
		return "Zstd"
	case Parquet:
		return "Parquet"
	case Avro:
		return "Avro"
	case X7zip:
		return "7z"
	case Xz:
		return "xz"
	case Rar:
		return "Rar"
	case BZip2:
		return "Bzip2"
	default:
		return "unknown"
	}
}

const maxSize = 7

var (
	gzipMagic    = []byte{0x1f, 0x8b}
	zipMagic     = []byte{0x50, 0x4B, 0x03, 0x04}
	zstdMagic    = []byte{0x28, 0xB5, 0x2F, 0xFD}
	snappyMagic  = []byte{0xFF, 0x06, 0x00, 0x00}
	parquetMagic = []byte{'P', 'A', 'R', '1'}
	avroMagic    = []byte{'O', 'b', 'j', 0x01}
	_7zipMagic   = []byte{0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C}
	bzip2Magic   = []byte{0x42, 0x5A, 0x68}
	xzMagic      = []byte{0xFD, 0x37, 0x7A, 0x58, 0x5A, 0x00}
	rarMagic     = []byte{0x52, 0x61, 0x72, 0x21, 0x1A, 0x07, 0x00}
)

var bufioReaderPool = sync.Pool{New: func() any {
	return bufio.NewReader(bytes.NewReader(nil))
}}
var readCheckerPool = sync.Pool{New: func() any {
	return &ReadChecker{}
}}
