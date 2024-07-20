package compresschecker

import (
	"bytes"
	"io"
	"unsafe"
)

// FormatOfString returns the format of the provided string
func FormatOfString(str string) CompressionType {
	return FormatOfBytes(stringToBytes(str))
}

// FormatOfBytes returns the format of the provided bytes
func FormatOfBytes(info []byte) CompressionType {
	switch {
	case bytes.HasPrefix(info, gzipMagic):
		return Gzip
	case bytes.HasPrefix(info, zipMagic):
		return Zip
	case bytes.HasPrefix(info, zstdMagic):
		return Zstd
	case bytes.HasPrefix(info, snappyMagic):
		return Snappy
	case bytes.HasPrefix(info, parquetMagic):
		return Parquet
	case bytes.HasPrefix(info, avroMagic):
		return Avro
	case bytes.HasPrefix(info, _7zipMagic):
		return X7zip
	case bytes.HasPrefix(info, bzip2Magic):
		return BZip2
	case bytes.HasPrefix(info, xzMagic):
		return Xz
	case bytes.HasPrefix(info, rarMagic):
		return Rar
	default:
		return None
	}
}

// FormatOfReader returns the format of the provided reader and a new io.ReadCloser
// Since this requires reading the first few bytes of the reader you should use the
// returned reader for subsequent read operations.
// note: the returned io.ReadCloser should be closed after read operations are complete
func FormatOfReader(reader io.Reader) (CompressionType, io.ReadCloser) {
	info := NewReadChecker(reader)
	return info.CompressionType(), info
}

func stringToBytes(s string) []byte {
	// Convert the string to a byte slice without allocating new memory
	return *(*[]byte)(unsafe.Pointer(&s))
}
