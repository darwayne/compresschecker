package tests

import (
	"bytes"
	"compress/gzip"
	"errors"
	"github.com/darwayne/compresschecker"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zip"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReaderCheckAgainstFiles(t *testing.T) {
	tests := []struct {
		expect compresschecker.CompressionType
		path   string
		name   string
	}{
		{
			expect: compresschecker.Parquet,
			path:   "userdata1.parquet",
			name:   "should handle parquet",
		},
		{
			expect: compresschecker.Avro,
			path:   "userdata1.avro",
			name:   "should handle avro",
		},
		{
			expect: compresschecker.Avro,
			path:   "userdata2.avro",
			name:   "should handle avro",
		},
		{
			expect: compresschecker.Avro,
			path:   "userdata3.avro",
			name:   "should handle avro",
		},
		{
			expect: compresschecker.Avro,
			path:   "userdata4.avro",
			name:   "should handle avro",
		},
		{
			expect: compresschecker.Avro,
			path:   "userdata5.avro",
			name:   "should handle avro",
		},
		{
			expect: compresschecker.X7zip,
			path:   "7z2407-src.7z",
			name:   "should handle 7zip",
		},
		{
			expect: compresschecker.BZip2,
			path:   "file.bz2",
			name:   "should handle bzip2",
		},
		{
			expect: compresschecker.Xz,
			path:   "7z2407-linux-arm.tar.xz",
			name:   "should handle xz",
		},
		{
			expect: compresschecker.Rar,
			path:   "sample-1.rar",
			name:   "should handle rar",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := os.Open(filepath.Join("testdata", tt.path))
			require.NoError(t, err)
			t.Cleanup(func() {
				f.Close()
			})

			checker := compresschecker.NewReadChecker(f)
			require.NoError(t, checker.Err())
			require.Equal(t, tt.expect, checker.CompressionType())
			t.Cleanup(func() {
				checker.Close()
			})
		})
	}
}

func TestReaderCheck(t *testing.T) {
	t.Run("should return reader that reads expected data", func(t *testing.T) {
		const expectedUnCompressed = "ok then what it do"
		fake := strings.NewReader(expectedUnCompressed)
		checkMe := compresschecker.NewReadChecker(fake)
		t.Cleanup(func() {
			checkMe.Close()
		})
		compression, err := checkMe.Check()
		require.NoError(t, err)
		require.Equal(t, compression, compresschecker.None)

		yo := bytes.NewBuffer(nil)
		_, err = io.Copy(yo, checkMe)
		require.NoError(t, err)
		require.Equal(t, expectedUnCompressed, yo.String())
	})

	t.Run("detect gzip", func(t *testing.T) {
		const expected = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
		buff := bytes.NewBuffer(nil)
		writer := gzip.NewWriter(buff)
		_, err := writer.Write([]byte(expected))
		require.NoError(t, err)
		writer.Close()

		size := buff.Len()
		info := buff.Bytes()

		buff2 := bytes.NewBuffer(nil)
		checkMe := compresschecker.NewReadChecker(buff)
		t.Cleanup(func() {
			checkMe.Close()
		})
		compression, err := checkMe.Check()
		require.NoError(t, err)
		require.Equal(t, compression, compresschecker.Gzip)
		readSize, err := io.Copy(buff2, checkMe)
		require.NoError(t, err)
		require.Equal(t, int64(size), readSize)
		require.Equal(t, buff2.Bytes(), info)
	})

	t.Run("detect snappy", func(t *testing.T) {
		const expected = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
		buff := bytes.NewBuffer(nil)
		writer := snappy.NewBufferedWriter(buff)
		_, err := writer.Write([]byte(expected))
		require.NoError(t, err)
		writer.Close()

		size := buff.Len()
		info := buff.Bytes()

		buff2 := bytes.NewBuffer(nil)
		checkMe := compresschecker.NewReadChecker(buff)
		t.Cleanup(func() {
			checkMe.Close()
		})
		compression, err := checkMe.Check()
		require.NoError(t, err)
		require.Equal(t, compression, compresschecker.Snappy)
		readSize, err := io.Copy(buff2, checkMe)
		require.NoError(t, err)
		require.Equal(t, int64(size), readSize)
		require.Equal(t, buff2.Bytes(), info)
	})

	t.Run("detect zstd", func(t *testing.T) {
		const expected = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
		buff := bytes.NewBuffer(nil)
		writer, err := zstd.NewWriter(buff)
		require.NoError(t, err)
		_, err = writer.Write([]byte(expected))
		require.NoError(t, err)
		writer.Close()

		size := buff.Len()
		info := buff.Bytes()

		buff2 := bytes.NewBuffer(nil)
		checkMe := compresschecker.NewReadChecker(buff)
		t.Cleanup(func() {
			checkMe.Close()
		})
		compression, err := checkMe.Check()
		require.NoError(t, err)
		require.Equal(t, compression, compresschecker.Zstd)
		readSize, err := io.Copy(buff2, checkMe)
		require.NoError(t, err)
		require.Equal(t, int64(size), readSize)
		require.Equal(t, buff2.Bytes(), info)
	})

	t.Run("detect zip", func(t *testing.T) {
		const expected = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
		buff := bytes.NewBuffer(nil)
		writera := zip.NewWriter(buff)
		writer, err := writera.Create("yo.son")
		require.NoError(t, err)
		_, err = writer.Write([]byte(expected))
		require.NoError(t, err)
		writera.Close()

		size := buff.Len()
		info := buff.Bytes()

		buff2 := bytes.NewBuffer(nil)
		checkMe := compresschecker.NewReadChecker(buff)
		t.Cleanup(func() {
			checkMe.Close()
		})
		compression, err := checkMe.Check()
		require.NoError(t, err)
		require.Equal(t, compression, compresschecker.Zip)
		readSize, err := io.Copy(buff2, checkMe)
		require.NoError(t, err)
		require.Equal(t, int64(size), readSize)
		require.Equal(t, buff2.Bytes(), info)
	})

	t.Run("should close underlying reader if it is a read closer", func(t *testing.T) {
		t.Run("should call close", func(t *testing.T) {
			info := &customReadCloser{Reader: bytes.NewBuffer(nil)}
			yo := compresschecker.NewReadChecker(info)
			err := yo.Close()
			require.NoError(t, err)
			require.True(t, info.closed)
		})

		t.Run("should return close error", func(t *testing.T) {
			info := &customReadCloser{Reader: bytes.NewBuffer(nil), err: errors.New("yo")}
			yo := compresschecker.NewReadChecker(info)
			err := yo.Close()
			require.Error(t, err)
			require.True(t, info.closed)
			require.ErrorContains(t, err, "yo")
		})
	})
}

type customReadCloser struct {
	io.Reader
	closed bool
	err    error
}

func (c *customReadCloser) Close() error {
	c.closed = true
	return c.err
}

func TestEarlyEOF(t *testing.T) {
	buff := bytes.NewBuffer([]byte("a"))
	reader := compresschecker.NewReadChecker(buff)
	require.NoError(t, reader.Err())
}
