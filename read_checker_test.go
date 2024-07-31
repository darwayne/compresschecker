package compresschecker

import (
	"bytes"
	"compress/gzip"
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
		expect CompressionType
		path   string
		name   string
	}{
		{
			expect: Parquet,
			path:   "userdata1.parquet",
			name:   "should handle parquet",
		},
		{
			expect: Avro,
			path:   "userdata1.avro",
			name:   "should handle avro",
		},
		{
			expect: Avro,
			path:   "userdata2.avro",
			name:   "should handle avro",
		},
		{
			expect: Avro,
			path:   "userdata3.avro",
			name:   "should handle avro",
		},
		{
			expect: Avro,
			path:   "userdata4.avro",
			name:   "should handle avro",
		},
		{
			expect: Avro,
			path:   "userdata5.avro",
			name:   "should handle avro",
		},
		{
			expect: X7zip,
			path:   "7z2407-src.7z",
			name:   "should handle 7zip",
		},
		{
			expect: BZip2,
			path:   "file.bz2",
			name:   "should handle bzip2",
		},
		{
			expect: Xz,
			path:   "7z2407-linux-arm.tar.xz",
			name:   "should handle xz",
		},
		{
			expect: Rar,
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

			checker := NewReadChecker(f)
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
		checkMe := NewReadChecker(fake)
		t.Cleanup(func() {
			checkMe.Close()
		})
		compression, err := checkMe.Check()
		require.NoError(t, err)
		require.Equal(t, compression, None)

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
		checkMe := NewReadChecker(buff)
		t.Cleanup(func() {
			checkMe.Close()
		})
		compression, err := checkMe.Check()
		require.NoError(t, err)
		require.Equal(t, compression, Gzip)
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
		checkMe := NewReadChecker(buff)
		t.Cleanup(func() {
			checkMe.Close()
		})
		compression, err := checkMe.Check()
		require.NoError(t, err)
		require.Equal(t, compression, Snappy)
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
		checkMe := NewReadChecker(buff)
		t.Cleanup(func() {
			checkMe.Close()
		})
		compression, err := checkMe.Check()
		require.NoError(t, err)
		require.Equal(t, compression, Zstd)
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
		checkMe := NewReadChecker(buff)
		t.Cleanup(func() {
			checkMe.Close()
		})
		compression, err := checkMe.Check()
		require.NoError(t, err)
		require.Equal(t, compression, Zip)
		readSize, err := io.Copy(buff2, checkMe)
		require.NoError(t, err)
		require.Equal(t, int64(size), readSize)
		require.Equal(t, buff2.Bytes(), info)
	})
}

func TestEarlyEOF(t *testing.T) {
	buff := bytes.NewBuffer([]byte("a"))
	reader := NewReadChecker(buff)
	require.NoError(t, reader.Err())
}

func BenchmarkCheckFinal(b *testing.B) {
	yo := strings.NewReader("oh snap what upoh snap what up")
	b.Run("check", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			yo.Seek(0, 0)
			y := NewReadChecker(yo)
			aa, bb := y.Check()
			y.Close()
			_, _ = aa, bb
		}
	})
}

func BenchmarkFormatOfBytes(b *testing.B) {
	yo := []byte("oh snap what is going on")
	b.Run("check", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			FormatOfBytes(yo)
		}
	})
}

func BenchmarkFormatOfString(b *testing.B) {
	yo := "oh snap what is going on"
	b.Run("check", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			FormatOfString(yo)
		}
	})
}

func BenchmarkStringToBytesV1(b *testing.B) {
	const yo = "what is up son"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		stringToBytes(yo)
	}
}
