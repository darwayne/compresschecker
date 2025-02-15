package compresschecker

import (
	"strings"
	"testing"
)

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
