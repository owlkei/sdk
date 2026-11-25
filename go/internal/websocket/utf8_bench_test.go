package websocket

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func benchmarkUTF8(b *testing.B, sample string, size int) {
	data := []byte(strings.Repeat(sample, size/len(sample)+1))[:size]
	for len(data) > 0 && !utf8.Valid(data) {
		data = data[:len(data)-1]
	}
	b.Run("State", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(data)))
		for b.Loop() {
			var s utf8State
			s.validate(data)
		}
	})
	b.Run("Stdlib", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(data)))
		for b.Loop() {
			utf8.Valid(data)
		}
	})
}

func BenchmarkUTF8_ASCII_1KiB(b *testing.B)      { benchmarkUTF8(b, "hello world ", 1024) }
func BenchmarkUTF8_ASCII_64KiB(b *testing.B)     { benchmarkUTF8(b, "hello world ", 64*1024) }
func BenchmarkUTF8_Vietnamese_1KiB(b *testing.B) { benchmarkUTF8(b, "Tiếng Việt có dấu ", 1024) }
func BenchmarkUTF8_Vietnamese_64KiB(b *testing.B) {
	benchmarkUTF8(b, "Tiếng Việt có dấu ", 64*1024)
}
func BenchmarkUTF8_CJK_1KiB(b *testing.B)  { benchmarkUTF8(b, "日本語のテキスト", 1024) }
func BenchmarkUTF8_CJK_64KiB(b *testing.B) { benchmarkUTF8(b, "日本語のテキスト", 64*1024) }
