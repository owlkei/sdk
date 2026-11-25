package websocket

import "testing"

func benchmarkMask(b *testing.B, size int) {
	key := [4]byte{0x37, 0xfa, 0x21, 0x3d}
	b.Run("Word", func(b *testing.B) {
		buf := make([]byte, size)
		b.ReportAllocs()
		b.SetBytes(int64(size))
		for b.Loop() {
			maskBytes(buf, key, 0)
		}
	})
	b.Run("Bytewise", func(b *testing.B) {
		buf := make([]byte, size)
		b.ReportAllocs()
		b.SetBytes(int64(size))
		for b.Loop() {
			maskBytewise(buf, key, 0)
		}
	})
}

func BenchmarkMaskBytes_16B(b *testing.B)   { benchmarkMask(b, 16) }
func BenchmarkMaskBytes_125B(b *testing.B)  { benchmarkMask(b, 125) }
func BenchmarkMaskBytes_1KiB(b *testing.B)  { benchmarkMask(b, 1024) }
func BenchmarkMaskBytes_64KiB(b *testing.B) { benchmarkMask(b, 64*1024) }
