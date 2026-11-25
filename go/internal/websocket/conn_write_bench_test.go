package websocket

import (
	"bufio"
	"testing"
)

func newDiscardServerConn() *Conn {
	rc := newRecConn()
	rc.discard = true
	return newConn(rc, bufio.NewReaderSize(rc, 4096), false, nil)
}

func benchWriteMessage(b *testing.B, size int) {
	c := newDiscardServerConn()
	data := repeatByte('w', size)
	b.ReportAllocs()
	b.SetBytes(int64(size))
	for b.Loop() {
		if err := c.WriteMessage(KindBinary, data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConn_WriteMessage_16B(b *testing.B)   { benchWriteMessage(b, 16) }
func BenchmarkConn_WriteMessage_1KiB(b *testing.B)  { benchWriteMessage(b, 1024) }
func BenchmarkConn_WriteMessage_64KiB(b *testing.B) { benchWriteMessage(b, 64*1024) }
func BenchmarkConn_WriteMessage_1MiB(b *testing.B)  { benchWriteMessage(b, 1<<20) }

func BenchmarkConn_WriteMessage_NonVectored_16B(b *testing.B)  { benchWriteMessage(b, 16) }
func BenchmarkConn_WriteMessage_NonVectored_1KiB(b *testing.B) { benchWriteMessage(b, 1024) }

func BenchmarkConn_NextWriter(b *testing.B) {
	c := newDiscardServerConn()
	data := repeatByte('w', 512)
	b.ReportAllocs()
	b.SetBytes(1024)
	for b.Loop() {
		w, err := c.NextWriter(KindBinary)
		if err != nil {
			b.Fatal(err)
		}
		_, _ = w.Write(data)
		_, _ = w.Write(data)
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConn_WritePrepared(b *testing.B) {
	c := newDiscardServerConn()
	pm, err := NewPreparedMessage(KindBinary, repeatByte('p', 1024))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(1024)
	for b.Loop() {
		if err := c.WritePrepared(pm); err != nil {
			b.Fatal(err)
		}
	}
}
