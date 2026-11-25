package websocket

import (
	"bufio"
	"io"
	"testing"
)

func benchReadMessage(b *testing.B, size int) {
	frame := clientFrame(testOpBinary, true, repeatByte('r', size))
	rc := newLoopConn(frame)
	c := newConn(rc, bufio.NewReaderSize(rc, 4096), false, &ConnOptions{MaxMessageBytes: max(size, 64*1024)})
	buf := make([]byte, 0, size)
	b.ReportAllocs()
	b.SetBytes(int64(size))
	for b.Loop() {
		if _, _, err := c.ReadMessage(buf); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConn_ReadMessage_16B(b *testing.B)   { benchReadMessage(b, 16) }
func BenchmarkConn_ReadMessage_1KiB(b *testing.B)  { benchReadMessage(b, 1024) }
func BenchmarkConn_ReadMessage_64KiB(b *testing.B) { benchReadMessage(b, 64*1024) }
func BenchmarkConn_ReadMessage_1MiB(b *testing.B)  { benchReadMessage(b, 1<<20) }

func BenchmarkConn_ReadMessage_Fragmented(b *testing.B) {
	var stream []byte
	stream = append(stream, clientFrame(testOpBinary, false, repeatByte('f', 128))...)
	for range 6 {
		stream = append(stream, clientFrame(testOpCont, false, repeatByte('f', 128))...)
	}
	stream = append(stream, clientFrame(testOpCont, true, repeatByte('f', 128))...)
	rc := newLoopConn(stream)
	c := newConn(rc, bufio.NewReaderSize(rc, 4096), false, nil)
	buf := make([]byte, 0, 1024)
	b.ReportAllocs()
	b.SetBytes(1024)
	for b.Loop() {
		if _, _, err := c.ReadMessage(buf); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConn_NextReader_1MiB(b *testing.B) {
	size := 1 << 20
	rc := newLoopConn(clientFrame(testOpBinary, true, repeatByte('n', size)))
	c := newConn(rc, bufio.NewReaderSize(rc, 4096), false, &ConnOptions{MaxMessageBytes: size})
	p := make([]byte, 64*1024)
	b.ReportAllocs()
	b.SetBytes(int64(size))
	for b.Loop() {
		_, r, err := c.NextReader()
		if err != nil {
			b.Fatal(err)
		}
		for {
			_, err := r.Read(p)
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}
