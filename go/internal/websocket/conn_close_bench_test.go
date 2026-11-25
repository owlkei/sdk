package websocket

import (
	"bufio"
	"testing"
)

func BenchmarkConn_PingPong(b *testing.B) {
	stream := append(clientFrame(testOpPing, true, []byte("12345678")), clientFrame(testOpText, true, []byte("hello"))...)
	rc := newLoopConn(stream)
	c := newConn(rc, bufio.NewReaderSize(rc, 4096), false, &ConnOptions{MaxPingsPerSecond: 1 << 30})
	buf := make([]byte, 0, 64)
	b.ReportAllocs()
	for b.Loop() {
		if _, _, err := c.ReadMessage(buf); err != nil {
			b.Fatal(err)
		}
	}
}
