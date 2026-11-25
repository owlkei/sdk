package websocket

import (
	"testing"
)

func BenchmarkUpgrader_Upgrade(b *testing.B) {
	u := openUpgrader(b)
	r := validRequest()
	b.ReportAllocs()
	for b.Loop() {
		rc := newRecConn()
		rc.discard = true
		w := newHijackWriter(rc)
		c, err := u.Upgrade(w, r)
		if err != nil {
			b.Fatal(err)
		}
		_ = c
	}
}
