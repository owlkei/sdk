package websocket

import (
	"bufio"
	"bytes"
	"errors"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"go.owlkei.com/sdk/internal/test"
)

func TestLoadConnOptions_Defaults(t *testing.T) {
	want := ConnOptions{
		MaxMessageBytes:   64 * 1024,
		MaxFrameBytes:     64 * 1024,
		MaxFragments:      128,
		MaxPingsPerSecond: 32,
		IdleTimeout:       60 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		CloseTimeout:      5 * time.Second,
	}
	for name, in := range map[string]*ConnOptions{"nil": nil, "zero": {}} {
		got, err := loadConnOptions(in)
		if err != nil || got != want {
			t.Error(test.DiffMessage(got, want, "L1, H1: defaults for "+name+" options"))
		}
	}
}

func TestLoadConnOptions_FrameLimitFollowsMessageLimit(t *testing.T) {
	got, err := loadConnOptions(&ConnOptions{MaxMessageBytes: 1000})
	if err != nil || got.MaxFrameBytes != 1000 {
		t.Error(test.DiffMessage(got.MaxFrameBytes, 1000, "L1: MaxFrameBytes defaults to MaxMessageBytes"))
	}
	got, err = loadConnOptions(&ConnOptions{MaxMessageBytes: 1000, MaxFrameBytes: 10, WriteTimeout: time.Second})
	if err != nil || got.MaxFrameBytes != 10 || got.WriteTimeout != time.Second || got.MaxMessageBytes != 1000 {
		t.Error(test.DiffMessage(got, "explicit values kept", "L1: explicit values are kept"))
	}
}

func TestLoadConnOptions_Invalid(t *testing.T) {
	cases := map[string]*ConnOptions{
		"MaxMessageBytes":   {MaxMessageBytes: -1},
		"MaxFrameBytes":     {MaxFrameBytes: -1},
		"MaxFragments":      {MaxFragments: -1},
		"MaxPingsPerSecond": {MaxPingsPerSecond: -1},
		"IdleTimeout":       {IdleTimeout: -time.Second},
		"ReadTimeout":       {ReadTimeout: -time.Second},
		"WriteTimeout":      {WriteTimeout: -time.Second},
		"CloseTimeout":      {CloseTimeout: -time.Second},
	}
	for field, o := range cases {
		_, err := loadConnOptions(o)
		if !isErr(err, ErrInvalidOptions) || !strings.Contains(err.Error(), field) {
			t.Error(test.DiffMessage(err, "ErrInvalidOptions naming "+field, "H1: a negative value"))
		}
	}
	_, err := loadConnOptions(&ConnOptions{MaxMessageBytes: 100, MaxFrameBytes: 101})
	if !isErr(err, ErrInvalidOptions) || !strings.Contains(err.Error(), "MaxFrameBytes") {
		t.Error(test.DiffMessage(err, "ErrInvalidOptions naming MaxFrameBytes", "H1: MaxFrameBytes over MaxMessageBytes"))
	}
}

func TestLoadConnOptions_MessageLimitOver1GiB(t *testing.T) {
	_, err := loadConnOptions(&ConnOptions{MaxMessageBytes: 1<<30 + 1})
	if !isErr(err, ErrInvalidOptions) || !strings.Contains(err.Error(), "MaxMessageBytes") {
		t.Error(test.DiffMessage(err, "ErrInvalidOptions naming MaxMessageBytes", "H1, T25: over 1 GiB"))
	}
	if _, err := loadConnOptions(&ConnOptions{MaxMessageBytes: 1 << 30}); err != nil {
		t.Error(test.DiffMessage(err, nil, "H1: exactly 1 GiB is valid"))
	}
}

func TestConn_Read_IdleCutAtIdleTimeout(t *testing.T) {
	c, _ := newPipeConn(t, false, connOpts(func(o *ConnOptions) { o.IdleTimeout = 50 * time.Millisecond }))
	var err error
	took := elapsed(func() { _, _, err = c.ReadMessage(nil) })
	checkErr(t, "L2, T7: no byte within IdleTimeout", err, ErrTimeout, "websocket: timeout")
	if took < 40*time.Millisecond || took > 5*time.Second {
		t.Error(test.DiffMessage(took, "about 50ms", "L2: cut at IdleTimeout"))
	}
	if _, _, err := c.ReadMessage(nil); !isErr(err, ErrClosed) {
		t.Error(test.DiffMessage(err, ErrClosed, "X1: aborted after the timeout"))
	}
}

func TestConn_Read_PongRestartsIdle(t *testing.T) {
	c, peer := newPipeConn(t, false, connOpts(func(o *ConnOptions) { o.IdleTimeout = 250 * time.Millisecond }))
	done := make(chan error, 1)
	go func() {
		for range 7 {
			if _, err := peer.Write(clientFrame(testOpPong, true, nil)); err != nil {
				done <- err
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		_, err := peer.Write(clientFrame(testOpText, true, []byte("alive")))
		done <- err
	}()
	_, data, err := c.ReadMessage(nil)
	if err != nil || string(data) != "alive" {
		t.Error(test.DiffMessage(err, nil, "L2: a pong restarts the idle wait"))
	}
	if err := <-done; err != nil {
		t.Error(err)
	}
}

func TestConn_Read_ByteASecondCutAtReadTimeout(t *testing.T) {
	c, peer := newPipeConn(t, false, connOpts(func(o *ConnOptions) {
		o.ReadTimeout = 100 * time.Millisecond
		o.IdleTimeout = 10 * time.Second
	}))
	done := feedSlowly(peer, clientFrame(testOpBinary, true, repeatByte('s', 1000)), 5*time.Millisecond)
	var err error
	took := elapsed(func() { _, _, err = c.ReadMessage(nil) })
	checkErr(t, "L3, T6: a slow sender", err, ErrTimeout, "websocket: timeout")
	if took < 80*time.Millisecond || took > 3*time.Second {
		t.Error(test.DiffMessage(took, "about 100ms", "L3: cut at ReadTimeout from the first byte"))
	}
	<-done
}

func TestConn_Read_SlowHeaderCutAtReadTimeout(t *testing.T) {
	c, peer := newPipeConn(t, false, connOpts(func(o *ConnOptions) {
		o.ReadTimeout = 50 * time.Millisecond
		o.IdleTimeout = 10 * time.Second
		o.MaxMessageBytes = 1 << 20
	}))
	done := feedSlowly(peer, rawHeader(0x82, true, 8, 100000), 20*time.Millisecond)
	var err error
	took := elapsed(func() { _, _, err = c.ReadMessage(nil) })
	checkErr(t, "L3, T6: a header sent a byte at a time", err, ErrTimeout, "websocket: timeout")
	if took < 40*time.Millisecond || took > 3*time.Second {
		t.Error(test.DiffMessage(took, "about 50ms", "L3: cut at ReadTimeout"))
	}
	<-done
}

func TestConn_Read_SlowControlFrameCutAtReadTimeout(t *testing.T) {
	c, peer := newPipeConn(t, false, connOpts(func(o *ConnOptions) {
		o.ReadTimeout = 50 * time.Millisecond
		o.IdleTimeout = 10 * time.Second
	}))
	done := feedSlowly(peer, clientFrame(testOpPing, true, repeatByte('c', 125)), 20*time.Millisecond)
	var err error
	took := elapsed(func() { _, _, err = c.ReadMessage(nil) })
	checkErr(t, "L3: a control frame outside a message", err, ErrTimeout, "websocket: timeout")
	if took < 40*time.Millisecond || took > 3*time.Second {
		t.Error(test.DiffMessage(took, "about 50ms", "L3"))
	}
	<-done
}

func TestConn_Read_FragmentsDoNotExtendDeadline(t *testing.T) {
	c, peer := newPipeConn(t, false, connOpts(func(o *ConnOptions) {
		o.ReadTimeout = 100 * time.Millisecond
		o.IdleTimeout = 10 * time.Second
		o.MaxFragments = 1 << 20
	}))
	done := feedGenerated(peer, func(i int) []byte {
		if i == 0 {
			return clientFrame(testOpText, false, []byte("a"))
		}
		return clientFrame(testOpCont, false, []byte("a"))
	}, 10*time.Millisecond)
	var err error
	took := elapsed(func() { _, _, err = c.ReadMessage(nil) })
	checkErr(t, "L3, T4: more fragments do not extend the deadline", err, ErrTimeout, "websocket: timeout")
	if took < 80*time.Millisecond || took > 3*time.Second {
		t.Error(test.DiffMessage(took, "about 100ms", "L3: absolute deadline"))
	}
	<-done
}

func TestConn_Read_PingFloodBoundsPongs(t *testing.T) {
	chunks := make([][]byte, 0, 1001)
	for range 1000 {
		chunks = append(chunks, clientFrame(testOpPing, true, []byte("f")))
	}
	chunks = append(chunks, clientFrame(testOpText, true, []byte("done")))
	c, rc := newRecServerConn(connOpts(func(o *ConnOptions) { o.MaxPingsPerSecond = 5 }), chunks...)
	_, data, err := c.ReadMessage(nil)
	if err != nil || string(data) != "done" {
		t.Fatal(test.DiffMessage(err, nil, "L4: excess pings are dropped and the connection stays open"))
	}
	frames := onlyFrames(t, "L4", rc, false)
	if len(frames) != 5 {
		t.Error(test.DiffMessage(len(frames), 5, "L4, T9: pongs written in one window with MaxPingsPerSecond 5"))
	}
	for _, f := range frames {
		if f.Opcode != testOpPong || string(f.Payload) != "f" {
			t.Error(test.DiffMessage(f, "a pong of the ping", "C1"))
		}
	}
	if rc.closes() != 0 {
		t.Error(test.DiffMessage(rc.closes(), 0, "L4: the connection stays open"))
	}
}

func TestConn_Read_FloodsDoNotGrowMemory(t *testing.T) {
	const n = 100000
	last := clientFrame(testOpText, true, []byte("end"))
	cases := map[string][]byte{
		"pings": append(bytes.Repeat(clientFrame(testOpPing, true, []byte("p")), n), last...),
		"pongs": append(bytes.Repeat(clientFrame(testOpPong, true, []byte("p")), n), last...),
		"empty fragments": append(append(clientFrame(testOpText, false, nil), bytes.Repeat(clientFrame(testOpCont, false, nil), n)...),
			clientFrame(testOpCont, true, []byte("end"))...),
	}
	for name, stream := range cases {
		c, _ := newRecServerConn(connOpts(func(o *ConnOptions) { o.MaxFragments = 2 * n }), stream)
		runtime.GC()
		before := totalAlloc()
		_, data, err := c.ReadMessage(nil)
		after := totalAlloc()
		if err != nil || string(data) != "end" {
			t.Fatal(test.DiffMessage(err, nil, "L5: flood of "+name))
		}
		if after-before > 64*1024 {
			t.Error(test.DiffMessage(after-before, "under 64 KiB", "L5, T9: bytes allocated while reading a flood of "+name))
		}
	}
}

func TestConn_IdleMemory(t *testing.T) {
	const n = 1000
	rcs := make([]*recConn, n)
	brs := make([]*bufio.Reader, n)
	for i := range n {
		rcs[i] = newRecConn()
		brs[i] = bufio.NewReaderSize(rcs[i], 4096)
	}
	conns := make([]*Conn, n)
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := range n {
		conns[i] = newConn(rcs[i], brs[i], false, nil)
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(conns)
	runtime.KeepAlive(brs)
	per := (int64(after.HeapAlloc) - int64(before.HeapAlloc)) / n
	if per > 1024 {
		t.Error(test.DiffMessage(per, "at most 1024", "L5: bytes owned by the package per idle connection"))
	}
}

func TestConn_StartsNoGoroutine(t *testing.T) {
	u := openUpgrader(t)
	before := runtime.NumGoroutine()
	c, _, _, err := upgradeOverRec(t, u, validRequest(),
		clientFrame(testOpText, true, []byte("one")),
		clientFrame(testOpPing, true, []byte("p")),
		clientFrame(testOpBinary, true, []byte("two")),
		peerClose(1000, ""),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.ReadMessage(nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.ReadMessage(nil); err != nil {
		t.Fatal(err)
	}
	if err := c.WriteMessage(KindText, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := c.Ping([]byte("y")); err != nil {
		t.Fatal(err)
	}
	var ce *CloseError
	if _, _, err := c.ReadMessage(nil); !errors.As(err, &ce) {
		t.Fatal(err)
	}
	c.Abort()
	after := runtime.NumGoroutine()
	if after > before {
		t.Error(test.DiffMessage(after, before, "L6: goroutines after Upgrade, reads, writes, pings and a close"))
	}
}

func TestConn_Arm_CloseDuringDeadlineSetKeepsCloseDeadline(t *testing.T) {
	c, rc := newRecServerConn(nil)
	closeAt := time.Now().Add(time.Hour)
	var once sync.Once
	rc.onReadDL = func() {
		once.Do(func() {
			c.closeDeadline.Store(closeAt.UnixNano())
			c.state.Store(stateClosing)
		})
	}
	c.arm(time.Now().Add(time.Minute))
	rdl, _ := rc.deadlines()
	if !rdl.Equal(time.Unix(0, closeAt.UnixNano())) {
		t.Error(test.DiffMessage(rdl, closeAt, "C7: a Close between the deadline set and the re-check keeps the close deadline"))
	}
	if !c.armed.IsZero() {
		t.Error(test.DiffMessage(c.armed, time.Time{}, "C7: the armed deadline is forgotten once closing"))
	}
}
