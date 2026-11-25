package websocket

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.owlkei.com/sdk/internal/test"
)

const sampleAccept = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="

const plain101 = "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + sampleAccept + "\r\n\r\n"

func refuse(t *testing.T, u *Upgrader, mutate func(*http.Request)) (*httptest.ResponseRecorder, error) {
	t.Helper()
	r := validRequest()
	if mutate != nil {
		mutate(r)
	}
	rec := httptest.NewRecorder()
	c, err := u.Upgrade(rec, r)
	if c != nil {
		t.Error(test.DiffMessage(c, nil, "a refused handshake returns no Conn"))
		c.Abort()
	}
	return rec, err
}

func expectRefusal(t *testing.T, rule string, rec *httptest.ResponseRecorder, err error, status int, class error, text string) {
	t.Helper()
	checkErr(t, rule, err, class, text)
	if rec.Code != status {
		t.Error(test.DiffMessage(rec.Code, status, rule+": status"))
	}
	if got := rec.Header().Get("Content-Type"); got != "text/plain; charset=utf-8" {
		t.Error(test.DiffMessage(got, "text/plain; charset=utf-8", rule+": Content-Type"))
	}
	if got := rec.Header().Get("X-Content-Type-Options"); got != "nosniff" {
		t.Error(test.DiffMessage(got, "nosniff", rule+": X-Content-Type-Options"))
	}
	if got := rec.Header().Get("Connection"); got != "close" {
		t.Error(test.DiffMessage(got, "close", rule+": Connection"))
	}
	if got := rec.Body.String(); got != refusalBody(status) {
		t.Error(test.DiffMessage(got, refusalBody(status), rule+": body is the fixed status text"))
	}
}

func badHandshake(detail string) string {
	return "websocket: bad handshake: " + detail
}

func succeedWith(t *testing.T, rule string, u *Upgrader, mutate func(*http.Request)) (*Conn, *recConn) {
	t.Helper()
	r := validRequest()
	if mutate != nil {
		mutate(r)
	}
	c, w, rc, err := upgradeOverRec(t, u, r)
	if err != nil || c == nil {
		t.Fatal(test.DiffMessage(err, nil, rule+": the handshake succeeds"))
	}
	if w.wrote {
		t.Error(test.DiffMessage("written through the ResponseWriter", "written to the hijacked connection", rule))
	}
	return c, rc
}

func TestNewUpgrader_NilOptions(t *testing.T) {
	u, err := NewUpgrader(nil)
	if err != nil || u == nil {
		t.Error(test.DiffMessage(err, nil, "H1: NewUpgrader(nil) is valid"))
	}
}

func TestNewUpgrader_NegativeTimeout(t *testing.T) {
	_, err := NewUpgrader(&UpgradeOptions{HandshakeTimeout: -time.Second})
	if !isErr(err, ErrInvalidOptions) || !strings.Contains(err.Error(), "HandshakeTimeout") {
		t.Error(test.DiffMessage(err, "ErrInvalidOptions naming HandshakeTimeout", "H1"))
	}
	_, err = NewUpgrader(&UpgradeOptions{Conn: &ConnOptions{ReadTimeout: -1}})
	if !isErr(err, ErrInvalidOptions) || !strings.Contains(err.Error(), "ReadTimeout") {
		t.Error(test.DiffMessage(err, "ErrInvalidOptions naming ReadTimeout", "H1: a negative ConnOptions value"))
	}
}

func TestNewUpgrader_TooManySubprotocols(t *testing.T) {
	var many []string
	for i := range 17 {
		many = append(many, "p"+itoa(i))
	}
	_, err := NewUpgrader(&UpgradeOptions{Subprotocols: many})
	if !isErr(err, ErrInvalidOptions) || !strings.Contains(err.Error(), "Subprotocols") {
		t.Error(test.DiffMessage(err, "ErrInvalidOptions naming Subprotocols", "H1: 17 subprotocols"))
	}
	if _, err := NewUpgrader(&UpgradeOptions{Subprotocols: many[:16]}); err != nil {
		t.Error(test.DiffMessage(err, nil, "H1: 16 subprotocols are valid"))
	}
}

func TestNewUpgrader_SubprotocolNotToken(t *testing.T) {
	for _, bad := range []string{"", "has space", "a,b", "quo\"te", "tab\t", strings.Repeat("a", 65), "café"} {
		_, err := NewUpgrader(&UpgradeOptions{Subprotocols: []string{"ok", bad}})
		if !isErr(err, ErrInvalidOptions) || !strings.Contains(err.Error(), "Subprotocols[1]") {
			t.Error(test.DiffMessage(err, "ErrInvalidOptions naming Subprotocols[1]", "H1: entry of "+itoa(len(bad))+" bytes"))
		}
	}
	if _, err := NewUpgrader(&UpgradeOptions{Subprotocols: []string{strings.Repeat("a", 64), "chat.v1", "x-y_z"}}); err != nil {
		t.Error(test.DiffMessage(err, nil, "H1: valid tokens up to 64 bytes"))
	}
}

func TestNewUpgrader_DuplicateSubprotocol(t *testing.T) {
	_, err := NewUpgrader(&UpgradeOptions{Subprotocols: []string{"chat", "other", "chat"}})
	if !isErr(err, ErrInvalidOptions) || !strings.Contains(err.Error(), "Subprotocols[2]") {
		t.Error(test.DiffMessage(err, "ErrInvalidOptions naming Subprotocols[2]", "H1: a duplicate"))
	}
}

func TestNewUpgrader_FrameLimitOverMessageLimit(t *testing.T) {
	_, err := NewUpgrader(&UpgradeOptions{Conn: &ConnOptions{MaxMessageBytes: 10, MaxFrameBytes: 11}})
	if !isErr(err, ErrInvalidOptions) || !strings.Contains(err.Error(), "MaxFrameBytes") {
		t.Error(test.DiffMessage(err, "ErrInvalidOptions naming MaxFrameBytes", "H1"))
	}
}

func TestNewUpgrader_MessageLimitOver1GiB(t *testing.T) {
	_, err := NewUpgrader(&UpgradeOptions{Conn: &ConnOptions{MaxMessageBytes: 1<<30 + 1}})
	if !isErr(err, ErrInvalidOptions) || !strings.Contains(err.Error(), "MaxMessageBytes") {
		t.Error(test.DiffMessage(err, "ErrInvalidOptions naming MaxMessageBytes", "H1, T25"))
	}
	if _, err := NewUpgrader(&UpgradeOptions{Conn: &ConnOptions{MaxMessageBytes: 1 << 30}}); err != nil {
		t.Error(test.DiffMessage(err, nil, "H1: exactly 1 GiB is valid"))
	}
}

func TestNewUpgrader_ErrorNamesFieldNotValue(t *testing.T) {
	_, err := NewUpgrader(&UpgradeOptions{Subprotocols: []string{"chat", leakMarker + " bad value"}})
	want := "websocket: invalid options: UpgradeOptions.Subprotocols[1] is not a token"
	checkErr(t, "H1, E1: the option path, never the value", err, ErrInvalidOptions, want)
}

func TestNewUpgrader_CopiesOptions(t *testing.T) {
	protocols := []string{"chat"}
	conn := &ConnOptions{MaxMessageBytes: 10}
	opts := &UpgradeOptions{CheckOrigin: allowAll, Subprotocols: protocols, Conn: conn}
	u := mustUpgrader(t, opts)
	protocols[0] = "other"
	conn.MaxMessageBytes = 1 << 20
	opts.CheckOrigin = nil
	opts.Subprotocols = nil
	r := validRequest()
	r.Header.Set("Sec-WebSocket-Protocol", "chat")
	c, _, _, err := upgradeOverRec(t, u, r, clientFrame(testOpBinary, true, repeatByte('c', 11)))
	if err != nil {
		t.Fatal(test.DiffMessage(err, nil, "H1: the copy keeps CheckOrigin"))
	}
	if c.Subprotocol() != "chat" {
		t.Error(test.DiffMessage(c.Subprotocol(), "chat", "H1: the subprotocol list is copied"))
	}
	_, _, err = c.ReadMessage(nil)
	checkErr(t, "H1: the ConnOptions are copied", err, ErrMessageTooBig, "")
}

func TestUpgrader_Upgrade_RFCSampleKey(t *testing.T) {
	c, rc := succeedWith(t, "H2", openUpgrader(t), nil)
	defer c.Abort()
	out := string(rc.written())
	if !strings.Contains(out, "Sec-WebSocket-Accept: "+sampleAccept+"\r\n") {
		t.Error(test.DiffMessage(out, sampleAccept, "H2: the RFC sample key gives the RFC sample accept value"))
	}
	other := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef"))
	c2, rc2 := succeedWith(t, "H2", openUpgrader(t), func(r *http.Request) { r.Header.Set("Sec-WebSocket-Key", other) })
	defer c2.Abort()
	if strings.Contains(string(rc2.written()), sampleAccept) {
		t.Error(test.DiffMessage(string(rc2.written()), "another accept value", "H2: the accept value depends on the key"))
	}
}

func TestUpgrader_Upgrade_ExactResponseBytes(t *testing.T) {
	c, rc := succeedWith(t, "H2", openUpgrader(t), nil)
	defer c.Abort()
	expectBytes(t, "H2: the 101 is exactly these bytes", rc.written(), []byte(plain101))
	u := mustUpgrader(t, &UpgradeOptions{CheckOrigin: allowAll, Subprotocols: []string{"chat"}})
	c2, rc2 := succeedWith(t, "H2", u, func(r *http.Request) { r.Header.Set("Sec-WebSocket-Protocol", "chat") })
	defer c2.Abort()
	want := strings.Replace(plain101, "\r\n\r\n", "\r\nSec-WebSocket-Protocol: chat\r\n\r\n", 1)
	expectBytes(t, "H2: the subprotocol line follows the accept line", rc2.written(), []byte(want))
}

func TestUpgrader_Upgrade_RefusesHTTP10(t *testing.T) {
	rec, err := refuse(t, openUpgrader(t), func(r *http.Request) {
		r.Proto, r.ProtoMajor, r.ProtoMinor = "HTTP/1.0", 1, 0
	})
	expectRefusal(t, "H3.1, T16: HTTP/1.0", rec, err, 400, ErrBadHandshake, badHandshake("HTTP/1.1 is required"))
}

func TestUpgrader_Upgrade_RefusesHTTP2(t *testing.T) {
	rec, err := refuse(t, openUpgrader(t), func(r *http.Request) {
		r.Proto, r.ProtoMajor, r.ProtoMinor = "HTTP/2.0", 2, 0
	})
	checkErr(t, "H3.1, T16: HTTP/2", err, ErrBadHandshake, badHandshake("HTTP/1.1 is required"))
	if rec.Code != 400 || rec.Body.String() != refusalBody(400) {
		t.Error(test.DiffMessage(rec.Code, 400, "H3.1: status and body"))
	}
	if got := rec.Header().Get("Connection"); got != "" {
		t.Error(test.DiffMessage(got, "", "refuse: Connection: close only on HTTP/1.x"))
	}
}

func TestUpgrader_Upgrade_RefusesH2CPriorKnowledge(t *testing.T) {
	type outcome struct {
		err   error
		proto int
		conn  *Conn
	}
	u := openUpgrader(t)
	results := make(chan outcome, 1)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := u.Upgrade(w, r)
		results <- outcome{err, r.ProtoMajor, c}
	}))
	server := new(http.Protocols)
	server.SetHTTP1(true)
	server.SetUnencryptedHTTP2(true)
	srv.Config.Protocols = server
	srv.Start()
	t.Cleanup(srv.Close)
	clientProtocols := new(http.Protocols)
	clientProtocols.SetUnencryptedHTTP2(true)
	client := &http.Client{Transport: &http.Transport{Protocols: clientProtocols}}
	req, err := http.NewRequest(http.MethodGet, srv.URL+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	got := <-results
	if got.conn != nil {
		got.conn.Abort()
		t.Error(test.DiffMessage(got.conn, nil, "T16: no Conn over h2c"))
	}
	if got.proto != 2 {
		t.Fatal(test.DiffMessage(got.proto, 2, "the request arrived over HTTP/2 (h2c)"))
	}
	checkErr(t, "H3.1, T16: h2c with prior knowledge", got.err, ErrBadHandshake, badHandshake("HTTP/1.1 is required"))
	if resp.StatusCode != 400 {
		t.Error(test.DiffMessage(resp.StatusCode, 400, "T16: status"))
	}
}

func TestUpgrader_Upgrade_RefusesNonGET(t *testing.T) {
	for _, method := range []string{"POST", "PUT", "HEAD", "get"} {
		rec, err := refuse(t, openUpgrader(t), func(r *http.Request) { r.Method = method })
		expectRefusal(t, "H3.2: method "+method, rec, err, 400, ErrBadHandshake, badHandshake("method is not GET"))
	}
}

func TestUpgrader_Upgrade_RefusesRequestBody(t *testing.T) {
	rec, err := refuse(t, openUpgrader(t), func(r *http.Request) {
		r.ContentLength = 5
		r.Body = io.NopCloser(strings.NewReader("hello"))
	})
	expectRefusal(t, "H3.3, T12: a request body", rec, err, 400, ErrBadHandshake, badHandshake("request has a body"))
}

func TestUpgrader_Upgrade_RefusesChunkedBody(t *testing.T) {
	rec, err := refuse(t, openUpgrader(t), func(r *http.Request) {
		r.ContentLength = -1
		r.TransferEncoding = []string{"chunked"}
	})
	expectRefusal(t, "H3.3, T12: a chunked body", rec, err, 400, ErrBadHandshake, badHandshake("request has a body"))
}

func TestUpgrader_Upgrade_RefusesMissingConnectionUpgrade(t *testing.T) {
	for name, mutate := range map[string]func(*http.Request){
		"missing": func(r *http.Request) { r.Header.Del("Connection") },
		"close":   func(r *http.Request) { r.Header.Set("Connection", "close") },
		"keep":    func(r *http.Request) { r.Header.Set("Connection", "keep-alive") },
		"similar": func(r *http.Request) { r.Header.Set("Connection", "upgrades, xupgrade") },
	} {
		rec, err := refuse(t, openUpgrader(t), mutate)
		expectRefusal(t, "H3.4: Connection "+name, rec, err, 400, ErrBadHandshake, badHandshake("Connection is not Upgrade"))
	}
}

func TestUpgrader_Upgrade_AcceptsConnectionTokenList(t *testing.T) {
	for _, v := range []string{"keep-alive, Upgrade", "upgrade", "UPGRADE", "Keep-Alive,Upgrade,foo"} {
		c, _ := succeedWith(t, "H3.4: "+v, openUpgrader(t), func(r *http.Request) { r.Header.Set("Connection", v) })
		c.Abort()
	}
	c, _ := succeedWith(t, "H3.4: two Connection lines", openUpgrader(t), func(r *http.Request) {
		r.Header.Set("Connection", "keep-alive")
		r.Header.Add("Connection", "Upgrade")
	})
	c.Abort()
}

func TestUpgrader_Upgrade_RefusesUpgradeNotWebsocket(t *testing.T) {
	for _, v := range []string{"foo", "websockets", "", "web socket"} {
		rec, err := refuse(t, openUpgrader(t), func(r *http.Request) { r.Header.Set("Upgrade", v) })
		expectRefusal(t, "H3.5: Upgrade "+v, rec, err, 400, ErrBadHandshake, badHandshake("Upgrade is not websocket"))
	}
	rec, err := refuse(t, openUpgrader(t), func(r *http.Request) { r.Header.Del("Upgrade") })
	expectRefusal(t, "H3.5: no Upgrade", rec, err, 400, ErrBadHandshake, badHandshake("Upgrade is not websocket"))
	rec, err = refuse(t, openUpgrader(t), func(r *http.Request) { r.Header.Add("Upgrade", "websocket") })
	expectRefusal(t, "H3.5: two Upgrade lines", rec, err, 400, ErrBadHandshake, badHandshake("Upgrade is not websocket"))
}

func TestUpgrader_Upgrade_RefusesH2CUpgradeToken(t *testing.T) {
	for _, v := range []string{"websocket, h2c", "h2c", "h2c, websocket"} {
		rec, err := refuse(t, openUpgrader(t), func(r *http.Request) { r.Header.Set("Upgrade", v) })
		expectRefusal(t, "H3.5, T16: Upgrade "+v, rec, err, 400, ErrBadHandshake, badHandshake("Upgrade is not websocket"))
	}
}

func TestUpgrader_Upgrade_RefusesUpgradeWithTrailingComma(t *testing.T) {
	for _, v := range []string{"websocket,", "websocket, websocket", ",websocket"} {
		rec, err := refuse(t, openUpgrader(t), func(r *http.Request) { r.Header.Set("Upgrade", v) })
		expectRefusal(t, "H3.5: Upgrade "+v, rec, err, 400, ErrBadHandshake, badHandshake("Upgrade is not websocket"))
	}
}

func TestUpgrader_Upgrade_AcceptsUpgradeCaseInsensitive(t *testing.T) {
	c, _ := succeedWith(t, "H3.5", openUpgrader(t), func(r *http.Request) { r.Header.Set("Upgrade", "WebSocket") })
	c.Abort()
}

func TestUpgrader_Upgrade_RefusesQuotedTokenList(t *testing.T) {
	for _, v := range []string{`"Upgrade"`, `Upgrade, "keep-alive"`, `"keep-alive", Upgrade`} {
		rec, err := refuse(t, openUpgrader(t), func(r *http.Request) { r.Header.Set("Connection", v) })
		expectRefusal(t, "H3: a quote anywhere refuses, Connection "+v, rec, err, 400, ErrBadHandshake, badHandshake("Connection is not Upgrade"))
	}
}

func TestUpgrader_Upgrade_TokenListTrimsOnlySpaceAndTab(t *testing.T) {
	c, _ := succeedWith(t, "H3: space and tab are trimmed", openUpgrader(t), func(r *http.Request) {
		r.Header["Connection"] = []string{" \tkeep-alive \t,\t Upgrade \t"}
	})
	c.Abort()
	for _, v := range []string{"\vUpgrade", "Upgrade\v", " Upgrade", "Upgrade\x00"} {
		rec, err := refuse(t, openUpgrader(t), func(r *http.Request) { r.Header["Connection"] = []string{v} })
		expectRefusal(t, "H3: only SP and HTAB are trimmed", rec, err, 400, ErrBadHandshake, badHandshake("Connection is not Upgrade"))
	}
}

func TestUpgrader_Upgrade_WrongVersionGets426(t *testing.T) {
	for _, v := range []string{"12", "8", "14", "13 ", "013", "1.3"} {
		rec, err := refuse(t, openUpgrader(t), func(r *http.Request) { r.Header["Sec-Websocket-Version"] = []string{v} })
		expectRefusal(t, "H3.6: version "+v, rec, err, 426, ErrUnsupportedVersion, "websocket: unsupported version")
		if got := rec.Header().Get("Sec-WebSocket-Version"); got != "13" {
			t.Error(test.DiffMessage(got, "13", "H3.6: the 426 names the version supported"))
		}
	}
	rec, err := refuse(t, openUpgrader(t), func(r *http.Request) { r.Header.Add("Sec-WebSocket-Version", "13") })
	expectRefusal(t, "H3.6: two version lines", rec, err, 426, ErrUnsupportedVersion, "websocket: unsupported version")
}

func TestUpgrader_Upgrade_MissingVersionGets426(t *testing.T) {
	rec, err := refuse(t, openUpgrader(t), func(r *http.Request) { r.Header.Del("Sec-WebSocket-Version") })
	expectRefusal(t, "H3.6: no version", rec, err, 426, ErrUnsupportedVersion, "websocket: unsupported version")
	if got := rec.Header().Get("Sec-WebSocket-Version"); got != "13" {
		t.Error(test.DiffMessage(got, "13", "H3.6"))
	}
}

func refuseKey(t *testing.T, rule string, mutate func(*http.Request)) {
	t.Helper()
	rec, err := refuse(t, openUpgrader(t), mutate)
	expectRefusal(t, rule, rec, err, 400, ErrBadHandshake, badHandshake("invalid Sec-WebSocket-Key"))
}

func TestUpgrader_Upgrade_RefusesMissingKey(t *testing.T) {
	refuseKey(t, "H3.7: no key", func(r *http.Request) { r.Header.Del("Sec-WebSocket-Key") })
}

func TestUpgrader_Upgrade_RefusesShortKey(t *testing.T) {
	refuseKey(t, "H3.7: short key", func(r *http.Request) { r.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZQ==") })
}

func TestUpgrader_Upgrade_RefusesNonBase64Key(t *testing.T) {
	refuseKey(t, "H3.7: not base64", func(r *http.Request) { r.Header.Set("Sec-WebSocket-Key", "!!!!!!!!!!!!!!!!!!!!!!!!") })
}

func TestUpgrader_Upgrade_RefusesTwoKeys(t *testing.T) {
	refuseKey(t, "H3.7: two keys", func(r *http.Request) { r.Header.Add("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==") })
}

func TestUpgrader_Upgrade_RefusesKeyWithTrailingBits(t *testing.T) {
	refuseKey(t, "H3.7: non-strict base64", func(r *http.Request) { r.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZR==") })
}

func TestUpgrader_Upgrade_RefusesKeyOf15Or17Bytes(t *testing.T) {
	for _, n := range []int{15, 17, 0, 32} {
		key := base64.StdEncoding.EncodeToString(make([]byte, n))
		refuseKey(t, "H3.7: a key of "+itoa(n)+" bytes", func(r *http.Request) { r.Header.Set("Sec-WebSocket-Key", key) })
	}
}

func TestUpgrader_Upgrade_RefusesTooManyProtocols(t *testing.T) {
	var values []string
	for i := range 17 {
		values = append(values, "p"+itoa(i))
	}
	rec, err := refuse(t, openUpgrader(t), func(r *http.Request) { r.Header.Set("Sec-WebSocket-Protocol", strings.Join(values, ", ")) })
	expectRefusal(t, "H3.8, T13: 17 values", rec, err, 400, ErrBadHandshake, badHandshake("invalid Sec-WebSocket-Protocol"))
	rec, err = refuse(t, openUpgrader(t), func(r *http.Request) {
		for _, v := range values {
			r.Header.Add("Sec-WebSocket-Protocol", v)
		}
	})
	expectRefusal(t, "H3.8, T13: 17 lines", rec, err, 400, ErrBadHandshake, badHandshake("invalid Sec-WebSocket-Protocol"))
	c, _ := succeedWith(t, "H3.8: 16 values pass", openUpgrader(t), func(r *http.Request) { r.Header.Set("Sec-WebSocket-Protocol", strings.Join(values[:16], ",")) })
	c.Abort()
}

func TestUpgrader_Upgrade_RefusesLongProtocolHeader(t *testing.T) {
	rec, err := refuse(t, openUpgrader(t), func(r *http.Request) { r.Header.Set("Sec-WebSocket-Protocol", strings.Repeat("a", 1025)) })
	expectRefusal(t, "H3.8, T13: over 1024 bytes", rec, err, 400, ErrBadHandshake, badHandshake("invalid Sec-WebSocket-Protocol"))
	rec, err = refuse(t, openUpgrader(t), func(r *http.Request) {
		r.Header.Add("Sec-WebSocket-Protocol", strings.Repeat("a", 600))
		r.Header.Add("Sec-WebSocket-Protocol", strings.Repeat("b", 600))
	})
	expectRefusal(t, "H3.8, T13: lines together over 1024 bytes", rec, err, 400, ErrBadHandshake, badHandshake("invalid Sec-WebSocket-Protocol"))
}

func TestUpgrader_Upgrade_AcceptsProtocolHeaderAtLimit(t *testing.T) {
	c, _ := succeedWith(t, "H3.8: 1024 bytes", openUpgrader(t), func(r *http.Request) { r.Header.Set("Sec-WebSocket-Protocol", strings.Repeat("a", 1024)) })
	if c.Subprotocol() != "" {
		t.Error(test.DiffMessage(c.Subprotocol(), "", "H4"))
	}
	c.Abort()
}

func TestUpgrader_Upgrade_RefusesEmptyProtocolValue(t *testing.T) {
	rec, err := refuse(t, openUpgrader(t), func(r *http.Request) { r.Header["Sec-Websocket-Protocol"] = []string{""} })
	expectRefusal(t, "H3.8: an empty value", rec, err, 400, ErrBadHandshake, badHandshake("invalid Sec-WebSocket-Protocol"))
	for _, bad := range []string{`"chat"`, "has space", "a;b", "café"} {
		rec, err = refuse(t, openUpgrader(t), func(r *http.Request) { r.Header.Set("Sec-WebSocket-Protocol", bad) })
		expectRefusal(t, "H3.8: not a token "+bad, rec, err, 400, ErrBadHandshake, badHandshake("invalid Sec-WebSocket-Protocol"))
	}
}

func TestUpgrader_Upgrade_RefusesLongExtensionsHeader(t *testing.T) {
	rec, err := refuse(t, openUpgrader(t), func(r *http.Request) {
		r.Header.Set("Sec-WebSocket-Extensions", "permessage-deflate; "+strings.Repeat("x", 1100))
	})
	expectRefusal(t, "H3.9, T13: over 1024 bytes", rec, err, 400, ErrBadHandshake, badHandshake("invalid Sec-WebSocket-Extensions"))
}

func TestUpgrader_Upgrade_RefusesTooManyExtensions(t *testing.T) {
	var values []string
	for i := range 17 {
		values = append(values, "ext"+itoa(i))
	}
	rec, err := refuse(t, openUpgrader(t), func(r *http.Request) { r.Header.Set("Sec-WebSocket-Extensions", strings.Join(values, ", ")) })
	expectRefusal(t, "H3.9, T13: 17 entries", rec, err, 400, ErrBadHandshake, badHandshake("invalid Sec-WebSocket-Extensions"))
}

func TestUpgrader_Upgrade_RefusesTwoOrigins(t *testing.T) {
	called := 0
	u := mustUpgrader(t, &UpgradeOptions{CheckOrigin: func(*http.Request) bool { called++; return true }})
	rec, err := refuse(t, u, func(r *http.Request) {
		r.Header.Add("Origin", "https://a.example")
		r.Header.Add("Origin", "https://b.example")
	})
	expectRefusal(t, "H3.10, T1: two Origin lines", rec, err, 400, ErrBadHandshake, badHandshake("more than one Origin"))
	if called != 0 {
		t.Error(test.DiffMessage(called, 0, "H3.10: CheckOrigin is not called"))
	}
}

func TestUpgrader_Upgrade_NoOriginCheckRefuses(t *testing.T) {
	for name, u := range map[string]*Upgrader{"no options": mustUpgrader(t, nil), "empty options": mustUpgrader(t, &UpgradeOptions{})} {
		rec, err := refuse(t, u, func(r *http.Request) { r.Header.Set("Origin", "https://app.example") })
		expectRefusal(t, "H3.11, T1: "+name, rec, err, 403, ErrOriginNotAllowed, "websocket: origin not allowed: UpgradeOptions.CheckOrigin is not set")
	}
}

func TestUpgrader_Upgrade_OriginRejectedGets403(t *testing.T) {
	var seen string
	u := mustUpgrader(t, &UpgradeOptions{CheckOrigin: func(r *http.Request) bool {
		seen = r.Header.Get("Origin")
		return false
	}})
	rec, err := refuse(t, u, func(r *http.Request) { r.Header.Set("Origin", "https://evil.example") })
	expectRefusal(t, "H3.11, T1: CheckOrigin false", rec, err, 403, ErrOriginNotAllowed, "websocket: origin not allowed")
	if seen != "https://evil.example" {
		t.Error(test.DiffMessage(seen, "https://evil.example", "H3.11: CheckOrigin receives the request"))
	}
}

func TestUpgrader_Upgrade_CallsCheckOriginWithoutOriginHeader(t *testing.T) {
	called := 0
	var hadOrigin bool
	u := mustUpgrader(t, &UpgradeOptions{CheckOrigin: func(r *http.Request) bool {
		called++
		_, hadOrigin = r.Header["Origin"]
		return true
	}})
	c, _ := succeedWith(t, "H3.11", u, nil)
	c.Abort()
	if called != 1 || hadOrigin {
		t.Error(test.DiffMessage(called, 1, "H3.11, T1: CheckOrigin is called once, also without an Origin header"))
	}
}

func TestUpgrader_Upgrade_CheckOriginNotCalledOnBadRequest(t *testing.T) {
	called := 0
	u := mustUpgrader(t, &UpgradeOptions{CheckOrigin: func(*http.Request) bool { called++; return true }})
	for _, mutate := range []func(*http.Request){
		func(r *http.Request) { r.Method = "POST" },
		func(r *http.Request) { r.Header.Del("Sec-WebSocket-Key") },
		func(r *http.Request) { r.Header.Set("Sec-WebSocket-Version", "7") },
		func(r *http.Request) { r.Header.Add("Origin", "a"); r.Header.Add("Origin", "b") },
	} {
		_, _ = refuse(t, u, mutate)
	}
	if called != 0 {
		t.Error(test.DiffMessage(called, 0, "H3.11: CheckOrigin runs only after checks 1-10 pass"))
	}
}

func TestUpgrader_Upgrade_HijackUnsupportedGets500(t *testing.T) {
	called := 0
	u := mustUpgrader(t, &UpgradeOptions{CheckOrigin: func(*http.Request) bool { called++; return true }})
	rec, err := refuse(t, u, nil)
	expectRefusal(t, "H3.12: a ResponseWriter that cannot hijack", rec, err, 500, ErrHijack, "websocket: connection cannot be hijacked")
	if called != 1 {
		t.Error(test.DiffMessage(called, 1, "H3.12: the origin was decided before the hijack"))
	}
}

func TestUpgrader_Upgrade_FirstFailingCheckDecides(t *testing.T) {
	rec, err := refuse(t, mustUpgrader(t, nil), func(r *http.Request) {
		r.Method = "POST"
		r.Header.Del("Connection")
		r.Header.Set("Sec-WebSocket-Key", "bad")
		r.Header.Set("Sec-WebSocket-Version", "5")
	})
	expectRefusal(t, "H3: the first failing check decides (method)", rec, err, 400, ErrBadHandshake, badHandshake("method is not GET"))
	rec, err = refuse(t, mustUpgrader(t, nil), func(r *http.Request) {
		r.Header.Del("Connection")
		r.Header.Set("Sec-WebSocket-Key", "bad")
		r.Header.Set("Sec-WebSocket-Version", "5")
	})
	expectRefusal(t, "H3: the first failing check decides (Connection)", rec, err, 400, ErrBadHandshake, badHandshake("Connection is not Upgrade"))
	rec, err = refuse(t, mustUpgrader(t, nil), func(r *http.Request) {
		r.Header.Set("Sec-WebSocket-Key", "bad")
		r.Header.Set("Sec-WebSocket-Version", "5")
	})
	expectRefusal(t, "H3: the first failing check decides (version before key)", rec, err, 426, ErrUnsupportedVersion, "websocket: unsupported version")
	rec, err = refuse(t, mustUpgrader(t, nil), func(r *http.Request) {
		r.Header.Set("Sec-WebSocket-Key", "bad")
	})
	expectRefusal(t, "H3: the first failing check decides (key before origin policy)", rec, err, 400, ErrBadHandshake, badHandshake("invalid Sec-WebSocket-Key"))
}

func TestUpgrader_Upgrade_CheckOriginPanicRefuses(t *testing.T) {
	u := mustUpgrader(t, &UpgradeOptions{CheckOrigin: func(*http.Request) bool { panic(leakMarker) }})
	rec, err := refuse(t, u, nil)
	expectRefusal(t, "H3.11, T26: CheckOrigin panics", rec, err, 403, ErrOriginNotAllowed, "websocket: origin not allowed: CheckOrigin panicked")
	if containsLeak(rec.Body.Bytes()) || strings.Contains(err.Error(), leakMarker) {
		t.Error(test.DiffMessage("the panic value", "nowhere", "H3.11: the panic value appears in no error and no response"))
	}
	for k, v := range rec.Header() {
		if strings.Contains(k+strings.Join(v, ","), leakMarker) {
			t.Error(test.DiffMessage(k, "no marker", "H3.11: response header"))
		}
	}
}

func TestUpgrader_Upgrade_ZeroValueRefuses(t *testing.T) {
	rec := httptest.NewRecorder()
	c, err := (&Upgrader{}).Upgrade(rec, validRequest())
	if c != nil {
		t.Error(test.DiffMessage(c, nil, "H1a: no Conn"))
	}
	expectRefusal(t, "H1a, T26: a zero Upgrader", rec, err, 403, ErrOriginNotAllowed, "websocket: origin not allowed: UpgradeOptions.CheckOrigin is not set")
	rec = httptest.NewRecorder()
	bad := validRequest()
	bad.Method = "POST"
	_, err = (&Upgrader{}).Upgrade(rec, bad)
	expectRefusal(t, "H1a: a zero Upgrader runs checks 1-10 first", rec, err, 400, ErrBadHandshake, badHandshake("method is not GET"))
}

func TestUpgrader_Upgrade_NilUpgraderRefuses(t *testing.T) {
	var u *Upgrader
	rec := httptest.NewRecorder()
	c, err := u.Upgrade(rec, validRequest())
	if c != nil {
		t.Error(test.DiffMessage(c, nil, "H1a: no Conn"))
	}
	expectRefusal(t, "H1a, T26: a nil *Upgrader", rec, err, 403, ErrOriginNotAllowed, "websocket: origin not allowed: UpgradeOptions.CheckOrigin is not set")
}

func TestUpgrader_Upgrade_NilArguments(t *testing.T) {
	u := openUpgrader(t)
	want := "websocket: invalid argument: nil request or response"
	rec := httptest.NewRecorder()
	c, err := u.Upgrade(rec, nil)
	checkErr(t, "H1a, T26: nil request", err, ErrInvalidArgument, want)
	if c != nil || rec.Body.Len() != 0 || len(rec.Header()) != 0 || rec.Flushed {
		t.Error(test.DiffMessage(rec.Body.String(), "", "H1a: nothing written for a nil request"))
	}
	c, err = u.Upgrade(nil, validRequest())
	checkErr(t, "H1a, T26: nil writer", err, ErrInvalidArgument, want)
	if c != nil {
		t.Error(test.DiffMessage(c, nil, "H1a: no Conn"))
	}
	c, err = u.Upgrade(nil, nil)
	checkErr(t, "H1a, T26: both nil", err, ErrInvalidArgument, want)
	if c != nil {
		t.Error(test.DiffMessage(c, nil, "H1a: no Conn"))
	}
}

func TestUpgrader_Upgrade_PicksServerPreferredSubprotocol(t *testing.T) {
	u := mustUpgrader(t, &UpgradeOptions{CheckOrigin: allowAll, Subprotocols: []string{"chat.v2", "chat.v1"}})
	c, rc := succeedWith(t, "H4", u, func(r *http.Request) { r.Header.Set("Sec-WebSocket-Protocol", "chat.v1, other, chat.v2") })
	defer c.Abort()
	if c.Subprotocol() != "chat.v2" {
		t.Error(test.DiffMessage(c.Subprotocol(), "chat.v2", "H4: the first server entry the client offered"))
	}
	if !strings.Contains(string(rc.written()), "Sec-WebSocket-Protocol: chat.v2\r\n") {
		t.Error(test.DiffMessage(string(rc.written()), "Sec-WebSocket-Protocol: chat.v2", "H4: the response names the choice"))
	}
	c2, rc2 := succeedWith(t, "H4: split over two lines", u, func(r *http.Request) {
		r.Header.Add("Sec-WebSocket-Protocol", "chat.v1")
		r.Header.Add("Sec-WebSocket-Protocol", "chat.v2")
	})
	defer c2.Abort()
	if c2.Subprotocol() != "chat.v2" || !strings.Contains(string(rc2.written()), "Sec-WebSocket-Protocol: chat.v2\r\n") {
		t.Error(test.DiffMessage(c2.Subprotocol(), "chat.v2", "H4: offers on several lines"))
	}
}

func TestUpgrader_Upgrade_NoCommonSubprotocol(t *testing.T) {
	u := mustUpgrader(t, &UpgradeOptions{CheckOrigin: allowAll, Subprotocols: []string{"chat"}})
	for name, mutate := range map[string]func(*http.Request){
		"none offered":    nil,
		"another offered": func(r *http.Request) { r.Header.Set("Sec-WebSocket-Protocol", "other") },
		"case differs":    func(r *http.Request) { r.Header.Set("Sec-WebSocket-Protocol", "CHAT") },
		"prefix only":     func(r *http.Request) { r.Header.Set("Sec-WebSocket-Protocol", "chat2") },
	} {
		c, rc := succeedWith(t, "H4: "+name, u, mutate)
		if c.Subprotocol() != "" {
			t.Error(test.DiffMessage(c.Subprotocol(), "", "H4: "+name))
		}
		expectBytes(t, "H4: no Sec-WebSocket-Protocol header, "+name, rc.written(), []byte(plain101))
		c.Abort()
	}
}

func TestUpgrader_Upgrade_NeverNegotiatesExtensions(t *testing.T) {
	u := openUpgrader(t)
	c, rc := succeedWith(t, "H5", u, func(r *http.Request) {
		r.Header.Set("Sec-WebSocket-Extensions", "permessage-deflate; client_max_window_bits, x-foo")
	})
	defer c.Abort()
	if strings.Contains(strings.ToLower(string(rc.written())), "extensions") {
		t.Error(test.DiffMessage(string(rc.written()), "no Sec-WebSocket-Extensions", "H5, T10"))
	}
	c2, _ := newRecServerConn(nil, encodeTestFrame(testFrame{Fin: true, Rsv: 0x40, Opcode: testOpText, Masked: true, Payload: []byte("x")}, testMaskKey))
	_, _, err := c2.ReadMessage(nil)
	checkErr(t, "H5, T10: a later RSV1 frame fails", err, ErrProtocol, "websocket: protocol error: reserved bit set")
}

func TestUpgrader_Upgrade_SendsResponseHeaders(t *testing.T) {
	u := openUpgrader(t)
	r := validRequest()
	rc := newRecConn()
	w := newHijackWriter(rc)
	w.Header().Set("Set-Cookie", "session=1; HttpOnly")
	w.Header().Set("X-Frame-Options", "DENY")
	c, err := u.Upgrade(w, r)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Abort()
	want := strings.Replace(plain101, "\r\n\r\n", "\r\nSet-Cookie: session=1; HttpOnly\r\nX-Frame-Options: DENY\r\n\r\n", 1)
	expectBytes(t, "H6: headers set before Upgrade are sent in the 101, after H2's lines", rc.written(), []byte(want))
}

func TestUpgrader_Upgrade_DropsReservedResponseHeaders(t *testing.T) {
	u := openUpgrader(t)
	rc := newRecConn()
	w := newHijackWriter(rc)
	for _, k := range []string{"Upgrade", "Connection", "Sec-Websocket-Accept", "Sec-Websocket-Protocol", "Sec-Websocket-Extensions", "Content-Length", "Content-Type", "Content-Encoding", "Transfer-Encoding", "Trailer"} {
		w.Header().Set(k, "zz-injected")
	}
	c, err := u.Upgrade(w, validRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Abort()
	expectBytes(t, "H6: reserved headers are dropped", rc.written(), []byte(plain101))
}

func TestUpgrader_Upgrade_HeaderValueCannotSplitResponse(t *testing.T) {
	u := openUpgrader(t)
	rc := newRecConn()
	w := newHijackWriter(rc)
	w.Header()["X-Evil"] = []string{"a\r\nInjected: 1\r\n\r\nHTTP/1.1 200 OK"}
	c, err := u.Upgrade(w, validRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Abort()
	out := string(rc.written())
	if strings.Contains(out, "\r\nInjected:") || strings.Count(out, "\r\n\r\n") != 1 || !strings.HasSuffix(out, "\r\n\r\n") {
		t.Error(test.DiffMessage(out, "one head with no injected line", "H6, T12: a CR or LF in a value cannot start a new line"))
	}
}

type serverResult struct {
	conn *Conn
	err  error
}

func startUpgradeServer(t *testing.T, u *Upgrader, configure func(*httptest.Server)) (string, chan serverResult) {
	t.Helper()
	results := make(chan serverResult, 8)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := u.Upgrade(w, r)
		results <- serverResult{c, err}
	}))
	if configure != nil {
		configure(srv)
	}
	srv.Start()
	t.Cleanup(func() {
		for {
			select {
			case res := <-results:
				if res.conn != nil {
					res.conn.Abort()
				}
			default:
				srv.Close()
				return
			}
		}
	})
	return srv.Listener.Addr().String(), results
}

func dialRaw(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

const rawRequestHead = "GET /ws HTTP/1.1\r\nHost: example.test\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"

func readHeadLines(t *testing.T, br *bufio.Reader) []string {
	t.Helper()
	var lines []string
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			return lines
		}
		lines = append(lines, line)
	}
}

func TestUpgrader_Upgrade_ReadsFrameSentWithRequest(t *testing.T) {
	addr, results := startUpgradeServer(t, openUpgrader(t), nil)
	raw := dialRaw(t, addr)
	_, _ = raw.Write(append([]byte(rawRequestHead), clientFrame(testOpText, true, []byte("hello"))...))
	br := bufio.NewReader(raw)
	if lines := readHeadLines(t, br); len(lines) == 0 || !strings.HasPrefix(lines[0], "HTTP/1.1 101") {
		t.Fatal(test.DiffMessage(lines, "a 101", "H7: the response"))
	}
	res := <-results
	if res.err != nil {
		t.Fatal(res.err)
	}
	kind, data, err := res.conn.ReadMessage(nil)
	if err != nil || kind != KindText || string(data) != "hello" {
		t.Error(test.DiffMessage(string(data), "hello", "H7: a frame in the same packet as the request is read once"))
	}
}

func TestUpgrader_Upgrade_ReadsPartialFrameSentWithRequest(t *testing.T) {
	addr, results := startUpgradeServer(t, openUpgrader(t), nil)
	raw := dialRaw(t, addr)
	frame := clientFrame(testOpText, true, []byte("split frame"))
	_, _ = raw.Write(append([]byte(rawRequestHead), frame[:3]...))
	br := bufio.NewReader(raw)
	readHeadLines(t, br)
	res := <-results
	if res.err != nil {
		t.Fatal(res.err)
	}
	_, _ = raw.Write(frame[3:])
	_, data, err := res.conn.ReadMessage(nil)
	if err != nil || string(data) != "split frame" {
		t.Error(test.DiffMessage(string(data), "split frame", "H7: the first half arrived with the request, the second later"))
	}
}

func TestUpgrader_Upgrade_ReadsTwoFramesSentWithRequest(t *testing.T) {
	addr, results := startUpgradeServer(t, openUpgrader(t), nil)
	raw := dialRaw(t, addr)
	_, _ = raw.Write(bytes.Join([][]byte{[]byte(rawRequestHead), clientFrame(testOpText, true, []byte("one")), clientFrame(testOpBinary, true, []byte("two"))}, nil))
	readHeadLines(t, bufio.NewReader(raw))
	res := <-results
	if res.err != nil {
		t.Fatal(res.err)
	}
	for _, want := range []string{"one", "two"} {
		_, data, err := res.conn.ReadMessage(nil)
		if err != nil || string(data) != want {
			t.Error(test.DiffMessage(string(data), want, "H7: buffered frames are read once, in order"))
		}
	}
}

func TestUpgrader_Upgrade_PipelinedHTTPRequestFailsAsFrame(t *testing.T) {
	addr, results := startUpgradeServer(t, openUpgrader(t), nil)
	raw := dialRaw(t, addr)
	_, _ = raw.Write([]byte(rawRequestHead + "GET /admin HTTP/1.1\r\nHost: example.test\r\n\r\n"))
	readHeadLines(t, bufio.NewReader(raw))
	res := <-results
	if res.err != nil {
		t.Fatal(res.err)
	}
	_, _, err := res.conn.ReadMessage(nil)
	checkErr(t, "T12: bytes pipelined after the upgrade are frames, and malformed ones", err, ErrProtocol, "")
}

func TestUpgrader_Upgrade_SlowRequestCutByServerDeadline(t *testing.T) {
	addr, results := startUpgradeServer(t, openUpgrader(t), func(s *httptest.Server) { s.Config.ReadHeaderTimeout = 100 * time.Millisecond })
	raw := dialRaw(t, addr)
	_, _ = raw.Write([]byte("GET /ws HTTP/1.1\r\nHost: example.test\r\nUpgrade: websocket\r\n"))
	var err error
	took := elapsed(func() { _, err = io.ReadAll(raw) })
	if err != nil {
		t.Error(test.DiffMessage(err, nil, "T5: the server ends the connection"))
	}
	if took < 80*time.Millisecond || took > 10*time.Second {
		t.Error(test.DiffMessage(took, "about 100ms", "T5, H8: a slow request is cut by the server's ReadHeaderTimeout"))
	}
	select {
	case res := <-results:
		t.Error(test.DiffMessage(res, "no Upgrade call", "T5: the handler never ran"))
	default:
	}
}

func TestUpgrader_Upgrade_BlockedResponseWriteCutAtHandshakeTimeout(t *testing.T) {
	a, b := net.Pipe()
	t.Cleanup(func() {
		_ = a.Close()
		_ = b.Close()
	})
	u := mustUpgrader(t, &UpgradeOptions{CheckOrigin: allowAll, HandshakeTimeout: 50 * time.Millisecond})
	w := newHijackWriter(a)
	var c *Conn
	var err error
	took := elapsed(func() { c, err = u.Upgrade(w, validRequest()) })
	checkErr(t, "H8, T5: the peer never reads the 101", err, ErrTimeout, "websocket: timeout")
	if c != nil {
		t.Error(test.DiffMessage(c, nil, "H8: no Conn"))
	}
	if took < 40*time.Millisecond || took > 5*time.Second {
		t.Error(test.DiffMessage(took, "about 50ms", "H8: cut at HandshakeTimeout"))
	}
	if _, err := b.Read(make([]byte, 1)); err == nil {
		t.Error(test.DiffMessage(err, io.EOF, "H8: the connection is aborted"))
	}
}

func TestUpgrader_Upgrade_WriteErrorAbortsHandshake(t *testing.T) {
	rc := newRecConn()
	rc.writeErr = errBoom{}
	c, err := openUpgrader(t).Upgrade(newHijackWriter(rc), validRequest())
	checkErr(t, "H8: the 101 cannot be written", err, ErrAbnormalClosure, "websocket: connection lost")
	if c != nil || rc.closes() != 1 {
		t.Error(test.DiffMessage(rc.closes(), 1, "H8: abort"))
	}
}

func TestUpgrader_Upgrade_LeavesNoDeadline(t *testing.T) {
	c, rc := succeedWith(t, "H8", mustUpgrader(t, &UpgradeOptions{CheckOrigin: allowAll, HandshakeTimeout: time.Second}), nil)
	defer c.Abort()
	rd, wd := rc.deadlines()
	if !rd.IsZero() || !wd.IsZero() {
		t.Error(test.DiffMessage([]time.Time{rd, wd}, "zero deadlines", "H8: Upgrade leaves no deadline set"))
	}
}

func TestUpgrader_Upgrade_RefusalWritesNothingElse(t *testing.T) {
	rec, err := refuse(t, openUpgrader(t), func(r *http.Request) {
		r.Header.Set("Sec-WebSocket-Key", "zz-LEAK")
		r.Header.Set("Origin", "zz-LEAK")
	})
	expectRefusal(t, "H9", rec, err, 400, ErrBadHandshake, badHandshake("invalid Sec-WebSocket-Key"))
	allowed := map[string]bool{"Content-Type": true, "X-Content-Type-Options": true, "Connection": true}
	for k := range rec.Header() {
		if !allowed[k] {
			t.Error(test.DiffMessage(k, "only the fixed headers", "H9: header of a refusal"))
		}
	}
}

func TestUpgrader_Upgrade_RefusalClosesConnection(t *testing.T) {
	addr, results := startUpgradeServer(t, openUpgrader(t), nil)
	raw := dialRaw(t, addr)
	_, _ = raw.Write([]byte(strings.Replace(rawRequestHead, "GET", "POST", 1)))
	all, err := io.ReadAll(raw)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(all)), nil)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 400 || !resp.Close || string(body) != refusalBody(400) {
		t.Error(test.DiffMessage(resp.StatusCode, 400, "T12, refuse: 400, Connection: close, the fixed body"))
	}
	res := <-results
	checkErr(t, "T12", res.err, ErrBadHandshake, badHandshake("method is not GET"))
}

func TestUpgrader_Upgrade_RefusalLeaksNothing(t *testing.T) {
	target := "/" + leakMarker + "?" + leakMarker + "=" + leakMarker
	scenarios := []struct {
		name   string
		opts   *UpgradeOptions
		mutate func(*http.Request)
	}{
		{"key", nil, func(r *http.Request) { r.Header.Set("Sec-WebSocket-Key", leakMarker) }},
		{"version", nil, func(r *http.Request) { r.Header.Set("Sec-WebSocket-Version", leakMarker) }},
		{"protocols", nil, func(r *http.Request) {
			r.Header.Set("Sec-WebSocket-Protocol", strings.TrimSuffix(strings.Repeat(leakMarker+",", 17), ","))
		}},
		{"extensions", nil, func(r *http.Request) {
			r.Header.Set("Sec-WebSocket-Extensions", strings.TrimSuffix(strings.Repeat(leakMarker+",", 17), ","))
		}},
		{"two origins", nil, func(r *http.Request) {
			r.Header.Add("Origin", leakMarker)
			r.Header.Add("Origin", leakMarker)
		}},
		{"origin refused", &UpgradeOptions{CheckOrigin: func(*http.Request) bool { return false }}, func(r *http.Request) { r.Header.Set("Origin", leakMarker) }},
		{"no origin policy", &UpgradeOptions{}, func(r *http.Request) { r.Header.Set("Origin", leakMarker) }},
		{"upgrade", nil, func(r *http.Request) { r.Header.Set("Upgrade", leakMarker) }},
		{"connection", nil, func(r *http.Request) { r.Header.Set("Connection", leakMarker) }},
		{"method", nil, func(r *http.Request) { r.Method = "ZZ-LEAK" }},
		{"custom header", nil, func(r *http.Request) {
			r.Header.Set("X-Custom", leakMarker)
			r.Header.Set("Sec-WebSocket-Key", leakMarker)
		}},
		{"panic", &UpgradeOptions{CheckOrigin: func(*http.Request) bool { panic(leakMarker) }}, nil},
	}
	for _, s := range scenarios {
		opts := s.opts
		if opts == nil {
			opts = &UpgradeOptions{CheckOrigin: allowAll}
		}
		u := mustUpgrader(t, opts)
		r := validRequestAt(target)
		if s.mutate != nil {
			s.mutate(r)
		}
		rec := httptest.NewRecorder()
		c, err := u.Upgrade(rec, r)
		if c != nil {
			c.Abort()
			t.Fatal(test.DiffMessage(c, nil, "T15: "+s.name+" must be refused"))
		}
		if err == nil || strings.Contains(err.Error(), leakMarker) {
			t.Error(test.DiffMessage(err, "an error without the marker", "H9, T15: "+s.name+": error"))
		}
		if containsLeak(rec.Body.Bytes()) {
			t.Error(test.DiffMessage(rec.Body.String(), "no marker", "H9, T15: "+s.name+": body"))
		}
		for k, v := range rec.Header() {
			if strings.Contains(k+strings.Join(v, ","), leakMarker) {
				t.Error(test.DiffMessage(k, "no marker", "H9, T15: "+s.name+": header"))
			}
		}
	}
}

func refTokenElements(values []string) (elements []string, ambiguous bool) {
	for _, v := range values {
		for e := range strings.SplitSeq(v, ",") {
			e = strings.Trim(e, " \t")
			if e == "" {
				return nil, true
			}
			elements = append(elements, e)
		}
	}
	return elements, false
}

func refIsToken(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		ok := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.IndexByte("!#$%&'*+-.^_`|~", c) >= 0
		if !ok {
			return false
		}
	}
	return true
}

func refTotal(values []string) int {
	n := 0
	for _, v := range values {
		n += len(v)
	}
	return n
}

func refHandshakeStatus(r *http.Request) (status int, decided bool) {
	if r.ProtoMajor != 1 || r.ProtoMinor != 1 || r.Method != http.MethodGet {
		return 400, true
	}
	if r.ContentLength != 0 || len(r.TransferEncoding) > 0 {
		return 400, true
	}
	conn := r.Header.Values("Connection")
	for _, v := range conn {
		if strings.Contains(v, `"`) {
			return 400, true
		}
	}
	elements, ambiguous := refTokenElements(conn)
	if ambiguous {
		return 0, false
	}
	found := false
	for _, e := range elements {
		if strings.EqualFold(e, "upgrade") {
			found = true
		}
	}
	if !found {
		return 400, true
	}
	up := r.Header.Values("Upgrade")
	if len(up) != 1 || !strings.EqualFold(strings.Trim(up[0], " \t"), "websocket") {
		return 400, true
	}
	if v := r.Header.Values("Sec-WebSocket-Version"); len(v) != 1 || v[0] != "13" {
		return 426, true
	}
	key := r.Header.Values("Sec-WebSocket-Key")
	if len(key) != 1 || len(key[0]) != 24 {
		return 400, true
	}
	if raw, err := base64.StdEncoding.Strict().DecodeString(key[0]); err != nil || len(raw) != 16 {
		return 400, true
	}
	protocols := r.Header.Values("Sec-WebSocket-Protocol")
	if len(protocols) > 0 {
		if refTotal(protocols) > 1024 {
			return 400, true
		}
		for _, v := range protocols {
			if strings.Contains(v, `"`) {
				return 400, true
			}
		}
		els, amb := refTokenElements(protocols)
		if amb {
			return 0, false
		}
		if len(els) > 16 {
			return 400, true
		}
		for _, e := range els {
			if !refIsToken(e) {
				return 400, true
			}
		}
	}
	extensions := r.Header.Values("Sec-WebSocket-Extensions")
	if len(extensions) > 0 {
		if refTotal(extensions) > 1024 {
			return 400, true
		}
		els, amb := refTokenElements(extensions)
		if amb {
			return 0, false
		}
		if len(els) > 16 {
			return 400, true
		}
	}
	if len(r.Header.Values("Origin")) > 1 {
		return 400, true
	}
	return 101, true
}

func FuzzHandshake(f *testing.F) {
	base := "GET /chat HTTP/1.1\r\nHost: example.test\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n"
	seeds := []string{
		base + "\r\n",
		base + "Sec-WebSocket-Protocol: chat, superchat\r\nSec-WebSocket-Extensions: permessage-deflate\r\nOrigin: https://a.example\r\n\r\n",
		strings.Replace(base, "HTTP/1.1", "HTTP/1.0", 1) + "\r\n",
		strings.Replace(base, "GET", "POST", 1) + "\r\n",
		base + "Content-Length: 5\r\n\r\nhello",
		strings.Replace(base, "Connection: Upgrade", "Connection: close", 1) + "\r\n",
		strings.Replace(base, "Upgrade: websocket", "Upgrade: websocket, h2c", 1) + "\r\n",
		strings.Replace(base, "Version: 13", "Version: 12", 1) + "\r\n",
		strings.Replace(base, "dGhlIHNhbXBsZSBub25jZQ==", "short", 1) + "\r\n",
		base + "Sec-WebSocket-Protocol: \"chat\"\r\n\r\n",
		base + "Origin: a\r\nOrigin: b\r\n\r\n",
		base + "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n",
		base + "Connection: keep-alive\r\n\r\n",
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		r, err := http.ReadRequest(bufio.NewReader(bytes.NewReader(raw)))
		if err != nil {
			return
		}
		u := mustUpgrader(t, &UpgradeOptions{CheckOrigin: allowAll, Subprotocols: []string{"chat"}})
		rc := newRecConn()
		w := newHijackWriter(rc)
		c, err := u.Upgrade(w, r)
		if c != nil {
			c.Abort()
		}
		want, decided := refHandshakeStatus(r)
		if !decided {
			return
		}
		if want == 101 {
			if err != nil || c == nil || !bytes.HasPrefix(rc.written(), []byte("HTTP/1.1 101 Switching Protocols\r\n")) {
				t.Fatal(test.DiffMessage(err, nil, "H3: the reference accepts this request"))
			}
			return
		}
		if err == nil || c != nil || w.status != want || w.hijacked {
			t.Fatal(test.DiffMessage(w.status, want, "H3: the reference refuses this request with this status"))
		}
	})
}

func TestUpgrader_Upgrade_DropsReservedHeadersOfAnyCase(t *testing.T) {
	u := openUpgrader(t)
	rc := newRecConn()
	w := newHijackWriter(rc)
	for _, k := range []string{"upgrade", "CONNECTION", "sec-websocket-accept", "Sec-WebSocket-Protocol", "SEC-WEBSOCKET-EXTENSIONS", "content-length", "content-type", "content-encoding", "transfer-encoding", "trailer"} {
		w.Header()[k] = []string{"zz-injected"}
	}
	c, err := u.Upgrade(w, validRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Abort()
	expectBytes(t, "H6: a reserved header set under a non-canonical name is dropped", rc.written(), []byte(plain101))
}

func TestUpgrader_Upgrade_SmallHijackedReaderReadsControlFrames(t *testing.T) {
	u := openUpgrader(t)
	rc := newRecConn(clientFrame(testOpPing, true, repeatByte('p', 125)), clientFrame(testOpText, true, []byte("x")))
	w := newHijackWriter(rc)
	w.br = bufio.NewReaderSize(rc, 16)
	c, err := u.Upgrade(w, validRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Abort()
	_, data, err := c.ReadMessage(nil)
	if err != nil || string(data) != "x" {
		t.Error(test.DiffMessage(err, nil, "H7, L: a ping of 125 bytes read through a 16-byte hijacked reader"))
	}
	frames, err := parseTestFrames(bytes.TrimPrefix(rc.written(), []byte(plain101)), false)
	if err != nil || len(frames) != 1 || frames[0].Opcode != testOpPong || len(frames[0].Payload) != 125 {
		t.Error(test.DiffMessage(frames, "one pong of 125 bytes", "H7, L: the ping is answered"))
	}
}
