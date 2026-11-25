package websocket

import (
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	"go.owlkei.com/sdk/internal/test"
)

var (
	viablePrefixesOnce sync.Once
	viablePrefixes     map[string]bool
)

func loadViablePrefixes() map[string]bool {
	viablePrefixesOnce.Do(func() {
		set := map[string]bool{}
		for r := rune(0); r <= utf8.MaxRune; r += 64 {
			if r >= 0xD800 && r <= 0xDFFF {
				continue
			}
			enc := utf8.AppendRune(nil, r)
			for n := 1; n < len(enc); n++ {
				set[string(enc[:n])] = true
			}
		}
		viablePrefixes = set
	})
	return viablePrefixes
}

func viableUTF8Prefix(p []byte) bool {
	set := loadViablePrefixes()
	for k := 0; k <= 3 && k <= len(p); k++ {
		head := p[:len(p)-k]
		tail := p[len(p)-k:]
		if utf8.Valid(head) && (k == 0 || set[string(tail)]) {
			return true
		}
	}
	return false
}

func firstNonViable(b []byte) int {
	for i := range b {
		if !viableUTF8Prefix(b[:i+1]) {
			return i
		}
	}
	return -1
}

func checkChunks(t testing.TB, rule string, b []byte, cuts []int) {
	t.Helper()
	first := firstNonViable(b)
	var s utf8State
	start := 0
	bounds := append(append([]int{}, cuts...), len(b))
	failedChunk := -1
	for ci, end := range bounds {
		if end < start {
			continue
		}
		ok := s.validate(b[start:end])
		if !ok {
			failedChunk = ci
			break
		}
		start = end
	}
	if first < 0 {
		if failedChunk >= 0 {
			t.Fatal(test.DiffMessage("a failing chunk", "every chunk passes", rule+": "+strings.ToValidUTF8(string(b), "?")))
		}
		if got, want := s.isComplete(), utf8.Valid(b); got != want {
			t.Fatal(test.DiffMessage(got, want, rule+": isComplete equals utf8.Valid on a viable input"))
		}
		return
	}
	if failedChunk < 0 {
		t.Fatal(test.DiffMessage("no failing chunk", "the chunk holding byte "+itoa(first), rule))
	}
	lo := 0
	if failedChunk > 0 {
		lo = bounds[failedChunk-1]
	}
	hi := bounds[failedChunk]
	if first < lo || first >= hi {
		t.Fatal(test.DiffMessage(failedChunk, "the chunk holding byte "+itoa(first), rule+": first failing chunk"))
	}
}

func feedBytes(b []byte) (failedAt int) {
	var s utf8State
	for i := range b {
		if !s.validate(b[i : i+1]) {
			return i
		}
	}
	return -1
}

func TestUTF8State_Overlong(t *testing.T) {
	cases := []struct {
		in   string
		fail int
	}{
		{"\xc0\x80", 0},
		{"\xc1\xbf", 0},
		{"\xe0\x80\x80", 1},
		{"\xe0\x9f\xbf", 1},
		{"\xf0\x80\x80\x80", 1},
		{"\xf0\x8f\xbf\xbf", 1},
	}
	for _, c := range cases {
		if got := feedBytes([]byte(c.in)); got != c.fail {
			t.Error(test.DiffMessage(got, c.fail, "U1, U2: overlong input of "+itoa(len(c.in))+" bytes, index of the failing byte"))
		}
	}
}

func TestUTF8State_Surrogates(t *testing.T) {
	for _, in := range []string{"\xed\xa0\x80", "\xed\xbf\xbf", "\xed\xa0"} {
		if got := feedBytes([]byte(in)); got != 1 {
			t.Error(test.DiffMessage(got, 1, "U1, U2: surrogate fails at its second byte"))
		}
	}
	var s utf8State
	if !s.validate([]byte("\xed\x9f\xbf")) || !s.isComplete() {
		t.Error(test.DiffMessage(false, true, "U1: U+D7FF is valid"))
	}
}

func TestUTF8State_AboveMax(t *testing.T) {
	if got := feedBytes([]byte("\xf4\x90\x80\x80")); got != 1 {
		t.Error(test.DiffMessage(got, 1, "U1: F4 90 fails at its second byte"))
	}
	for _, b := range []byte{0xf5, 0xf8, 0xfb, 0xfc, 0xfe, 0xff} {
		if got := feedBytes([]byte{b, 0x80, 0x80, 0x80}); got != 0 {
			t.Error(test.DiffMessage(got, 0, "U1: lead byte "+itoa(int(b))+" fails at once"))
		}
	}
	var s utf8State
	if !s.validate([]byte("\xf4\x8f\xbf\xbf")) || !s.isComplete() {
		t.Error(test.DiffMessage(false, true, "U1: U+10FFFF is valid"))
	}
}

func TestUTF8State_StrayContinuation(t *testing.T) {
	for _, in := range []string{"\x80", "\xbf", "a\x80", "\xc3\xa9\x80"} {
		if feedBytes([]byte(in)) < 0 {
			t.Error(test.DiffMessage("valid", "invalid", "U1: stray continuation byte in "+itoa(len(in))+" bytes"))
		}
	}
}

func TestUTF8State_TruncatedAtEnd(t *testing.T) {
	for _, full := range []string{"\xc3\xa9", "\xe1\xbb\x87", "\xf0\x9f\x98\x80"} {
		for n := 1; n < len(full); n++ {
			var s utf8State
			if !s.validate([]byte(full[:n])) {
				t.Error(test.DiffMessage(false, true, "U1: a valid prefix passes validate"))
			}
			if s.isComplete() {
				t.Error(test.DiffMessage(true, false, "U1: a message ending inside a code point is not complete"))
			}
		}
		var s utf8State
		if !s.validate([]byte(full)) || !s.isComplete() {
			t.Error(test.DiffMessage(false, true, "U1: the whole code point is complete"))
		}
	}
}

func TestUTF8State_ValidBoundaries(t *testing.T) {
	for _, in := range []string{"", "a", "\x7f", "\xc2\x80", "\xdf\xbf", "\xe0\xa0\x80", "\xef\xbf\xbf", "\xf0\x90\x80\x80", "Tiếng Việt 日本語 😀"} {
		var s utf8State
		if !s.validate([]byte(in)) || !s.isComplete() {
			t.Error(test.DiffMessage(false, true, "U1: valid input "+itoa(len(in))+" bytes"))
		}
	}
}

func TestUTF8State_OpenCodePointRejectsASCII(t *testing.T) {
	var s utf8State
	if !s.validate([]byte("\xe1\xbb")) {
		t.Fatal("valid prefix")
	}
	if s.validate([]byte("abcdefgh")) {
		t.Error(test.DiffMessage(true, false, "P2: ASCII after an open code point fails"))
	}
}

func TestUTF8State_SplitAcrossChunks(t *testing.T) {
	in := []byte("aé日😀")
	for i := 0; i <= len(in); i++ {
		for j := i; j <= len(in); j++ {
			var s utf8State
			ok := s.validate(in[:i]) && s.validate(in[i:j]) && s.validate(in[j:])
			if !ok || !s.isComplete() {
				t.Fatal(test.DiffMessage(false, true, "U3: split at "+itoa(i)+" and "+itoa(j)))
			}
		}
	}
}

func utf8Corpus() [][]byte {
	valid := []string{
		"", "hello world, this is plain ASCII of more than eight bytes", "Tiếng Việt có dấu", "日本語のテキスト",
		"😀😃😄", "\x00\x7f", "\xc2\x80\xdf\xbf", "\xe0\xa0\x80\xef\xbf\xbf", "\xf0\x90\x80\x80\xf4\x8f\xbf\xbf", "\xed\x9f\xbf",
	}
	invalid := []string{
		"\xc0\x80", "\xc1\xbf", "\xe0\x80\x80", "\xe0\x9f\xbf", "\xf0\x80\x80\x80", "\xed\xa0\x80", "\xed\xbf\xbf",
		"\xf4\x90\x80\x80", "\xf5\x80\x80\x80", "\xff", "\x80", "abcdefgh\x80", "abcdefghijklmnop\xff", "é\xc3", "日\xe6\x97",
		"\xf0\x9f\x98", "ok\xed\xa0\x80ok", "\xe1\xbb\xe1",
	}
	var out [][]byte
	for _, s := range valid {
		out = append(out, []byte(s))
	}
	for _, s := range invalid {
		out = append(out, []byte(s))
	}
	return out
}

func TestUTF8State_EqualsStdlib(t *testing.T) {
	for _, b := range utf8Corpus() {
		for i := 0; i <= len(b); i++ {
			checkChunks(t, "U4: corpus split in two", b, []int{i})
		}
		var cuts []int
		for i := 1; i < len(b); i++ {
			cuts = append(cuts, i)
		}
		checkChunks(t, "U4: corpus byte by byte", b, cuts)
		checkChunks(t, "U4: corpus whole", b, nil)
	}
	r := newTestRand()
	interesting := []byte{0x00, 0x41, 0x7f, 0x80, 0x8f, 0x90, 0x9f, 0xa0, 0xbf, 0xc0, 0xc1, 0xc2, 0xdf, 0xe0, 0xe1, 0xec, 0xed, 0xee, 0xef, 0xf0, 0xf1, 0xf3, 0xf4, 0xf5, 0xff}
	seeds := utf8Corpus()
	for range 20000 {
		var b []byte
		switch r.IntN(3) {
		case 0:
			n := r.IntN(24)
			for range n {
				b = append(b, interesting[r.IntN(len(interesting))])
			}
		case 1:
			b = append(b, seeds[r.IntN(len(seeds))]...)
			if len(b) > 0 {
				b[r.IntN(len(b))] = interesting[r.IntN(len(interesting))]
			}
		default:
			b = randomBytes(r, r.IntN(40))
			for i := range b {
				if r.IntN(4) != 0 {
					b[i] &= 0x7f
				}
			}
		}
		var cuts []int
		for i := 1; i < len(b); i++ {
			if r.IntN(3) == 0 {
				cuts = append(cuts, i)
			}
		}
		checkChunks(t, "U4: random input", b, cuts)
	}
}

func FuzzUTF8(f *testing.F) {
	for _, s := range utf8Corpus() {
		f.Add(s, uint8(len(s)/2), uint8(len(s)))
	}
	f.Add([]byte("\xed\xa0\x80"), uint8(1), uint8(2))
	f.Fuzz(func(t *testing.T, data []byte, cutA, cutB uint8) {
		a, b := int(cutA), int(cutB)
		if a > len(data) {
			a = len(data)
		}
		if b > len(data) {
			b = len(data)
		}
		if a > b {
			a, b = b, a
		}
		checkChunks(t, "U4: fuzz", data, []int{a, b})
	})
}
