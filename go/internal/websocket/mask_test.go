package websocket

import (
	"bytes"
	"testing"

	"go.owlkei.com/sdk/internal/test"
)

func maskBytewise(b []byte, key [4]byte, pos int) int {
	for i := range b {
		b[i] ^= key[(pos+i)&3]
	}
	return (pos + len(b)) & 3
}

func TestMaskBytes_EqualsBytewise(t *testing.T) {
	r := newTestRand()
	backing := randomBytes(r, 1024)
	key := [4]byte{0x01, 0xa5, 0x5a, 0xff}
	for length := 0; length <= 257; length++ {
		for align := range 8 {
			for pos := range 4 {
				got := append([]byte(nil), backing[align:align+length]...)
				want := append([]byte(nil), got...)
				gotNext := maskBytes(got, key, pos)
				wantNext := maskBytewise(want, key, pos)
				if !bytes.Equal(got, want) || gotNext != wantNext {
					t.Fatal(test.DiffMessage(got, want, "P1: maskBytes differs from the byte loop (length, align, pos)"+
						"\nlength="+itoa(length)+" align="+itoa(align)+" pos="+itoa(pos)))
				}
			}
		}
	}
	for i := range 10000 {
		length := r.IntN(2048)
		align := r.IntN(8)
		pos := r.IntN(4)
		var k [4]byte
		copy(k[:], randomBytes(r, 4))
		data := randomBytes(r, length+align)
		got := append([]byte(nil), data[align:]...)
		want := append([]byte(nil), got...)
		gotNext := maskBytes(got, k, pos)
		wantNext := maskBytewise(want, k, pos)
		if !bytes.Equal(got, want) || gotNext != wantNext {
			t.Fatal(test.DiffMessage(got, want, "P1: maskBytes differs from the byte loop, random case "+itoa(i)))
		}
	}
}

func TestMaskBytes_ReturnsNextOffset(t *testing.T) {
	key := [4]byte{1, 2, 3, 4}
	for pos := range 4 {
		for length := range 40 {
			b := make([]byte, length)
			got := maskBytes(b, key, pos)
			if want := (pos + length) & 3; got != want {
				t.Error(test.DiffMessage(got, want, "P1: next key offset for length "+itoa(length)+" pos "+itoa(pos)))
			}
		}
	}
}

func TestMaskBytes_Involution(t *testing.T) {
	r := newTestRand()
	key := [4]byte{9, 8, 7, 6}
	for range 200 {
		orig := randomBytes(r, r.IntN(300))
		b := append([]byte(nil), orig...)
		pos := r.IntN(4)
		maskBytes(b, key, pos)
		maskBytes(b, key, pos)
		if !bytes.Equal(b, orig) {
			t.Error(test.DiffMessage(b, orig, "P1: masking twice restores the input"))
		}
	}
}

func TestMaskBytes_TouchesOnlyItsSlice(t *testing.T) {
	key := [4]byte{0xff, 0xff, 0xff, 0xff}
	for length := 0; length <= 40; length++ {
		buf := make([]byte, length+16)
		maskBytes(buf[8:8+length], key, 0)
		for i := range buf {
			inside := i >= 8 && i < 8+length
			if inside && buf[i] != 0xff || !inside && buf[i] != 0 {
				t.Fatal(test.DiffMessage(buf[i], "unchanged outside the slice", "P1: byte "+itoa(i)+" for length "+itoa(length)))
			}
		}
	}
}

func FuzzMaskBytes(f *testing.F) {
	for n := 0; n <= 17; n++ {
		f.Add(bytes.Repeat([]byte{0x5a}, n), uint32(0x12345678), uint8(n&3), uint8(n&7))
	}
	f.Fuzz(func(t *testing.T, data []byte, key uint32, pos uint8, align uint8) {
		a := int(align & 7)
		padded := append(make([]byte, a), data...)
		got := append([]byte(nil), padded[a:]...)
		want := append([]byte(nil), got...)
		k := [4]byte{byte(key), byte(key >> 8), byte(key >> 16), byte(key >> 24)}
		p := int(pos & 3)
		gotNext := maskBytes(got, k, p)
		wantNext := maskBytewise(want, k, p)
		if !bytes.Equal(got, want) || gotNext != wantNext {
			t.Fatal(test.DiffMessage(got, want, "P1: maskBytes differs from the byte loop"))
		}
	})
}
