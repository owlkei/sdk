package websocket

import "encoding/binary"

func maskBytes(b []byte, key [4]byte, pos int) int {
	pos &= 3
	next := (pos + len(b)) & 3
	if len(b) >= 16 {
		k32 := uint64(key[pos]) | uint64(key[(pos+1)&3])<<8 | uint64(key[(pos+2)&3])<<16 | uint64(key[(pos+3)&3])<<24
		k64 := k32 | k32<<32
		for len(b) >= 32 {
			binary.LittleEndian.PutUint64(b, binary.LittleEndian.Uint64(b)^k64)
			binary.LittleEndian.PutUint64(b[8:], binary.LittleEndian.Uint64(b[8:])^k64)
			binary.LittleEndian.PutUint64(b[16:], binary.LittleEndian.Uint64(b[16:])^k64)
			binary.LittleEndian.PutUint64(b[24:], binary.LittleEndian.Uint64(b[24:])^k64)
			b = b[32:]
		}
		for len(b) >= 8 {
			binary.LittleEndian.PutUint64(b, binary.LittleEndian.Uint64(b)^k64)
			b = b[8:]
		}
	}
	for i := range b {
		b[i] ^= key[(pos+i)&3]
	}
	return next
}
