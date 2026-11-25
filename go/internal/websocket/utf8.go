package websocket

import "encoding/binary"

const asciiMask = 0x8080808080808080

type utf8State struct {
	need uint8
	lo   uint8
	hi   uint8
}

func (s *utf8State) validate(b []byte) bool {
	need, lo, hi := s.need, s.lo, s.hi
	i := 0
	for i < len(b) {
		if need == 0 {
			for i+8 <= len(b) && binary.LittleEndian.Uint64(b[i:])&asciiMask == 0 {
				i += 8
			}
			if i == len(b) {
				break
			}
			c := b[i]
			i++
			switch {
			case c < 0x80:
				continue
			case c < 0xc2:
				return false
			case c < 0xe0:
				need, lo, hi = 1, 0x80, 0xbf
			case c == 0xe0:
				need, lo, hi = 2, 0xa0, 0xbf
			case c == 0xed:
				need, lo, hi = 2, 0x80, 0x9f
			case c < 0xf0:
				need, lo, hi = 2, 0x80, 0xbf
			case c == 0xf0:
				need, lo, hi = 3, 0x90, 0xbf
			case c < 0xf4:
				need, lo, hi = 3, 0x80, 0xbf
			case c == 0xf4:
				need, lo, hi = 3, 0x80, 0x8f
			default:
				return false
			}
			continue
		}
		c := b[i]
		i++
		if c < lo || c > hi {
			return false
		}
		need--
		lo, hi = 0x80, 0xbf
	}
	s.need, s.lo, s.hi = need, lo, hi
	return true
}

func (s *utf8State) isComplete() bool { return s.need == 0 }
