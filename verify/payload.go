package verify

import (
	"encoding/binary"
	"errors"
	"fmt"

	"bridges/bloom"
)

// Wire-format primitives. The verifier reimplements payload PARSING from
// the spec, but imports the bloom library for hashing: the hash function
// is part of the wire contract between the emitting SDK and any reader
// (two correct parties must agree on it byte-for-byte), so sharing it is
// sharing the spec, not the engine. No reconstruction logic is imported.

// decodePCRB parses type(1) || uvarint(depth) || prefix(K) || bloomBits.
func decodePCRB(p []byte, cfg Config) (prefix []byte, bloomBits []byte, err error) {
	if len(p) < 2 || p[0] != cfg.PCRBTypeByte {
		return nil, nil, errors.New("not a PCRB payload")
	}
	_, n := binary.Uvarint(p[1:])
	if n <= 0 {
		return nil, nil, errors.New("bad depth varint")
	}
	rest := p[1+n:]
	if len(rest) < cfg.PrefixLen {
		return nil, nil, errors.New("short prefix")
	}
	prefix = rest[:cfg.PrefixLen]
	bits := rest[cfg.PrefixLen:]
	if want := int((cfg.BloomM + 7) / 8); len(bits) != 0 && len(bits) != want {
		return nil, nil, fmt.Errorf("bloom length %d != %d", len(bits), want)
	}
	if len(bits) == 0 {
		return prefix, nil, nil
	}
	return prefix, bits, nil
}

func bloomTest(bits []byte, cfg Config, key [16]byte) bool {
	f := bloom.Deserialize(bits, cfg.BloomM, cfg.BloomK)
	return f != nil && f.Test(key[:])
}

// hexOf renders a span ID the way the SDK keys bloom entries (16
// lowercase hex chars) — wire convention, restated from the spec.
func hexOf(id uint64) [16]byte {
	var out [16]byte
	const digits = "0123456789abcdef"
	for i := 15; i >= 0; i-- {
		out[i] = digits[id&0xf]
		id >>= 4
	}
	return out
}
