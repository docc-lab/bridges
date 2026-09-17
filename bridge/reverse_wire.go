package bridge

import (
	"encoding/binary"
	"fmt"
	"math"
)

// ReverseEncoding names the simulator's binary returned-truss format. As with
// the forward bridge formats, bytes are measured before external RPC/OTLP
// transport encoding. Empty baggage is represented by no bytes.
const ReverseEncoding = "bridges.reverse.binary.v1"
const ReverseEnvelopeVersion byte = 1

// EncodeReverseEnvelope packs one bundle:
//
//	version(1) || repeated [uvarint(payloadLen<<1 | hasTTL) || originID(8)
//	                       || uvarint(originDepth) || optionalTTL(1) || payload]
//
// Records are length-framed and consume the remaining buffer; no count or kind
// string is needed. The intact native payload already identifies PB0/CGP0/SB3.
// Probability records carry no countdown byte. Forward checkpoint geometry and
// the exact originating payload are retained unchanged.
func EncodeReverseEnvelope(segments []ReverseSegment) ([]byte, error) {
	if len(segments) == 0 {
		return nil, nil
	}
	out := []byte{ReverseEnvelopeVersion}
	for i, s := range segments {
		if s.OriginSpanID == 0 || s.OriginDepth < 0 {
			return nil, fmt.Errorf("reverse segment %d has invalid origin", i)
		}
		if kind := reverseKind(s.Payload); kind == "" || kind != s.Kind {
			return nil, fmt.Errorf("reverse segment %d kind disagrees with its native payload", i)
		}
		if s.TTL != nil && (*s.TTL < 0 || *s.TTL > 255) {
			return nil, fmt.Errorf("reverse segment %d TTL must fit one byte", i)
		}
		frame := uint64(len(s.Payload)) << 1
		if s.TTL != nil {
			frame |= 1
		}
		out = binary.AppendUvarint(out, frame)
		origin := BigEndian8(s.OriginSpanID)
		out = append(out, origin[:]...)
		out = binary.AppendUvarint(out, uint64(s.OriginDepth))
		if s.TTL != nil {
			out = append(out, byte(*s.TTL))
		}
		out = append(out, s.Payload...)
	}
	return out, nil
}

func EncodeReverseContext(segments []ReverseSegment) ([]byte, error) {
	return EncodeReverseEnvelope(segments)
}

// DecodeReverseEnvelope owns the returned payload buffers and never invents a
// parent or substitutes the exporter's identity for the origin. A malformed
// bundle fails atomically, including truncation after a valid first record.
func DecodeReverseEnvelope(encoded []byte) ([]ReverseSegment, error) {
	if len(encoded) == 0 {
		return nil, nil
	}
	if encoded[0] != ReverseEnvelopeVersion {
		return nil, fmt.Errorf("unsupported reverse bundle version %d", encoded[0])
	}
	encoded = encoded[1:]
	if len(encoded) == 0 {
		return nil, fmt.Errorf("reverse bundle header has no records")
	}
	var out []ReverseSegment
	for len(encoded) > 0 {
		frame, n := binary.Uvarint(encoded)
		if n <= 0 || frame>>1 == 0 {
			return nil, fmt.Errorf("reverse record %d has invalid length", len(out))
		}
		encoded = encoded[n:]
		if len(encoded) < 9 {
			return nil, fmt.Errorf("reverse record %d lacks origin metadata", len(out))
		}
		origin := binary.BigEndian.Uint64(encoded[:8])
		depth, n := binary.Uvarint(encoded[8:])
		if origin == 0 || n <= 0 || depth > math.MaxInt {
			return nil, fmt.Errorf("reverse record %d has invalid origin metadata", len(out))
		}
		encoded = encoded[8+n:]
		var ttl *int
		if frame&1 != 0 {
			if len(encoded) == 0 {
				return nil, fmt.Errorf("reverse record %d lacks TTL", len(out))
			}
			value := int(encoded[0])
			ttl = &value
			encoded = encoded[1:]
		}
		length := frame >> 1
		if length > uint64(len(encoded)) {
			return nil, fmt.Errorf("reverse record %d has truncated payload", len(out))
		}
		payload := encoded[:int(length)]
		kind := reverseKind(payload)
		if kind == "" {
			return nil, fmt.Errorf("reverse record %d has unsupported native payload type", len(out))
		}
		out = append(out, ReverseSegment{Kind: kind, OriginSpanID: origin, OriginDepth: int(depth),
			Payload: append([]byte(nil), payload...), TTL: ttl, OriginCheckpointDepth: -1})
		encoded = encoded[int(length):]
	}
	return out, nil
}

func DecodeReverseContext(encoded []byte) ([]ReverseSegment, error) {
	return DecodeReverseEnvelope(encoded)
}
