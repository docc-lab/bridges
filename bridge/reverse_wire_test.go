package bridge

import (
	"bytes"
	"testing"
)

func TestReverseBinaryLiteralLayout(t *testing.T) {
	ttl := 2
	segments := []ReverseSegment{
		{Kind: "checkpoint.pb", OriginSpanID: 9, OriginDepth: 3, Payload: []byte{5, 3, 0xaa, 0xbb}},
		{Kind: "checkpoint.sb", OriginSpanID: 10, OriginDepth: 3, Payload: []byte{7, 3, 0xcc, 0xdd}, TTL: &ttl},
	}
	// One bundle byte and one framing byte per record. The low length bit
	// marks a present TTL; probability-mode metadata occupies no TTL byte.
	want := []byte{
		1,
		8, 0, 0, 0, 0, 0, 0, 0, 9, 3, 5, 3, 0xaa, 0xbb,
		9, 0, 0, 0, 0, 0, 0, 0, 10, 3, 2, 7, 3, 0xcc, 0xdd,
	}
	got, err := EncodeReverseContext(segments)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("encoded %x, want %x (error %v)", got, want, err)
	}
	decoded, err := DecodeReverseContext(want)
	if err != nil || len(decoded) != 2 || decoded[0].TTL != nil || decoded[1].TTL == nil || *decoded[1].TTL != 2 {
		t.Fatalf("incorrect probability/TTL records: %+v, %v", decoded, err)
	}
	for i, segment := range decoded {
		if segment.OriginSpanID != segments[i].OriginSpanID || !bytes.Equal(segment.Payload, segments[i].Payload) {
			t.Fatal("binary layout changed origin or native truss")
		}
	}
	// A partial final record must not expose an apparently complete bundle.
	for n := 1; n < len(want); n++ {
		if n == 15 { // exactly the first record is a valid one-record bundle
			continue
		}
		if partial, err := DecodeReverseContext(want[:n]); err == nil || partial != nil {
			t.Fatalf("truncation at %d returned partial evidence", n)
		}
	}
}

func TestReverseBinaryLargePayloadAndDepth(t *testing.T) {
	payload := make([]byte, 130)
	payload[0], payload[1], payload[2] = 5, 128, 1
	segment := ReverseSegment{Kind: "checkpoint.pb", OriginSpanID: 1, OriginDepth: 128, Payload: payload}
	encoded, err := EncodeReverseContext([]ReverseSegment{segment})
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) != 1+2+8+2+130 {
		t.Fatalf("incorrect multi-byte length/depth accounting: %d", len(encoded))
	}
	decoded, err := DecodeReverseContext(encoded)
	if err != nil || len(decoded) != 1 || decoded[0].OriginDepth != 128 || !bytes.Equal(decoded[0].Payload, payload) {
		t.Fatalf("long record changed: %v", err)
	}
	encoded[len(encoded)-1] ^= 255
	if !bytes.Equal(decoded[0].Payload, payload) {
		t.Fatal("decoded evidence aliases the transport buffer")
	}
	empty, err := EncodeReverseContext(nil)
	if err != nil || len(empty) != 0 {
		t.Fatal("empty baggage must occupy zero bytes")
	}
	if empty, err := DecodeReverseContext(nil); err != nil || len(empty) != 0 {
		t.Fatal("empty baggage did not decode as empty")
	}
}
