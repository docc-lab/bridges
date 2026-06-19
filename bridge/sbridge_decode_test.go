package bridge

import (
	"reflect"
	"testing"
)

// TestDecodeSBridgeBRRoundTrip packs a breadcrumb chain (with per-level EE) plus
// a couple of trailing DEE quads, then decodes it back and checks structural
// equality. cpd=4 is chosen so the chain mixes a checkpoint-rooted level (no fp)
// with non-checkpoint levels (fp present) — exercising the implicit-fp recompute.
func TestDecodeSBridgeBRRoundTrip(t *testing.T) {
	const cpd = 4
	ckpt := [4]byte{0x11, 0x22, 0x33, 0x44}

	// Span at depth 3, window root at depth 0 → chain levels at depths 1,2,3.
	// hasFp must equal (levelDepth-1)%cpd != 0: depth1→false, depth2→true, depth3→true.
	chain := []bcEntry{
		{ord: 2},                                            // depth 1, parent = ckpt root
		{ord: 1, fp: 0xabcd, hasFp: true},                   // depth 2
		{ord: 3, fp: 0xdef0, hasFp: true, ee: []int{5, 6}},  // depth 3, has EE
	}

	dee := append(
		EncodeDEEQuad(TraceIDHexTo16Bytes("928f188ef2409811"), 2, 0xabcd1234, []int{1, 2}),
		EncodeDEEQuad([16]byte{}, 5, 0x11223344, []int{7})...,
	)

	payload := PackSBridgeBR(3, ckpt, chain, dee, 16)

	got, err := DecodeSBridgeBR(payload, cpd, 16)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	want := SBridgeBR{
		Depth: 3,
		Ckpt4: ckpt,
		Chain: []SBChainLevel{
			{Depth: 1, Ord: 2, HasFP: false, FP: 0},
			{Depth: 2, Ord: 1, HasFP: true, FP: 0xabcd},
			{Depth: 3, Ord: 3, HasFP: true, FP: 0xdef0, EE: []int{5, 6}},
		},
		DEE: []DEEQuad{
			{TraceID16: TraceIDHexTo16Bytes("928f188ef2409811"), Depth: 2, OwnerFP: 0xabcd1234, Seqs: []int{1, 2}},
			{TraceID16: [16]byte{}, Depth: 5, OwnerFP: 0x11223344, Seqs: []int{7}},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("round-trip mismatch:\n got  %+v\n want %+v", got, want)
	}

	// Empty payload edge: depth 0, no chain, no dee.
	empty := PackSBridgeBR(0, [4]byte{}, nil, nil, 16)
	be, err := DecodeSBridgeBR(empty, cpd, 16)
	if err != nil {
		t.Fatalf("decode empty: %v", err)
	}
	if be.Depth != 0 || len(be.Chain) != 0 || len(be.DEE) != 0 {
		t.Errorf("empty decode = %+v, want zero-ish", be)
	}

	// Sub-byte and wide fp widths must round-trip the bit-packed fp section.
	for _, w := range []int{8, 10, 12, 16, 20, 24} {
		mask := uint32((1 << uint(w)) - 1)
		ch := []bcEntry{
			{ord: 1},                                          // depth1, no fp
			{ord: 2, fp: 0xABCDEF & mask, hasFp: true},        // depth2
			{ord: 3, fp: 0x123456 & mask, hasFp: true},        // depth3
		}
		p := PackSBridgeBR(3, ckpt, ch, nil, w)
		d, err := DecodeSBridgeBR(p, cpd, w)
		if err != nil {
			t.Fatalf("w=%d decode: %v", w, err)
		}
		if d.Chain[1].FP != (0xABCDEF&mask) || d.Chain[2].FP != (0x123456&mask) {
			t.Errorf("w=%d: fps round-tripped to %x,%x want %x,%x", w,
				d.Chain[1].FP, d.Chain[2].FP, 0xABCDEF&mask, 0x123456&mask)
		}
	}

	// DecodeDEEQuads standalone on the same dee blob.
	quads, err := DecodeDEEQuads(dee)
	if err != nil {
		t.Fatalf("decode dee: %v", err)
	}
	if !reflect.DeepEqual(quads, want.DEE) {
		t.Errorf("DecodeDEEQuads = %+v, want %+v", quads, want.DEE)
	}
}
