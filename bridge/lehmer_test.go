package bridge

import (
	"reflect"
	"testing"
)

func visitPartialPermutations(n, k int, fn func([]int)) {
	used := make([]bool, n+1)
	seq := make([]int, k)
	var visit func(int)
	visit = func(i int) {
		if i == k {
			fn(append([]int(nil), seq...))
			return
		}
		for v := 1; v <= n; v++ {
			if used[v] {
				continue
			}
			used[v] = true
			seq[i] = v
			visit(i + 1)
			used[v] = false
		}
	}
	visit(0)
}

func TestPartialPermutationLehmerRoundTrip(t *testing.T) {
	for n := 0; n <= 7; n++ {
		for k := 0; k <= n; k++ {
			visitPartialPermutations(n, k, func(want []int) {
				encoded, err := encodePartialPermutation(n, want)
				if err != nil {
					t.Fatalf("encode n=%d seq=%v: %v", n, want, err)
				}
				if len(encoded) != partialPermutationBytes(n, k) {
					t.Fatalf("width n=%d k=%d: got %d", n, k, len(encoded))
				}
				got, err := decodePartialPermutation(n, k, encoded)
				if err != nil {
					t.Fatalf("decode n=%d seq=%v: %v", n, want, err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("round trip n=%d: got %v, want %v", n, got, want)
				}
			})
		}
	}
}

func TestPartialPermutationLehmerRejectsInvalidInput(t *testing.T) {
	for _, seq := range [][]int{{0}, {4}, {2, 2}} {
		if _, err := encodePartialPermutation(3, seq); err == nil {
			t.Errorf("encode n=3 seq=%v unexpectedly succeeded", seq)
		}
	}
	// P(3,2)=6, so the one-byte ranks 6 and above are invalid.
	if _, err := decodePartialPermutation(3, 2, []byte{6}); err == nil {
		t.Error("out-of-range rank unexpectedly decoded")
	}
}

func TestSBridgeLehmerGroupsRoundTrip(t *testing.T) {
	const (
		cpd    = 4
		fpBits = 16
	)
	ckpt := [8]byte{0x11, 0x22, 0x33, 0x44}
	chain := []bcEntry{
		{ord: 1},
		{ord: 7, fp: 0xabcd, hasFp: true, ee: []int{5, 1, 4}},
		{ord: 4, fp: 0xdef0, hasFp: true, ee: []int{2, 1}},
	}
	tid := TraceIDHexTo16Bytes("928f188ef2409811")
	dee := append(
		EncodeDEEQuadLehmer(tid, 2, 0xabcd, fpBits, 7, []int{6, 2, 5}),
		EncodeDEEQuadLehmer(tid, 5, 0x1122, fpBits, 3, []int{3})...,
	)
	payload := PackSBridgeBRLehmer(3, ckpt, 4, chain, dee, fpBits)
	if len(payload) != sbridgeBRSizeWithCoding(3, chain, dee, fpBits, 4, true) {
		t.Fatalf("size estimator=%d, payload=%d", sbridgeBRSizeWithCoding(3, chain, dee, fpBits, 4, true), len(payload))
	}
	got, err := DecodeSBridgeBRLehmer(payload, cpd, fpBits, 4)
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if !reflect.DeepEqual(got.Chain[1].EE, []int{5, 1, 4}) ||
		!reflect.DeepEqual(got.Chain[2].EE, []int{2, 1}) {
		t.Fatalf("EE round trip: %+v", got.Chain)
	}
	wantDEE := []DEEQuad{
		{TraceID16: tid, Depth: 2, OwnerFP: 0xabcd, Seqs: []int{6, 2, 5}},
		{TraceID16: tid, Depth: 5, OwnerFP: 0x1122, Seqs: []int{3}},
	}
	if !reflect.DeepEqual(got.DEE, wantDEE) {
		t.Fatalf("inline DEE = %+v, want %+v", got.DEE, wantDEE)
	}
	standalone, err := DecodeDEEQuadsLehmer(dee, fpBits)
	if err != nil {
		t.Fatalf("decode standalone DEE: %v", err)
	}
	if !reflect.DeepEqual(standalone, wantDEE) {
		t.Fatalf("standalone DEE = %+v, want %+v", standalone, wantDEE)
	}
}

func TestSBridgeHandlerLehmerPreservesEndGroups(t *testing.T) {
	const (
		tid  = uint64(9)
		root = uint64(0x1111000000000001)
		c1   = uint64(0x2222000000000001)
		c2   = uint64(0x3333000000000001)
		c3   = uint64(0x4444000000000001)
	)
	h := NewSBridgeHandler(100, nil)
	h.LehmerEE = true
	payloads := map[uint64][]byte{}
	var dees [][]byte
	h.EmitSink = func(_, sid uint64, p []byte) { payloads[sid] = append([]byte(nil), p...) }
	h.DEESink = func(_ uint64, q []byte) { dees = append(dees, append([]byte(nil), q...)) }
	event := func(sid, pid uint64) *Event {
		return &Event{TraceID: tid, SpanID: sid, ParentID: pid, ServiceID: 1}
	}
	h.OnStart(event(root, 0), 0)
	h.OnStart(event(c1, root), 1)
	h.OnEnd(event(c1, root))
	h.OnStart(event(c2, root), 2)
	h.OnStart(event(c3, root), 3)
	h.OnEnd(event(c3, root))
	h.OnEnd(event(c2, root))
	h.OnEnd(event(root, 0))

	br, err := DecodeSBridgeBRLehmer(payloads[c2], 100, 16, 4)
	if err != nil {
		t.Fatalf("decode child payload: %v", err)
	}
	if got := br.Chain[len(br.Chain)-1].EE; !reflect.DeepEqual(got, []int{1}) {
		t.Fatalf("child-2 EE = %v, want [1]", got)
	}
	if len(dees) != 1 {
		t.Fatalf("DEE groups = %d, want 1", len(dees))
	}
	qs, err := DecodeDEEQuadsLehmer(dees[0], 16)
	if err != nil {
		t.Fatalf("decode DEE: %v", err)
	}
	if len(qs) != 1 || !reflect.DeepEqual(qs[0].Seqs, []int{3}) {
		t.Fatalf("DEE = %+v, want seqs [3]", qs)
	}
}
