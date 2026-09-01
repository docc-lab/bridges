package bridge

import "testing"

func TestSB3EmitsOrdinalsOnlyForSecondAndLaterChildren(t *testing.T) {
	h := NewSB3Handler(4, 4, DefaultBloomFPRate, nil)
	h.Capture = true
	tid, root := uint64(0x100), uint64(0x10)
	children := []uint64{0x11, 0x12, 0x13}
	h.OnStart(&Event{TraceID: tid, SpanID: root}, 0)
	payloads := make(map[uint64][]byte)
	for i, sid := range children {
		h.OnStart(&Event{TraceID: tid, SpanID: sid, ParentID: root}, i+1)
		r := h.OnEnd(&Event{TraceID: tid, SpanID: sid, ParentID: root})
		payloads[sid] = r.Payload
	}
	h.OnEnd(&Event{TraceID: tid, SpanID: root})

	decode := func(sid uint64) SB3Payload {
		p, err := DecodeSB3Payload(payloads[sid], h.prefixLen, h.bloomLen, h.FPBits, false)
		if err != nil {
			t.Fatalf("decode %x: %v", sid, err)
		}
		return p
	}
	a, b, c := decode(children[0]), decode(children[1]), decode(children[2])
	if len(a.Branches) != 0 {
		t.Fatalf("first child branches=%v, want none", a.Branches)
	}
	if len(b.Branches) != 1 || b.Branches[0].Ord != 2 {
		t.Fatalf("second child branches=%v, want [2]", b.Branches)
	}
	if len(b.Branches[0].EE) != 1 || b.Branches[0].EE[0] != 1 {
		t.Fatalf("second child EE=%v, want [1]", b.Branches[0].EE)
	}
	if len(c.Branches) != 1 || c.Branches[0].Ord != 3 {
		t.Fatalf("third child branches=%v, want [3]", c.Branches)
	}
	if len(c.Branches[0].EE) != 1 || c.Branches[0].EE[0] != 2 {
		t.Fatalf("third child EE=%v, want [2]", c.Branches[0].EE)
	}
	if len(b.HA) != 1 || b.HA[0].ParentID != root || b.HA[0].Depth != 1 {
		t.Fatalf("second child HA=%v, want root fanout", b.HA)
	}
	if len(c.HA) != 0 {
		t.Fatalf("third child HA=%v, CGPRB should record fanout only on child 2", c.HA)
	}
}

func TestSB3LehmerRoundTrip(t *testing.T) {
	h := NewSB3Handler(4, 4, DefaultBloomFPRate, nil)
	h.Capture = true
	h.LehmerEE = true
	tid, root := uint64(0x200), uint64(0x20)
	h.OnStart(&Event{TraceID: tid, SpanID: root}, 0)
	for i, sid := range []uint64{0x21, 0x22} {
		h.OnStart(&Event{TraceID: tid, SpanID: sid, ParentID: root}, i+1)
		r := h.OnEnd(&Event{TraceID: tid, SpanID: sid, ParentID: root})
		if i == 1 {
			p, err := DecodeSB3Payload(r.Payload, h.prefixLen, h.bloomLen, h.FPBits, true)
			if err != nil {
				t.Fatal(err)
			}
			if len(p.Branches) != 1 || p.Branches[0].Ord != 2 || len(p.Branches[0].EE) != 1 || p.Branches[0].EE[0] != 1 {
				t.Fatalf("decoded sparse Lehmer branch=%v", p.Branches)
			}
		}
	}
}

func TestSB3KeepsDelayedEndEventEncoding(t *testing.T) {
	h := NewSB3Handler(4, 4, DefaultBloomFPRate, nil)
	tid, root := uint64(0x300), uint64(0x3030000000000000)
	var delayed [][]byte
	h.DEESink = func(_ uint64, q []byte) { delayed = append(delayed, append([]byte(nil), q...)) }
	h.OnStart(&Event{TraceID: tid, SpanID: root}, 0)
	children := []uint64{0x31, 0x32, 0x33}
	for i, sid := range children {
		h.OnStart(&Event{TraceID: tid, SpanID: sid, ParentID: root}, i+1)
	}
	for _, sid := range children {
		h.OnEnd(&Event{TraceID: tid, SpanID: sid, ParentID: root})
	}
	h.OnEnd(&Event{TraceID: tid, SpanID: root})
	if len(delayed) != 1 {
		t.Fatalf("DEE batches=%d, want 1", len(delayed))
	}
	q, err := DecodeDEEQuads(delayed[0], h.FPBits)
	if err != nil {
		t.Fatal(err)
	}
	if len(q) != 1 || q[0].Depth != 0 || q[0].OwnerFP != root>>48 || len(q[0].Seqs) != 2 || q[0].Seqs[0] != 1 || q[0].Seqs[1] != 2 {
		t.Fatalf("decoded DEE=%+v, want owner=root seqs=[1 2]", q)
	}
}
