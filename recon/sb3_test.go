package recon

import (
	"reflect"
	"testing"

	"bridges/bloom"
	"bridges/bridge"
)

type sb3TestSpan struct {
	id, parent uint64
	depth      int
	payload    []byte
}

func sb3TestBloom(t *testing.T, cfg Config, ids ...uint64) []byte {
	t.Helper()
	bf, err := bloom.New(cfg.BloomM, cfg.BloomK)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		key := bridge.HexOf(id)
		bf.Add(key[:])
	}
	return bf.ToBytes()
}

func sb3TestPrefix(id uint64, n int) []byte {
	b := bridge.BigEndian8(id)
	return append([]byte(nil), b[:n]...)
}

func sb3TestBridgeAnchors(res Result) map[uint64]uint64 {
	out := make(map[uint64]uint64, len(res.Bridges))
	for _, b := range res.Bridges {
		out[b.OrphanID] = b.AnchorID
	}
	return out
}

func nestedSB3Spans(t *testing.T) ([]sb3TestSpan, Config) {
	t.Helper()
	const (
		tid  = uint64(0xabc)
		root = uint64(0x10)
		a    = uint64(0x20)
		a1   = uint64(0x21)
		a2   = uint64(0x22)
		b    = uint64(0x30)
	)
	h := bridge.NewSB3Handler(4, 4, bridge.DefaultBloomFPRate, nil)
	h.Capture = true
	payloads := make(map[uint64][]byte)
	start := func(id, parent uint64, seq int) {
		r := h.OnStart(&bridge.Event{TraceID: tid, SpanID: id, ParentID: parent}, seq)
		if r.Payload != nil {
			payloads[id] = r.Payload
		}
	}
	end := func(id, parent uint64) {
		r := h.OnEnd(&bridge.Event{TraceID: tid, SpanID: id, ParentID: parent})
		if r.Payload != nil {
			payloads[id] = r.Payload
		}
	}
	start(root, 0, 0)
	start(a, root, 1)
	start(a1, a, 1)
	end(a1, a)
	start(a2, a, 2)
	end(a2, a)
	end(a, root)
	start(b, root, 2)
	end(b, root)
	end(root, 0)

	cfg := NewPCRBConfig(4, 4, bridge.DefaultBloomFPRate)
	cfg.FPBits = 16
	return []sb3TestSpan{
		{id: root, depth: 0, payload: payloads[root]},
		{id: a, parent: root, depth: 1},
		{id: a1, parent: a, depth: 2, payload: payloads[a1]},
		{id: a2, parent: a, depth: 2, payload: payloads[a2]},
		{id: b, parent: root, depth: 1, payload: payloads[b]},
	}, cfg
}

func decodeSB3TestSurvivors(t *testing.T, raw []sb3TestSpan, cfg Config, drop uint64) []Span {
	t.Helper()
	var out []Span
	for _, r := range raw {
		if r.id == drop {
			continue
		}
		s := Span{SpanID: r.id, ParentID: r.parent, Depth: r.depth}
		if r.payload != nil {
			d, prefix, bits, ha, ords, err := DecodeSB3SpanPayload(r.payload, cfg)
			if err != nil {
				t.Fatalf("decode %x: %v", r.id, err)
			}
			s.Depth, s.CkptPrefix, s.BloomBits, s.HA, s.SparseOrdinals = d, prefix, bits, ha, ords
			s.LeafCarrier = d%cfg.CPD != 0
		}
		out = append(out, s)
	}
	return out
}

func TestReconstructSB3AlignsNestedSparseChains(t *testing.T) {
	raw, cfg := nestedSB3Spans(t)
	res := ReconstructSB3(decodeSB3TestSurvivors(t, raw, cfg, 0), cfg)
	if !res.Compatible {
		t.Fatalf("alignment failed: %s (%d conflicts)", res.Reason, res.Conflicts)
	}
	root := res.Structure.Root
	if root == nil || root.RealID != 0x10 {
		t.Fatalf("root=%v", root)
	}
	a, b := root.Children[1], root.Children[2]
	if a == nil || a.RealID != 0x20 || b == nil || b.RealID != 0x30 {
		t.Fatalf("root children: ord1=%v ord2=%v", a, b)
	}
	if a.Children[1] == nil || a.Children[1].RealID != 0x21 || a.Children[2] == nil || a.Children[2].RealID != 0x22 {
		t.Fatalf("A children not sparsely ordered: %+v", a.Children)
	}
	if len(b.EE) != 1 || b.EE[0] != 1 || len(a.Children[2].EE) != 1 || a.Children[2].EE[0] != 1 {
		t.Fatalf("EE did not remain attached: B=%v A2=%v", b.EE, a.Children[2].EE)
	}
	if res.ImplicitOrdinals != 2 {
		t.Fatalf("implicit first-child ordinals=%d, want 2", res.ImplicitOrdinals)
	}
	if !res.StructureStatus.Complete {
		t.Fatalf("lateral structure was not completed: %+v", res.StructureStatus)
	}
	if !reflect.DeepEqual(root.EndOrder, []int{1, 2}) || !reflect.DeepEqual(a.EndOrder, []int{1, 2}) {
		t.Fatalf("materialized end orders root=%v A=%v", root.EndOrder, a.EndOrder)
	}
	truth := SBTruth{RootID: 0x10, ChildByOrd: map[uint64]map[int]uint64{
		0x10: {1: 0x20, 2: 0x30},
		0x20: {1: 0x21, 2: 0x22},
	}}
	structure := ScoreStructure(res.Structure, truth, map[uint64]int64{
		0x21: 1, 0x22: 2, 0x20: 3, 0x30: 4, 0x10: 5,
	}, nil)
	if !structure.EventOrderOK || !structure.CriticalPath {
		t.Fatalf("existing EE/DEE structure pass rejected SB3 tree: %+v", structure)
	}
}

func TestReconstructSB3AppliesDEEAfterImplicitFirstChildLabeling(t *testing.T) {
	const (
		tid  = uint64(0xabc123)
		root = uint64(0x1111_0000_0000_0001)
		l1   = uint64(0x2222_0000_0000_0002)
		l2   = uint64(0x3333_0000_0000_0003)
		l3   = uint64(0x4444_0000_0000_0004)
	)
	h := bridge.NewSB3Handler(8, 8, bridge.DefaultBloomFPRate, nil)
	h.Capture = true
	payloads := map[uint64][]byte{}
	var rawDEE [][]byte
	h.DEESink = func(_ uint64, q []byte) { rawDEE = append(rawDEE, append([]byte(nil), q...)) }
	start := func(id, parent uint64, ord int) {
		r := h.OnStart(&bridge.Event{TraceID: tid, SpanID: id, ParentID: parent}, ord)
		if r.Payload != nil {
			payloads[id] = r.Payload
		}
	}
	end := func(id, parent uint64) {
		r := h.OnEnd(&bridge.Event{TraceID: tid, SpanID: id, ParentID: parent})
		if r.Payload != nil {
			payloads[id] = r.Payload
		}
	}
	start(root, 0, 0)
	start(l1, root, 1)
	start(l2, root, 2)
	start(l3, root, 3)
	end(l1, root)
	end(l3, root)
	end(l2, root)
	end(root, 0)

	cfg := NewPCRBConfig(8, 8, bridge.DefaultBloomFPRate)
	cfg.FPBits = 16
	raw := []sb3TestSpan{
		{id: root, depth: 0, payload: payloads[root]},
		{id: l1, parent: root, depth: 1, payload: payloads[l1]},
		{id: l2, parent: root, depth: 1, payload: payloads[l2]},
		{id: l3, parent: root, depth: 1, payload: payloads[l3]},
	}
	var dees []bridge.DEEQuad
	for _, q := range rawDEE {
		decoded, err := bridge.DecodeDEEQuads(q, cfg.FPBits)
		if err != nil {
			t.Fatal(err)
		}
		dees = append(dees, decoded...)
	}
	if len(dees) != 1 || !reflect.DeepEqual(dees[0].Seqs, []int{1, 3}) {
		t.Fatalf("decoded DEE=%+v, want one [1 3] group", dees)
	}

	without := ReconstructSB3(decodeSB3TestSurvivors(t, raw, cfg, 0), cfg)
	if without.StructureStatus.Complete {
		t.Fatal("structure unexpectedly completed without the required DEE")
	}
	with := ReconstructSB3WithDEE(decodeSB3TestSurvivors(t, raw, cfg, 0), dees, cfg)
	if !with.Compatible || !with.StructureStatus.Complete {
		t.Fatalf("full SB3 reconstruction failed: compatible=%v structure=%+v", with.Compatible, with.StructureStatus)
	}
	if with.ImplicitOrdinals != 1 || with.Structure.Root.Children[1].Ord != 1 {
		t.Fatalf("implicit first child was not labeled exactly once: count=%d child=%+v",
			with.ImplicitOrdinals, with.Structure.Root.Children[1])
	}
	if got := with.Structure.Root.EndOrder; !reflect.DeepEqual(got, []int{1, 3, 2}) {
		t.Fatalf("root end order=%v, want [1 3 2]", got)
	}
}

func TestReconstructSB3TopoOnlyMarksStructureOmitted(t *testing.T) {
	raw, cfg := nestedSB3Spans(t)
	cfg.SB3TopoOnly = true
	res := ReconstructSB3(decodeSB3TestSurvivors(t, raw, cfg, 0), cfg)
	if !res.Compatible || !res.StructureStatus.Omitted || res.StructureStatus.Complete {
		t.Fatalf("topo-only structure status=%+v compatible=%v", res.StructureStatus, res.Compatible)
	}
}

func TestReconstructSB3PlacesOrdinalsOnRecoveredFanout(t *testing.T) {
	raw, cfg := nestedSB3Spans(t)
	const droppedA = uint64(0x20)
	res := ReconstructSB3(decodeSB3TestSurvivors(t, raw, cfg, droppedA), cfg)
	if !res.Compatible {
		t.Fatalf("alignment with recovered parent failed: %s (%d conflicts)", res.Reason, res.Conflicts)
	}
	if got := res.Topology.ReconParent[droppedA]; got != 0x10 {
		t.Fatalf("recovered A parent=%x, want root", got)
	}
	a := res.Structure.Root.Children[1]
	if a == nil || a.RealID != 0 || a.Children[1] == nil || a.Children[2] == nil {
		t.Fatalf("ordinals were not placed on recovered A: %+v", a)
	}
}

func TestReconstructCGP0DefaultsToFullEvidenceGreedy(t *testing.T) {
	raw, cfg := nestedSB3Spans(t)
	const droppedParent = uint64(0x20)
	survivors := decodeSB3TestSurvivors(t, raw, cfg, droppedParent)

	cgp := ReconstructCGP0(survivors, cfg)
	ablationCfg := cfg
	ablationCfg.SB3IgnoreOrdinals = true
	sb3 := ReconstructSB3(survivors, ablationCfg)
	if cgp.GreedyMode != "full-evidence" {
		t.Fatalf("CGP0 greedy mode=%q, want full-evidence", cgp.GreedyMode)
	}
	if !reflect.DeepEqual(cgp.ReconParent, sb3.Topology.ReconParent) {
		t.Fatalf("CGP0 and no-ordinal SB3 topology differ:\nCGP0=%v\nSB3=%v", cgp.ReconParent, sb3.Topology.ReconParent)
	}
	if cgp.GreedyHardConflicts != 0 {
		t.Fatalf("full-evidence CGP0 violated hard facts: parent=%d HA=%d",
			cgp.GreedyParentConflicts, cgp.GreedyHAConflicts)
	}

	legacyCfg := cfg
	legacyCfg.CGP0Legacy = true
	legacy := ReconstructCGP0(survivors, legacyCfg)
	if legacy.GreedyMode != "legacy-lean" {
		t.Fatalf("legacy CGP0 mode=%q", legacy.GreedyMode)
	}
	// Both surviving children literally name the dropped parent. The full
	// engine materializes that exact identity; the old lean emitter does not.
	for _, child := range []uint64{0x21, 0x22} {
		if got := cgp.ReconParent[child]; got != droppedParent {
			t.Fatalf("full CGP0 parent[%x]=%x, want exact dropped parent %x", child, got, droppedParent)
		}
	}
	if legacy.ReconParent[0x21] == droppedParent && legacy.ReconParent[0x22] == droppedParent {
		t.Fatal("legacy CGP0 unexpectedly materialized every exact-parent edge")
	}
	if legacy.GreedyParentConflicts == 0 {
		t.Fatal("legacy exact-parent evidence loss was not reported")
	}
}

func TestReconstructPB0DefaultsToFullPathEvidence(t *testing.T) {
	const (
		root  = uint64(0x01)
		wrong = uint64(0x10)
		right = uint64(0x20)
		m     = uint64(0x30)
		c1    = uint64(0x31)
		c2    = uint64(0x32)
	)
	cfg := NewPCRBConfig(4, 8, bridge.DefaultBloomFPRate)
	survivors := []Span{
		{SpanID: root, Depth: 0},
		{SpanID: wrong, ParentID: root, Depth: 1},
		{SpanID: right, ParentID: root, Depth: 1},
		{SpanID: c1, ParentID: m, Depth: 4, BloomBits: sb3TestBloom(t, cfg, wrong, right, m), CkptPrefix: sb3TestPrefix(root, 8)},
		{SpanID: c2, ParentID: m, Depth: 4, BloomBits: sb3TestBloom(t, cfg, right, m), CkptPrefix: sb3TestPrefix(root, 8)},
	}

	full := ReconstructPB0(survivors, cfg)
	if full.GreedyMode != "full-evidence" {
		t.Fatalf("PB0 greedy mode=%q, want full-evidence", full.GreedyMode)
	}
	anchors := sb3TestBridgeAnchors(full)
	for _, child := range []uint64{c1, c2} {
		if got := anchors[child]; got != right {
			t.Fatalf("pooled PB0 anchor for %x=%x, want %x", child, got, right)
		}
	}
	if full.GreedyParentConflicts != 0 || full.GreedyHAConflicts != 0 {
		t.Fatalf("full PB0 hard conflicts: parent=%d HA=%d", full.GreedyParentConflicts, full.GreedyHAConflicts)
	}

	legacyCfg := cfg
	legacyCfg.PB0Legacy = true
	legacy := ReconstructPB0(survivors, legacyCfg)
	if legacy.GreedyMode != "legacy-lean" {
		t.Fatalf("legacy PB0 mode=%q", legacy.GreedyMode)
	}
	if got := sb3TestBridgeAnchors(legacy)[c1]; got != wrong {
		t.Fatalf("legacy unpooled anchor=%x, want lower-ID false candidate %x", got, wrong)
	}
}

func TestReconstructPB0IgnoresHAExtension(t *testing.T) {
	const (
		root  = uint64(0x01)
		wrong = uint64(0x10)
		right = uint64(0x20)
		m     = uint64(0x30)
		child = uint64(0x31)
	)
	cfg := NewPCRBConfig(4, 8, bridge.DefaultBloomFPRate)
	base := []Span{
		{SpanID: root, Depth: 0},
		{SpanID: wrong, ParentID: root, Depth: 1},
		{SpanID: right, ParentID: root, Depth: 1},
		{SpanID: child, ParentID: m, Depth: 3, BloomBits: sb3TestBloom(t, cfg, wrong, right, m), CkptPrefix: sb3TestPrefix(root, 8)},
	}
	withHA := append([]Span(nil), base...)
	withHA[len(withHA)-1].HA = []HAEntry{{ParentID: right, Depth: 2}}

	without := ReconstructPB0(base, cfg)
	with := ReconstructPB0(withHA, cfg)
	if !reflect.DeepEqual(without.ReconParent, with.ReconParent) || !reflect.DeepEqual(without.Bridges, with.Bridges) {
		t.Fatalf("PB0 consumed HA evidence:\nwithout=%+v\nwith=%+v", without, with)
	}
	if with.GreedyHAConflicts != 0 {
		t.Fatalf("PB0 audited unavailable HA evidence: %d conflicts", with.GreedyHAConflicts)
	}
}

func TestReconstructSB3RejectsTwoImplicitChildrenAtFanout(t *testing.T) {
	const (
		root = uint64(0x01)
		a    = uint64(0x10)
		b    = uint64(0x20)
	)
	cfg := NewPCRBConfig(4, 8, bridge.DefaultBloomFPRate)
	survivors := []Span{
		{SpanID: root, Depth: 0, BloomBits: sb3TestBloom(t, cfg), CkptPrefix: make([]byte, 8)},
		{SpanID: a, ParentID: root, Depth: 1, BloomBits: sb3TestBloom(t, cfg, root), CkptPrefix: sb3TestPrefix(root, 8), LeafCarrier: true},
		{SpanID: b, ParentID: root, Depth: 1, BloomBits: sb3TestBloom(t, cfg, root), CkptPrefix: sb3TestPrefix(root, 8), LeafCarrier: true},
	}
	res := ReconstructSB3(survivors, cfg)
	if res.Compatible || res.Conflicts == 0 {
		t.Fatalf("two implicit child edges were accepted: compatible=%v conflicts=%d", res.Compatible, res.Conflicts)
	}
}

// A no-ordinal SB3 run must preserve the ordinary deepest/ID greedy choice.
// In particular, it must not introduce a global open-end reservation that
// silently turns the CGP0-style baseline into a different topology solver.
func TestReconstructSB3NoOrdinalsKeepsBloomGreedyAnchor(t *testing.T) {
	const (
		tid   = uint64(0xdef)
		root  = uint64(0x01)
		wrong = uint64(0x10) // root child 1; lower ID wins a Bloom-only tie
		wleaf = uint64(0x11) // keeps wrong non-leaf while carrying its empty chain
		right = uint64(0x20) // root child 2; true ancestor of M
		m     = uint64(0x30) // dropped exact parent and fanout
		c1    = uint64(0x31)
		c2    = uint64(0x32)
	)
	h := bridge.NewSB3Handler(4, 4, bridge.DefaultBloomFPRate, nil)
	h.Capture = true
	payloads := make(map[uint64][]byte)
	start := func(id, parent uint64, seq int) {
		r := h.OnStart(&bridge.Event{TraceID: tid, SpanID: id, ParentID: parent}, seq)
		if r.Payload != nil {
			payloads[id] = r.Payload
		}
	}
	end := func(id, parent uint64) {
		r := h.OnEnd(&bridge.Event{TraceID: tid, SpanID: id, ParentID: parent})
		if r.Payload != nil {
			payloads[id] = r.Payload
		}
	}
	start(root, 0, 0)
	start(wrong, root, 1)
	start(wleaf, wrong, 1)
	end(wleaf, wrong)
	end(wrong, root)
	start(right, root, 2)
	start(m, right, 1)
	start(c1, m, 1)
	end(c1, m)
	start(c2, m, 2)
	end(c2, m)
	end(m, right)
	end(right, root)
	end(root, 0)

	cfg := NewPCRBConfig(4, 4, bridge.DefaultBloomFPRate)
	cfg.FPBits = 16
	raw := []sb3TestSpan{
		{id: root, depth: 0, payload: payloads[root]},
		{id: wrong, parent: root, depth: 1},
		{id: wleaf, parent: wrong, depth: 2, payload: payloads[wleaf]},
		{id: right, parent: root, depth: 1},
		{id: c1, parent: m, depth: 3, payload: payloads[c1]},
		{id: c2, parent: m, depth: 3, payload: payloads[c2]},
	}
	survivors := decodeSB3TestSurvivors(t, raw, cfg, 0)
	for i := range survivors {
		if survivors[i].SpanID == c1 || survivors[i].SpanID == c2 {
			// Force both same-depth surviving candidates to pass every Bloom test.
			for j := range survivors[i].BloomBits {
				survivors[i].BloomBits[j] = 0xff
			}
		}
	}

	// Prove the ordinary deepest/ID greedy starting point is the wrong sibling.
	sk := cgpParse(survivors, cfg)
	cgpResolveEvidence(sk, cfg)
	cgpResolveAnchors(sk, cfg)
	ev := sb3CollectFragmentEvidence(sk, cfg)
	units := sb3IntersectRouteUnits(sk, ev)
	var base *sb3RouteUnit
	for _, u := range units {
		if u.parentID == m {
			base = u
			break
		}
	}
	if base == nil || base.anchor == nil || base.anchor.SpanID != wrong {
		t.Fatalf("Bloom-only base anchor=%v, want wrong sibling %x", base, wrong)
	}

	ablationCfg := cfg
	ablationCfg.SB3IgnoreOrdinals = true
	ablated := ReconstructSB3(survivors, ablationCfg)
	if got := ablated.Topology.ReconParent[m]; got != wrong {
		t.Fatalf("no-ordinal parent of M=%x, want CGP0-style base %x", got, wrong)
	}
	if ablated.HardOverrides != 0 {
		t.Fatalf("non-ordinal heuristic overrode the greedy base %d times", ablated.HardOverrides)
	}
	if ablated.OrdinalGuidance || ablated.OrdinalOverrides != 0 {
		t.Fatalf("ordinal ablation still used guidance: enabled=%v overrides=%d",
			ablated.OrdinalGuidance, ablated.OrdinalOverrides)
	}
	if ablated.Topology.SB3OrdinalChecked {
		t.Fatal("ordinal ablation topology was charged for ignored ordinal evidence")
	}

	// The legacy lean reconstructor remains a regression oracle for the ordinary
	// first deepest/ID anchor preference. Default CGP0 now shares the full
	// evidence engine under test here.
	want := sb3TestBridgeAnchors(ReconstructCGP0Legacy(survivors, cfg))
	got := sb3TestBridgeAnchors(ablated.Topology)
	for _, orphan := range []uint64{c1, c2} {
		if got[orphan] != want[orphan] {
			t.Fatalf("orphan %x anchor=%x, CGP0 anchor=%x", orphan, got[orphan], want[orphan])
		}
	}
}

// Repeated witnesses for one ordinal may coexist only inside one immediate
// child subtree of a fanout. This trace makes W and P equally Bloom-admissible:
// attaching M below W would assign ordinal 2 to both W->WS and W->M, while
// attaching it below P yields the legal P children RF (implicit) and M (2).
func TestReconstructSB3GreedyUsesOrdinalCollisionToChooseAnchor(t *testing.T) {
	const (
		tid   = uint64(0xfed)
		root  = uint64(0x01)
		wrong = uint64(0x10)
		wf    = uint64(0x11)
		ws    = uint64(0x12)
		right = uint64(0x20)
		rf    = uint64(0x21)
		m     = uint64(0x30)
		c     = uint64(0x31)
	)
	h := bridge.NewSB3Handler(4, 4, bridge.DefaultBloomFPRate, nil)
	h.Capture = true
	payloads := make(map[uint64][]byte)
	start := func(id, parent uint64, seq int) {
		r := h.OnStart(&bridge.Event{TraceID: tid, SpanID: id, ParentID: parent}, seq)
		if r.Payload != nil {
			payloads[id] = r.Payload
		}
	}
	end := func(id, parent uint64) {
		r := h.OnEnd(&bridge.Event{TraceID: tid, SpanID: id, ParentID: parent})
		if r.Payload != nil {
			payloads[id] = r.Payload
		}
	}
	start(root, 0, 0)
	start(wrong, root, 1)
	start(wf, wrong, 1)
	end(wf, wrong)
	start(ws, wrong, 2)
	end(ws, wrong)
	end(wrong, root)
	start(right, root, 2)
	start(rf, right, 1)
	end(rf, right)
	start(m, right, 2)
	start(c, m, 1)
	end(c, m)
	end(m, right)
	end(right, root)
	end(root, 0)

	cfg := NewPCRBConfig(4, 4, bridge.DefaultBloomFPRate)
	cfg.FPBits = 16
	raw := []sb3TestSpan{
		{id: root, depth: 0, payload: payloads[root]},
		{id: wrong, parent: root, depth: 1},
		{id: wf, parent: wrong, depth: 2, payload: payloads[wf]},
		{id: ws, parent: wrong, depth: 2, payload: payloads[ws]},
		{id: right, parent: root, depth: 1},
		{id: rf, parent: right, depth: 2, payload: payloads[rf]},
		{id: c, parent: m, depth: 3, payload: payloads[c]},
	}
	survivors := decodeSB3TestSurvivors(t, raw, cfg, 0)
	for i := range survivors {
		if survivors[i].SpanID == c {
			for j := range survivors[i].BloomBits {
				survivors[i].BloomBits[j] = 0xff
			}
			// The ordinal collision, rather than the ordinary HA witness for
			// right's second child, is the evidence under test.
			survivors[i].HA = nil
		}
	}

	sk := cgpParse(survivors, cfg)
	cgpResolveEvidence(sk, cfg)
	cgpResolveAnchors(sk, cfg)
	units := sb3IntersectRouteUnits(sk, sb3CollectFragmentEvidence(sk, cfg))
	var base *sb3RouteUnit
	for _, u := range units {
		if u.parentID == m {
			base = u
			break
		}
	}
	if base == nil || base.anchor == nil || base.anchor.SpanID != wrong {
		t.Fatalf("Bloom-only base anchor=%v, want wrong sibling %x", base, wrong)
	}

	res := ReconstructSB3(survivors, cfg)
	if !res.Compatible {
		t.Fatalf("ordinal-aware reconstruction failed: %s (%d conflicts)", res.Reason, res.Conflicts)
	}
	if got := res.Topology.ReconParent[m]; got != right {
		t.Fatalf("ordinal-aware parent of M=%x, want %x", got, right)
	}
	if res.OrdinalOverrides == 0 || res.CandidateEvaluations < 2 {
		t.Fatalf("ordinal collision did not prune the first route: overrides=%d evaluations=%d",
			res.OrdinalOverrides, res.CandidateEvaluations)
	}

	ablationCfg := cfg
	ablationCfg.SB3IgnoreOrdinals = true
	ablated := ReconstructSB3(survivors, ablationCfg)
	if got := ablated.Topology.ReconParent[m]; got != wrong {
		t.Fatalf("ordinal ablation parent of M=%x, want Bloom-only base %x", got, wrong)
	}
}

// Matching ParentIDs are a hard join. A false anchor present in only one
// child's Bloom must not survive the exact-parent group's intersection.
func TestSB3PoolsBloomsAcrossMatchingParentIDs(t *testing.T) {
	const (
		root  = uint64(0x01)
		wrong = uint64(0x10)
		right = uint64(0x20)
		m     = uint64(0x30)
		c1    = uint64(0x31)
		c2    = uint64(0x32)
	)
	cfg := NewPCRBConfig(4, 8, bridge.DefaultBloomFPRate)
	survivors := []Span{
		{SpanID: root, Depth: 0, BloomBits: sb3TestBloom(t, cfg), CkptPrefix: make([]byte, 8)},
		{SpanID: wrong, ParentID: root, Depth: 1},
		{SpanID: right, ParentID: root, Depth: 1},
		{SpanID: c1, ParentID: m, Depth: 4, BloomBits: sb3TestBloom(t, cfg, wrong, right), CkptPrefix: sb3TestPrefix(root, 8)},
		{SpanID: c2, ParentID: m, Depth: 4, BloomBits: sb3TestBloom(t, cfg, right), CkptPrefix: sb3TestPrefix(root, 8)},
	}
	sk := cgpParse(survivors, cfg)
	cgpResolveEvidence(sk, cfg)
	cgpResolveAnchors(sk, cfg)
	units := sb3IntersectRouteUnits(sk, sb3CollectFragmentEvidence(sk, cfg))
	var got *sb3RouteUnit
	for _, u := range units {
		if u.parentID == m {
			got = u
			break
		}
	}
	if got == nil || got.anchor == nil {
		t.Fatalf("missing exact-parent route unit: %+v", got)
	}
	if got.anchor.SpanID != right {
		t.Fatalf("pooled anchor=%x, want %x; one-branch false positive %x leaked through", got.anchor.SpanID, right, wrong)
	}
	if !got.knownFanout {
		t.Fatal("matching ParentIDs did not mark the recovered parent as a known fanout")
	}
}

// HA records on carriers in different exact-parent groups prove that both
// routes descend through F. Their Blooms must therefore be pooled for claims
// at or above F; a false anchor visible in only one branch is rejected.
func TestSB3PoolsHAWitnessBloomsAcrossRouteUnits(t *testing.T) {
	const (
		root  = uint64(0x01)
		wrong = uint64(0x10)
		right = uint64(0x20)
		f     = uint64(0x28)
		m1    = uint64(0x30)
		m2    = uint64(0x40)
		c1    = uint64(0x31)
		c2    = uint64(0x41)
	)
	cfg := NewPCRBConfig(4, 8, bridge.DefaultBloomFPRate)
	survivors := []Span{
		{SpanID: root, Depth: 0, BloomBits: sb3TestBloom(t, cfg), CkptPrefix: make([]byte, 8)},
		{SpanID: wrong, ParentID: root, Depth: 1},
		{SpanID: right, ParentID: root, Depth: 1},
		{
			SpanID: c1, ParentID: m1, Depth: 4,
			BloomBits: sb3TestBloom(t, cfg, wrong, right, f), CkptPrefix: sb3TestPrefix(root, 8),
			HA: []HAEntry{{ParentID: f, Depth: 3}},
		},
		{
			SpanID: c2, ParentID: m2, Depth: 4,
			BloomBits: sb3TestBloom(t, cfg, right, f), CkptPrefix: sb3TestPrefix(root, 8),
			HA: []HAEntry{{ParentID: f, Depth: 3}},
		},
	}
	sk := cgpParse(survivors, cfg)
	cgpResolveEvidence(sk, cfg)
	cgpResolveAnchors(sk, cfg)
	units := sb3IntersectRouteUnits(sk, sb3CollectFragmentEvidence(sk, cfg))
	for _, parentID := range []uint64{m1, m2} {
		var got *sb3RouteUnit
		for _, u := range units {
			if u.parentID == parentID {
				got = u
				break
			}
		}
		if got == nil || got.anchor == nil {
			t.Fatalf("missing route unit for %x", parentID)
		}
		if got.anchor.SpanID != right {
			t.Fatalf("unit %x anchor=%x, want pooled HA-group anchor %x", parentID, got.anchor.SpanID, right)
		}
		if got.requiredFanout[2] != f || got.nodeChoice[2] != f {
			t.Fatalf("unit %x did not hard-route through HA fanout %x: required=%x choice=%x",
				parentID, f, got.requiredFanout[2], got.nodeChoice[2])
		}
	}
	accepted := sb3SeedGreedyParent(sk, units)
	for _, u := range units {
		sb3ApplyUnitRoute(accepted, u)
	}
	topo := sb3EmitGreedyTopology(sk, units, accepted)
	if parentConflicts, haConflicts, _ := sb3CheckHardEvidence(survivors, topo); parentConflicts+haConflicts != 0 {
		t.Fatalf("emitted topology violates hard constraints: parent=%d HA=%d", parentConflicts, haConflicts)
	}

	noGroupCfg := cfg
	noGroupCfg.GreedyNoGroupedEvidence = true
	noGroupUnits := sb3IntersectRouteUnitsWithConfig(sk, sb3CollectFragmentEvidence(sk, cfg), noGroupCfg)
	var noGroupM1 *sb3RouteUnit
	for _, u := range noGroupUnits {
		if u.parentID == m1 {
			noGroupM1 = u
			break
		}
	}
	if noGroupM1 == nil || noGroupM1.anchor == nil || noGroupM1.anchor.SpanID != wrong {
		t.Fatalf("grouped-evidence ablation did not restore the one-branch false positive: %+v", noGroupM1)
	}

	noHardCfg := cfg
	noHardCfg.GreedyNoHardHA = true
	noHardUnits := sb3IntersectRouteUnitsWithConfig(sk, sb3CollectFragmentEvidence(sk, cfg), noHardCfg)
	for _, u := range noHardUnits {
		if (u.parentID == m1 || u.parentID == m2) && len(u.requiredFanout) != 0 {
			t.Fatalf("hard-HA ablation left required fanouts on unit %x: %v", u.parentID, u.requiredFanout)
		}
	}
}

func TestSB3EmissionUsesAcceptedParentMap(t *testing.T) {
	const (
		root     = uint64(0x01)
		parent   = uint64(0x10)
		accepted = uint64(0x20)
		rejected = uint64(0x21)
	)
	sk := &cgpSkeleton{byID: map[uint64]*Span{root: {SpanID: root, Depth: 0}}}
	u := &sb3RouteUnit{
		parentID: parent, depth: 2, anchor: sk.byID[root], anchors: []*Span{sk.byID[root]},
		nodeChoice: map[int]uint64{1: rejected}, anonAtDepth: map[int]uint64{1: accepted},
	}
	committed := map[uint64]uint64{parent: accepted, accepted: root}
	topo := sb3EmitGreedyTopology(sk, []*sb3RouteUnit{u}, committed)
	if got := topo.ReconParent[parent]; got != accepted {
		t.Fatalf("emitted parent=%x, want committed %x (stale unit choice was %x)", got, accepted, rejected)
	}
	if got := topo.ReconParent[accepted]; got != root {
		t.Fatalf("emitted accepted route parent=%x, want root %x", got, root)
	}
	if !topo.ReconAnon[accepted] {
		t.Fatal("accepted anonymous route node was not marked anonymous")
	}
}

func TestCGP0ReportsSelectedAnchorChainEvidence(t *testing.T) {
	const (
		root = uint64(0x01)
		a    = uint64(0x02)
		b    = uint64(0x03)
		m    = uint64(0x04)
		c    = uint64(0x05)
	)
	cfg := NewPCRBConfig(5, 8, bridge.DefaultBloomFPRate)
	survivors := []Span{
		{SpanID: root, Depth: 0, BloomBits: sb3TestBloom(t, cfg), CkptPrefix: sb3TestPrefix(root, 8)},
		{SpanID: a, ParentID: root, Depth: 1},
		{SpanID: b, ParentID: a, Depth: 2},
		{
			SpanID: c, ParentID: m, Depth: 4, LeafCarrier: true,
			BloomBits: sb3TestBloom(t, cfg, a, b, m), CkptPrefix: sb3TestPrefix(root, 8),
		},
	}

	res := ReconstructCGP0(survivors, cfg)
	if res.GreedyChain.CandidateInitialHits != 2 || res.GreedyChain.CandidateAccepted != 2 || res.GreedyChain.CandidateRejected != 0 {
		t.Fatalf("candidate chain telemetry=%+v, want two accepted initial hits", res.GreedyChain)
	}
	if len(res.GreedyChain.Routes) != 1 {
		t.Fatalf("routes=%d, want 1", len(res.GreedyChain.Routes))
	}
	route := res.GreedyChain.Routes[0]
	if !route.Routed || route.ParentID != m || route.AnchorID != b {
		t.Fatalf("selected route=%+v, want M routed to B", route)
	}
	if route.MatchedLevels != 2 || route.PositiveBloomChecks != 2 || route.SupportingCarriers != 1 {
		t.Fatalf("selected route chain=%+v, want two levels/two checks/one carrier", route)
	}
	if route.CheckpointDepth != 0 || route.AnchorDepth != 2 {
		t.Fatalf("selected route depths=%+v, want checkpoint=0 anchor=2", route)
	}
}

// Exact HA ancestry outranks sparse-chain admissibility. This intentionally
// malformed chain rejects every ordinal-guided anchor path, but the emitted
// topology must still route the carrier through the explicitly witnessed F.
func TestSB3HardHAFallbackWhenAllOrdinalPathsReject(t *testing.T) {
	const (
		root  = uint64(0x01)
		wrong = uint64(0x10)
		right = uint64(0x20)
		f     = uint64(0x30)
		m     = uint64(0x40)
		c     = uint64(0x50)
		wl    = uint64(0x11)
		rl    = uint64(0x21)
	)
	cfg := NewPCRBConfig(4, 8, bridge.DefaultBloomFPRate)
	bits := sb3TestBloom(t, cfg, root, wrong, right, f, m)
	for i := range bits {
		bits[i] = 0xff // keep both surviving anchors Bloom-admissible
	}
	survivors := []Span{
		{SpanID: root, Depth: 0, BloomBits: sb3TestBloom(t, cfg), CkptPrefix: make([]byte, 8)},
		{SpanID: wrong, ParentID: root, Depth: 1},
		{SpanID: right, ParentID: root, Depth: 1},
		{SpanID: wl, ParentID: wrong, Depth: 2, BloomBits: sb3TestBloom(t, cfg, root, wrong), CkptPrefix: sb3TestPrefix(root, 8), LeafCarrier: true},
		{SpanID: rl, ParentID: right, Depth: 2, BloomBits: sb3TestBloom(t, cfg, root, right), CkptPrefix: sb3TestPrefix(root, 8), LeafCarrier: true, SparseOrdinals: []bridge.SB3Branch{{Ord: 2}}},
		{
			SpanID: c, ParentID: m, Depth: 4,
			BloomBits: bits, CkptPrefix: sb3TestPrefix(root, 8),
			HA:             []HAEntry{{ParentID: f, Depth: 3}},
			SparseOrdinals: []bridge.SB3Branch{{Ord: 1}}, // impossible: sparse ordinals start at 2
		},
	}
	res := ReconstructSB3(survivors, cfg)
	if res.HardConflicts != 0 {
		t.Fatalf("exact HA evidence was sacrificed: hard conflicts=%d", res.HardConflicts)
	}
	if !sb3HasAncestor(res.Topology.ReconParent, c, f) {
		t.Fatalf("carrier %x does not route through witnessed fanout %x", c, f)
	}
	if res.Compatible || res.Conflicts == 0 {
		t.Fatalf("malformed ordinal chain was not surfaced: compatible=%v conflicts=%d", res.Compatible, res.Conflicts)
	}
	if res.HardOverrides == 0 {
		t.Fatal("hard-priority fallback was not reported")
	}
}

func TestSB3HardHACompatibilityDistinguishesWrongFromUnresolvedPath(t *testing.T) {
	const (
		root    = uint64(0x01)
		fanout  = uint64(0x10)
		wrong   = uint64(0x11)
		shared  = uint64(0x20)
		parent  = uint64(0x30)
		carrier = uint64(0x40)
	)
	u := &sb3RouteUnit{members: []*sb3FragmentEvidence{{
		haWitnesses: []sb3HAWitness{{fanoutID: fanout, depth: 1, carrier: carrier}},
	}}}
	depth := map[uint64]int{
		root: 0, fanout: 1, wrong: 1, shared: 2, parent: 3, carrier: 4,
	}
	base := map[uint64]uint64{carrier: parent, parent: shared}

	// The partial route has not selected shared's parent yet. The fanout may
	// still be installed at depth 1, so absence is unresolved rather than false.
	if !sb3UnitHardCompatible(u, base, depth) {
		t.Fatal("unresolved upstream path was rejected as an HA contradiction")
	}

	wrongPath := map[uint64]uint64{carrier: parent, parent: shared, shared: wrong, wrong: root}
	if sb3UnitHardCompatible(u, wrongPath, depth) {
		t.Fatal("route crossing the witness depth through a different ID was accepted")
	}

	rightPath := map[uint64]uint64{carrier: parent, parent: shared, shared: fanout, fanout: root}
	if !sb3UnitHardCompatible(u, rightPath, depth) {
		t.Fatal("route through the exact witnessed fanout was rejected")
	}
}

func TestSB3HATrackerRejectsFutureContradictionTransactionally(t *testing.T) {
	const (
		root    = uint64(0x01)
		fanout  = uint64(0x10)
		wrong   = uint64(0x11)
		shared  = uint64(0x20)
		parent  = uint64(0x30)
		carrier = uint64(0x40)
	)
	depth := map[uint64]int{
		root: 0, fanout: 1, wrong: 1, shared: 2, parent: 3, carrier: 4,
	}
	parents := map[uint64]uint64{carrier: parent}
	tracker, initial := sb3BuildHATracker([]Span{{
		SpanID: carrier,
		Depth:  4,
		HA:     []HAEntry{{ParentID: fanout, Depth: 2}},
	}}, parents, depth)
	if initial != 0 || tracker.pending() != 1 {
		t.Fatalf("initial HA state: conflicts=%d pending=%d", initial, tracker.pending())
	}

	parents[parent] = shared
	if _, ok := tracker.tryEdges([]uint64{parent}, parents); !ok {
		t.Fatal("unresolved intermediate route was rejected")
	}
	if tracker.pending() != 1 {
		t.Fatalf("pending after intermediate route=%d, want 1", tracker.pending())
	}

	parents[shared] = wrong
	if _, ok := tracker.tryEdges([]uint64{shared}, parents); ok {
		t.Fatal("future route crossing the witness depth through the wrong node was accepted")
	}
	delete(parents, shared)
	if tracker.pending() != 1 {
		t.Fatalf("rejected trial did not restore pending HA state: %d", tracker.pending())
	}

	parents[shared] = fanout
	if _, ok := tracker.tryEdges([]uint64{shared}, parents); !ok {
		t.Fatal("future route through the exact fanout was rejected")
	}
	if tracker.pending() != 0 {
		t.Fatalf("satisfied HA obligation remains pending: %d", tracker.pending())
	}
}

func TestSB3HATrackerRejectsDistinctSameDepthFanoutsOnSharedPendingPath(t *testing.T) {
	const (
		fanoutA  = uint64(0x10)
		fanoutB  = uint64(0x11)
		terminal = uint64(0x20)
		parentA  = uint64(0x30)
		parentB  = uint64(0x31)
		carrierA = uint64(0x40)
		carrierB = uint64(0x41)
	)
	depth := map[uint64]int{
		fanoutA: 1, fanoutB: 1, terminal: 2,
		parentA: 3, parentB: 3, carrierA: 4, carrierB: 4,
	}
	parents := map[uint64]uint64{carrierA: parentA, carrierB: parentB}
	tracker, initial := sb3BuildHATracker([]Span{
		{SpanID: carrierA, Depth: 4, HA: []HAEntry{{ParentID: fanoutA, Depth: 2}}},
		{SpanID: carrierB, Depth: 4, HA: []HAEntry{{ParentID: fanoutB, Depth: 2}}},
	}, parents, depth)
	if initial != 0 || tracker.pending() != 2 {
		t.Fatalf("initial HA state: conflicts=%d pending=%d", initial, tracker.pending())
	}

	parents[parentA] = terminal
	if _, ok := tracker.tryEdges([]uint64{parentA}, parents); !ok {
		t.Fatal("first obligation could not enter an unresolved shared path")
	}

	// Sending the second obligation into the same terminal would require two
	// differently named fanouts at depth 1 on one ancestry path. Reject it at
	// candidate time and restore its original pending terminal.
	parents[parentB] = terminal
	if _, ok := tracker.tryEdges([]uint64{parentB}, parents); ok {
		t.Fatal("distinct same-depth fanouts were allowed onto one pending path")
	}
	delete(parents, parentB)

	waitingA, waitingB := uint64(0), uint64(0)
	for _, c := range tracker.all {
		switch c.carrier {
		case carrierA:
			waitingA = c.terminal
		case carrierB:
			waitingB = c.terminal
		}
	}
	if waitingA != terminal || waitingB != parentB {
		t.Fatalf("transaction rollback terminals: A=%x B=%x, want %x/%x", waitingA, waitingB, terminal, parentB)
	}
}

func TestSB3HATrackerDerivesOrderedFanoutAncestry(t *testing.T) {
	const (
		root           = uint64(0x01)
		shallowFanout  = uint64(0x10)
		wrongShallow   = uint64(0x11)
		deepFanout     = uint64(0x20)
		sharedTerminal = uint64(0x30)
		parentA        = uint64(0x40)
		parentB        = uint64(0x41)
		carrierA       = uint64(0x50)
		carrierB       = uint64(0x51)
	)
	depth := map[uint64]int{
		root: 0, shallowFanout: 1, wrongShallow: 1, deepFanout: 2,
		sharedTerminal: 3, parentA: 4, parentB: 4, carrierA: 5, carrierB: 5,
	}
	parents := map[uint64]uint64{carrierA: parentA, carrierB: parentB}
	tracker, initial := sb3BuildHATracker([]Span{
		{SpanID: carrierA, Depth: 5, HA: []HAEntry{{ParentID: deepFanout, Depth: 3}}},
		{SpanID: carrierB, Depth: 5, HA: []HAEntry{{ParentID: shallowFanout, Depth: 2}}},
	}, parents, depth)
	if initial != 0 {
		t.Fatalf("initial HA conflicts=%d", initial)
	}

	parents[parentA] = sharedTerminal
	if _, ok := tracker.tryEdges([]uint64{parentA}, parents); !ok {
		t.Fatal("first obligation could not enter the shared terminal")
	}
	parents[parentB] = sharedTerminal
	if _, ok := tracker.tryEdges([]uint64{parentB}, parents); !ok {
		t.Fatal("ordered different-depth obligations were rejected at convergence")
	}

	derived := tracker.derived[[2]uint64{deepFanout, shallowFanout}]
	if derived == nil || !derived.active || derived.terminal != deepFanout {
		t.Fatalf("derived deep-to-shallow obligation=%+v", derived)
	}
	parents[deepFanout] = wrongShallow
	if _, ok := tracker.tryEdges([]uint64{deepFanout}, parents); ok {
		t.Fatal("upstream route that violated derived fanout ancestry was accepted")
	}
	delete(parents, deepFanout)
	parents[deepFanout] = shallowFanout
	if _, ok := tracker.tryEdges([]uint64{deepFanout}, parents); !ok {
		t.Fatal("upstream route through the derived shallow fanout was rejected")
	}
	if !derived.satisfied {
		t.Fatal("derived fanout ancestry remains unsatisfied")
	}
}

func TestSB3GreedyRouteFallbackIsAnExplicitAblation(t *testing.T) {
	const (
		root    = uint64(0x01)
		wrong   = uint64(0x11)
		fanout  = uint64(0x12)
		parent  = uint64(0x30)
		carrier = uint64(0x40)
		gap     = uint64(0x99)
	)
	rootSpan := &Span{SpanID: root, Depth: 0}
	wrongSpan := &Span{SpanID: wrong, ParentID: root, Depth: 1}
	sk := &cgpSkeleton{
		byID: map[uint64]*Span{root: rootSpan, wrong: wrongSpan},
		fanouts: map[uint64]*cgpFanout{
			fanout: {id: fanout, depth: 1},
		},
	}
	newUnit := func() *sb3RouteUnit {
		return &sb3RouteUnit{
			parentID: parent, depth: 3, anchors: []*Span{wrongSpan, rootSpan}, anchor: wrongSpan,
			fanoutsByDepth: map[int][]uint64{1: {fanout}},
			requiredFanout: map[int]uint64{1: fanout},
			nodeChoice:     map[int]uint64{1: fanout},
			anonAtDepth:    map[int]uint64{2: gap},
		}
	}
	newTracker := func(parents map[uint64]uint64) *sb3HATracker {
		tracker, initial := sb3BuildHATracker([]Span{{
			SpanID: carrier, Depth: 4, HA: []HAEntry{{ParentID: fanout, Depth: 2}},
		}}, parents, map[uint64]int{
			root: 0, wrong: 1, fanout: 1, gap: 2, parent: 3, carrier: 4,
		})
		if initial != 0 {
			t.Fatalf("initial HA conflicts=%d", initial)
		}
		return tracker
	}

	parents := map[uint64]uint64{carrier: parent, wrong: root, fanout: root}
	u := newUnit()
	var stats sb3GreedyStats
	if !sb3SelectGreedyRoute(Config{CPD: 4, SB3IgnoreOrdinals: true}, sk, u, nil, newTracker(parents), parents, &stats) {
		t.Fatal("full greedy engine did not fall back from the hard-invalid deepest anchor")
	}
	if u.anchor == nil || u.anchor.SpanID != root || stats.HardOverrides != 1 {
		t.Fatalf("fallback result anchor=%v hard_overrides=%d", u.anchor, stats.HardOverrides)
	}

	parents = map[uint64]uint64{carrier: parent, wrong: root, fanout: root}
	u = newUnit()
	stats = sb3GreedyStats{}
	cfg := Config{CPD: 4, SB3IgnoreOrdinals: true, GreedyNoRouteFallback: true}
	if sb3SelectGreedyRoute(cfg, sk, u, nil, newTracker(parents), parents, &stats) {
		t.Fatal("route-fallback ablation tried a second anchor")
	}
	if u.anchor != nil {
		t.Fatalf("rejected first route left anchor=%x", u.anchor.SpanID)
	}
}

func TestSB3PendingHARevisitsUpstreamRouteAsHardRequirement(t *testing.T) {
	const (
		root     = uint64(0x01)
		fanout   = uint64(0x10)
		terminal = uint64(0x20)
		carrier  = uint64(0x30)
	)
	rootSpan := &Span{SpanID: root, Depth: 0}
	sk := &cgpSkeleton{
		byID:    map[uint64]*Span{root: rootSpan},
		fanouts: map[uint64]*cgpFanout{fanout: {id: fanout, depth: 1}},
	}
	u := &sb3RouteUnit{
		parentID:       terminal,
		depth:          2,
		anchors:        []*Span{rootSpan},
		fanoutsByDepth: make(map[int][]uint64),
		requiredFanout: make(map[int]uint64),
		nodeChoice:     make(map[int]uint64),
		anonAtDepth:    map[int]uint64{1: 0x99},
	}
	parents := map[uint64]uint64{carrier: terminal, fanout: root}
	depth := map[uint64]int{root: 0, fanout: 1, terminal: 2, carrier: 3, 0x99: 1}
	tracker, initial := sb3BuildHATracker([]Span{{
		SpanID: carrier,
		Depth:  3,
		HA:     []HAEntry{{ParentID: fanout, Depth: 2}},
	}}, parents, depth)
	if initial != 0 || tracker.pending() != 1 {
		t.Fatalf("initial HA state: conflicts=%d pending=%d", initial, tracker.pending())
	}
	cfg := Config{CPD: 4, SB3IgnoreOrdinals: true}
	var stats sb3GreedyStats
	sb3ResolvePendingHA(cfg, sk, []*sb3RouteUnit{u}, nil, tracker, parents, &stats)
	if got := parents[terminal]; got != fanout {
		t.Fatalf("upstream terminal parent=%x, want witnessed fanout %x", got, fanout)
	}
	if tracker.pending() != 0 {
		t.Fatalf("hard-routed HA obligation remains pending: %d", tracker.pending())
	}
}
