package recon

import (
	"reflect"
	"testing"

	"bridges/bridge"
)

func TestApplyStructureEvidenceLearnsUniqueAnonymousOwnerFingerprint(t *testing.T) {
	root := newSBNode(0)
	root.RealID = 0x1000_0000_0000_0001
	anon := newSBNode(1)
	root.Children[1] = anon
	anon.Children[1] = newSBNode(1)
	anon.Children[2] = newSBNode(2)
	anon.Children[3] = newSBNode(3)
	anon.Children[2].EE = []int{1}
	res := SBResult{Root: root, FPBits: 16}

	st := ApplyStructureEvidence(&res, []bridge.DEEQuad{{Depth: 1, OwnerFP: 0xabcd, Seqs: []int{3}}})
	if !st.Complete || st.DEEPlaced != 1 {
		t.Fatalf("status=%+v", st)
	}
	if anon.FP != 0xabcd || anon.FPBits != 16 {
		t.Fatalf("anonymous owner fingerprint=(%x,%d), want (abcd,16)", anon.FP, anon.FPBits)
	}
	if !reflect.DeepEqual(anon.EndOrder, []int{1, 3, 2}) {
		t.Fatalf("anonymous owner end order=%v", anon.EndOrder)
	}
}

func TestApplyStructureEvidenceRejectsAmbiguousAnonymousOwner(t *testing.T) {
	root := newSBNode(0)
	root.RealID = 1
	for ord := 1; ord <= 2; ord++ {
		p := newSBNode(ord)
		root.Children[ord] = p
		p.Children[1] = newSBNode(1)
		p.Children[2] = newSBNode(2)
		p.Children[3] = newSBNode(3)
		p.Children[2].EE = []int{1}
	}
	// Complete the root's own ordering so only the depth-1 owner is at issue.
	root.Children[2].EE = []int{1}
	res := SBResult{Root: root, FPBits: 16}
	st := ApplyStructureEvidence(&res, []bridge.DEEQuad{{Depth: 1, OwnerFP: 0xbeef, Seqs: []int{3}}})
	if st.Complete || st.DEEAmbiguous != 1 {
		t.Fatalf("status=%+v, want one ambiguity and no guessed order", st)
	}
	for ord := 1; ord <= 2; ord++ {
		if got := root.Children[ord].FPBits; got != 0 {
			t.Fatalf("ambiguous candidate %d learned a fingerprint", ord)
		}
	}
}

func TestApplyStructureEvidencePropagatesAnonymousOwnerConstraints(t *testing.T) {
	root := newSBNode(0)
	root.RealID = 1
	a, b := newSBNode(1), newSBNode(2)
	root.Children[1], root.Children[2] = a, b
	b.EE = []int{1} // complete the root's own end evidence
	for _, p := range []*SBNode{a, b} {
		p.Children[1] = newSBNode(1)
		p.Children[2] = newSBNode(2)
		p.Children[3] = newSBNode(3)
	}
	a.Children[2].EE = []int{1}
	b.Children[3].EE = []int{2}
	res := SBResult{Root: root, FPBits: 16}

	// The first record initially fits both anonymous parents. The second fits
	// only B (A already witnessed end 1), fixing B's fingerprint; a second pass
	// can then uniquely place the first record on A.
	st := ApplyStructureEvidence(&res, []bridge.DEEQuad{
		{Depth: 1, OwnerFP: 0xaaaa, Seqs: []int{3}},
		{Depth: 1, OwnerFP: 0xbbbb, Seqs: []int{1}},
	})
	if !st.Complete || st.DEEPlaced != 2 || st.DEEAmbiguous != 0 {
		t.Fatalf("status=%+v", st)
	}
	if a.FP != 0xaaaa || b.FP != 0xbbbb {
		t.Fatalf("learned fingerprints A=%x B=%x", a.FP, b.FP)
	}
	if !reflect.DeepEqual(a.EndOrder, []int{1, 3, 2}) || !reflect.DeepEqual(b.EndOrder, []int{2, 1, 3}) {
		t.Fatalf("end orders A=%v B=%v", a.EndOrder, b.EndOrder)
	}
}
