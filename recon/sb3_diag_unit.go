package recon

// Per-unit routing diagnostics for the shared greedy engine. Enabled only when
// TRACE_RECON_SB3UNIT names one or more unit heads (hex, comma-separated) or
// "all". With the variable unset every hook is a nil-map lookup and a return.

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"bridges/bridge"
)

var sb3DiagUnitSet = func() map[uint64]bool {
	v := os.Getenv("TRACE_RECON_SB3UNIT")
	if v == "" {
		return nil
	}
	m := map[uint64]bool{}
	for _, part := range strings.Split(v, ",") {
		part = strings.TrimSpace(part)
		if part == "all" {
			m[0] = true
			continue
		}
		var id uint64
		fmt.Sscanf(part, "%x", &id)
		m[id] = true
	}
	return m
}()

func sb3DiagWant(u *sb3RouteUnit) bool {
	if sb3DiagUnitSet == nil || u == nil {
		return false
	}
	return sb3DiagUnitSet[0] || sb3DiagUnitSet[u.head()]
}

func sb3DiagSortedDepths(m map[int]uint64) []int {
	ks := make([]int, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(ks)))
	return ks
}

func sb3DiagChoice(u *sb3RouteUnit) string {
	parts := []string{}
	for _, d := range sb3DiagSortedDepths(u.nodeChoice) {
		id := u.nodeChoice[d]
		if id == 0 {
			parts = append(parts, fmt.Sprintf("d%d=anon", d))
		} else {
			parts = append(parts, fmt.Sprintf("d%d=%016x", d, id))
		}
	}
	return strings.Join(parts, ",")
}

func sb3DiagUnitState(sk *cgpSkeleton, u *sb3RouteUnit, label string, parent map[uint64]uint64) {
	if !sb3DiagWant(u) {
		return
	}
	var b strings.Builder
	fmt.Fprintf(&b, "SB3UNIT[%s] head=%016x depth=%d anonParent=%v members=%d", label, u.head(), u.depth, u.anonymousParent, len(u.members))
	if u.anchor != nil {
		fmt.Fprintf(&b, " anchor=%016x@%d", u.anchor.SpanID, u.anchor.Depth)
	} else {
		b.WriteString(" anchor=nil")
	}
	if p, ok := parent[u.head()]; ok {
		fmt.Fprintf(&b, " head_parent=%016x", p)
	} else {
		b.WriteString(" head_parent=NONE")
	}
	b.WriteString("\n  anchors:")
	for _, a := range u.anchors {
		fmt.Fprintf(&b, " %016x@%d", a.SpanID, a.Depth)
	}
	if root := sb3CertainWindowRoot(u); root != nil {
		fmt.Fprintf(&b, "\n  certain_root=%016x@%d", root.SpanID, root.Depth)
	} else {
		b.WriteString("\n  certain_root=nil")
	}
	b.WriteString("\n  requiredFanout:")
	for _, d := range sb3DiagSortedDepths(u.requiredFanout) {
		id := u.requiredFanout[d]
		_, surv := sk.byID[id]
		_, fo := sk.fanouts[id]
		fmt.Fprintf(&b, " d%d=%016x(surv=%v skfanout=%v)", d, id, surv, fo)
	}
	b.WriteString("\n  nodeChoice: " + sb3DiagChoice(u))
	b.WriteString("\n  anonAtDepth:")
	for _, d := range sb3DiagSortedDepths(u.anonAtDepth) {
		fmt.Fprintf(&b, " d%d", d)
	}
	b.WriteString("\n  fanoutsByDepth:")
	{
		ds := make([]int, 0, len(u.fanoutsByDepth))
		for d := range u.fanoutsByDepth {
			ds = append(ds, d)
		}
		sort.Sort(sort.Reverse(sort.IntSlice(ds)))
		for _, d := range ds {
			fmt.Fprintf(&b, " d%d=[", d)
			for _, id := range u.fanoutsByDepth[d] {
				fmt.Fprintf(&b, "%016x ", id)
			}
			b.WriteString("]")
		}
	}
	// Direct membership test of every required fanout against every member Bloom,
	// with the carrier's decoded geometry, so a contradiction is visible here.
	for _, d := range sb3DiagSortedDepths(u.requiredFanout) {
		id := u.requiredFanout[d]
		key := bridge.HexOf(id)
		if f := sk.byID[id]; f != nil {
			fmt.Fprintf(&b, "\n  REQUIRED-SURVIVOR %016x: depth=%d parent=%016x bloom=%v cpd=%d m=%d k=%d leaf=%v prefix=%x ha=%d", id, f.Depth, f.ParentID, f.BloomBits != nil, f.WindowCPD, f.BloomM, f.BloomK, f.LeafCarrier, f.CkptPrefix, len(f.HA))
		}
		for _, e := range u.members {
			if e == nil {
				continue
			}
			for _, wb := range e.blooms {
				c := sk.byID[wb.carrier]
				geo := "?"
				if c != nil {
					geo = fmt.Sprintf("depth=%d cpd=%d m=%d k=%d leaf=%v prefix=%x parent=%016x", c.Depth, c.WindowCPD, c.BloomM, c.BloomK, c.LeafCarrier, c.CkptPrefix, c.ParentID)
				}
				fmt.Fprintf(&b, "\n  BLOOMTEST required d%d=%016x vs carrier %016x -> %v  [%s] popcount=%d", d, id, wb.carrier, wb.bf.Test(key[:]), geo, wb.bf.PopCount())
			}
		}
	}
	for i, e := range u.members {
		if e == nil || e.frag == nil {
			continue
		}
		fmt.Fprintf(&b, "\n  member[%d] root=%016x@%d resolved=%v", i, e.frag.root.SpanID, e.frag.root.Depth, e.resolved)
		if e.ckpt != nil {
			fmt.Fprintf(&b, " ckpt=%016x@%d", e.ckpt.SpanID, e.ckpt.Depth)
		}
		b.WriteString(" checkpoints:")
		for _, c := range e.checkpoints {
			fmt.Fprintf(&b, " %016x@%d", c.SpanID, c.Depth)
		}
		b.WriteString(" blooms:")
		for _, wb := range e.blooms {
			fmt.Fprintf(&b, " %016x@%d", wb.carrier, wb.depth)
		}
		b.WriteString(" ha:")
		for _, w := range e.haWitnesses {
			fmt.Fprintf(&b, " %016x@%d(by %016x)", w.fanoutID, w.depth, w.carrier)
		}
	}
	fmt.Fprintln(os.Stderr, b.String())
}

func sb3DiagCandidate(u *sb3RouteUnit, inserted []uint64, parent map[uint64]uint64, ok bool, why string) {
	if !sb3DiagWant(u) {
		return
	}
	anchor := "nil"
	if u.anchor != nil {
		anchor = fmt.Sprintf("%016x@%d", u.anchor.SpanID, u.anchor.Depth)
	}
	path := make([]string, 0, len(inserted))
	for _, id := range inserted {
		path = append(path, fmt.Sprintf("%016x->%016x", id, parent[id]))
	}
	fmt.Fprintf(os.Stderr, "SB3CAND head=%016x anchor=%s ok=%v why=%s inserted=%d choice={%s} path=[%s]\n",
		u.head(), anchor, ok, why, len(inserted), sb3DiagChoice(u), strings.Join(path, " "))
}

func sb3DiagCertain(u *sb3RouteUnit, root *Span, mode int, why string) {
	if !sb3DiagWant(u) {
		return
	}
	r := "nil"
	if root != nil {
		r = fmt.Sprintf("%016x@%d", root.SpanID, root.Depth)
	}
	fmt.Fprintf(os.Stderr, "SB3CERTAIN head=%016x root=%s mode=%d why=%s\n", u.head(), r, mode, why)
}

func sb3DiagNote(u *sb3RouteUnit, format string, args ...interface{}) {
	if !sb3DiagWant(u) {
		return
	}
	fmt.Fprintf(os.Stderr, "SB3NOTE head=%016x "+format+"\n", append([]interface{}{u.head()}, args...)...)
}
