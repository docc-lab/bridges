package recon

import (
	"testing"

	"bridges/bloom"
	"bridges/bridge"
)

// Each constraint accepts the unfinished join in isolation: the filter accepts
// its materialized ancestors and the HA path stops before the witness depth.
// Together they rule it out, because the filter rejects the mandatory fanout.
// Check both arrival orders and the actual greedy fallback/rollback behavior.
func TestGreedyPrunesPendingHAAcrossFilterJoins(t *testing.T) {
	for _, filterMoves := range []bool{false, true} {
		name := "witness_joins_filter"
		if filterMoves {
			name = "filter_joins_witness"
		}
		t.Run(name, func(t *testing.T) {
			const root, fanout, terminal = uint64(1), uint64(0x10), uint64(0x20)
			const wrong, missing, carrier, other = uint64(0x30), uint64(0x40), uint64(0x50), uint64(0x60)
			rootSpan := &Span{SpanID: root, Depth: 0}
			fanoutSpan := &Span{SpanID: fanout, Depth: 1}
			wrongSpan := &Span{SpanID: wrong, Depth: 3}
			parents := map[uint64]uint64{carrier: missing, other: wrong, wrong: terminal, fanout: root}
			depths := map[uint64]int{
				root: 0, fanout: 1, terminal: 2, wrong: 3, missing: 4, carrier: 5, other: 4,
				0xa1: 1, 0xa2: 2, 0xa3: 3,
			}
			witnessCarrier, filterCarrier, goodAnchor := carrier, other, fanoutSpan
			if filterMoves {
				witnessCarrier, filterCarrier, goodAnchor = other, carrier, rootSpan
			}
			ha, initial := sb3BuildHATracker([]Span{{
				SpanID: witnessCarrier, Depth: depths[witnessCarrier],
				HA: []HAEntry{{ParentID: fanout, Depth: 2}},
			}}, parents, depths)
			if initial != 0 || ha.pending() != 1 {
				t.Fatalf("initial HA conflicts=%d pending=%d", initial, ha.pending())
			}
			bf := bloom.NewWithEstimates(8, 1e-6)
			for _, id := range []uint64{missing, wrong, terminal} {
				key := bridge.HexOf(id)
				bf.Add(key[:])
			}
			key := bridge.HexOf(fanout)
			if bf.Test(key[:]) {
				t.Fatal("fixture must reject the mandatory fanout")
			}
			amq := &greedyAMQTracker{
				named:   map[uint64]bool{root: true, fanout: true, terminal: true, wrong: true, missing: true, carrier: true, other: true},
				waiting: make(map[uint64]map[*greedyAMQConstraint]bool),
			}
			filter := &greedyAMQConstraint{
				carrier: filterCarrier, carrierDepth: depths[filterCarrier], floor: 0, bf: bf,
				start: filterCarrier, startDepth: depths[filterCarrier],
				state: greedyAMQState{terminal: filterCarrier, depth: depths[filterCarrier]},
			}
			amq.all = append(amq.all, filter)
			if !amq.advance(filter, parents) {
				t.Fatal("fixture's literal ancestry contradicts its filter")
			}
			amq.addWaiting(filter)
			sk := &cgpSkeleton{
				byID: map[uint64]*Span{root: rootSpan, fanout: fanoutSpan, wrong: wrongSpan}, amq: amq,
			}
			u := &sb3RouteUnit{
				parentID: missing, depth: 4, anchors: []*Span{wrongSpan, goodAnchor}, anchor: wrongSpan,
				fanoutsByDepth: make(map[int][]uint64), requiredFanout: make(map[int]uint64),
				nodeChoice: make(map[int]uint64), anonAtDepth: map[int]uint64{1: 0xa1, 2: 0xa2, 3: 0xa3},
			}
			cfg := Config{CPD: 8, SB3IgnoreOrdinals: true}
			var stats sb3GreedyStats
			if !sb3SelectGreedyRoute(cfg, sk, u, nil, ha, parents, &stats) {
				t.Fatal("no route selected despite a compatible alternative")
			}
			if u.anchor != goodAnchor || stats.AMQPrunes != 1 || parents[missing] == wrong {
				t.Fatalf("contradictory join survived: anchor=%v AMQ prunes=%d parent=%x", u.anchor, stats.AMQPrunes, parents[missing])
			}
			if filterMoves {
				upstream := &sb3RouteUnit{
					parentID: terminal, depth: 2, anchors: []*Span{fanoutSpan},
					requiredFanout: make(map[int]uint64), nodeChoice: make(map[int]uint64),
				}
				if !sb3SelectGreedyRoute(cfg, sk, upstream, nil, ha, parents, &stats) {
					t.Fatal("rejected join left stale filter constraints on the witness path")
				}
			} else if filter.state.terminal != terminal || !amq.waiting[terminal][filter] {
				t.Fatal("rejected join changed the preexisting filter")
			}
			if ha.pending() != 0 || !sb3HasAncestor(parents, witnessCarrier, fanout) || amq.conflicts(parents) != 0 {
				t.Fatalf("fallback violated evidence: pending HA=%d AMQ conflicts=%d", ha.pending(), amq.conflicts(parents))
			}
			if parents[carrier] != missing || parents[other] != wrong || parents[wrong] != terminal {
				t.Fatal("fallback changed literal parent evidence")
			}
		})
	}
}

func TestPendingHAAMQRespectsFilterDepthBounds(t *testing.T) {
	for _, tc := range []struct {
		name  string
		depth int
		want  bool
	}{
		{"above_checkpoint", 1, true},
		{"checkpoint_root", 2, true},
		{"inside_window", 3, false},
		{"carrier", 4, true},
		{"below_carrier", 5, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filter := &greedyAMQConstraint{floor: 2, carrierDepth: 4, bf: bloom.NewWithEstimates(4, 1e-6)}
			filter.state.terminal = 99
			witness := &sb3HAConstraint{fanout: 10, depth: tc.depth, terminal: 99, active: true}
			amq := &greedyAMQTracker{waiting: map[uint64]map[*greedyAMQConstraint]bool{99: {filter: true}}}
			ha := &sb3HATracker{waiting: map[uint64]map[*sb3HAConstraint]bool{99: {witness: true}}}
			tx := &greedyAMQTxn{originals: map[*greedyAMQConstraint]greedyAMQState{filter: {terminal: 100}}}
			if got := greedyPendingHAAMQCompatible(amq, ha, tx, nil); got != tc.want {
				t.Fatalf("witness depth %d: compatible=%v, want %v", tc.depth, got, tc.want)
			}
		})
	}
}
