package bridge

import (
	"bytes"
	"reflect"
	"testing"
)

func TestCheckpointSnapshotPreservesForwardState(t *testing.T) {
	for mode, makeHandler := range checkpointTestHandlers() {
		for _, randomized := range []bool{false, true} {
			name := mode + "/fixed"
			if randomized {
				name = mode + "/random"
			}
			t.Run(name, func(t *testing.T) {
				baseline, sampled := makeHandler(4), makeHandler(4)
				if randomized {
					for _, h := range []Handler{baseline, sampled} {
						if err := ConfigureCheckpoints(h, &CheckpointRange{Min: 4, Max: 4, Seed: 42}); err != nil {
							t.Fatal(err)
						}
					}
				}
				snapshot := sampled.(interface{ CheckpointPayload(*Event) []byte })
				root := Event{TraceID: 5, SpanID: 1}
				parent := Event{TraceID: 5, SpanID: 2, ParentID: 1}
				start := func(ev *Event, seq int) {
					t.Helper()
					a, b := baseline.OnStart(ev, seq), sampled.OnStart(ev, seq)
					if !reflect.DeepEqual(a, b) {
						t.Fatalf("snapshot changed forward start at %d: %+v != %+v", ev.SpanID, a, b)
					}
				}
				end := func(ev *Event) EndResult {
					t.Helper()
					a, b := baseline.OnEnd(ev), sampled.OnEnd(ev)
					if !reflect.DeepEqual(a, b) {
						t.Fatalf("snapshot changed end emission at %d", ev.SpanID)
					}
					return b
				}
				start(&root, 0)
				if snapshot.CheckpointPayload(&root) != nil {
					t.Fatal("original checkpoint snapshot must use retained StartResult payload")
				}
				start(&parent, 1)
				own := snapshot.CheckpointPayload(&parent)
				if len(own) == 0 || (randomized && !IsLeafPayload(own)) {
					t.Fatal("promoted checkpoint snapshot lacks partial-window ancestry")
				}
				copyBefore := append([]byte(nil), own...)
				for i := range own {
					own[i] ^= 0xff
				}
				if !bytes.Equal(copyBefore, snapshot.CheckpointPayload(&parent)) {
					t.Fatal("caller mutation altered handler evidence")
				}
				// Sequential siblings exercise SB3's accumulated end-event evidence
				// and the second-child CGP0/SB3 fanout witness.
				for i := 0; i < 2; i++ {
					child := Event{TraceID: 5, SpanID: uint64(i + 3), ParentID: 2}
					start(&child, i+1)
					leafPayload := snapshot.CheckpointPayload(&child)
					if got := end(&child); !bytes.Equal(got.Payload, leafPayload) {
						t.Fatal("leaf snapshot differs from native emitted bytes")
					}
				}
				end(&parent)
				end(&root)
			})
		}
	}
}
