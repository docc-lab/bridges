package bridge

import (
	"fmt"
	"testing"

	"bridges/bloom"
)

func TestSB3KeepsFreshDEEsOnNonFirstChildren(t *testing.T) {
	for _, geometry := range []struct {
		name   string
		cpd    int
		random bool
	}{
		{"leaf", 4, false},
		{"checkpoint", 2, false},
		{"random2_8", 8, true},
	} {
		for _, lehmer := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/lehmer=%t", geometry.name, lehmer), func(t *testing.T) {
				h := NewSB3Handler(geometry.cpd, 8, DefaultBloomFPRate, nil)
				h.Capture, h.UseDEEQueueID, h.DequeueOneDEE = true, true, true
				h.LehmerEE, h.FPBits = lehmer, 64
				h.DEEStats = NewDEEQueueStats()
				if geometry.random {
					r, err := ParseCheckpointRange("2:8", 42)
					if err != nil {
						t.Fatal(err)
					}
					if err := ConfigureCheckpoints(h, r); err != nil {
						t.Fatal(err)
					}
				}
				const tid, root, parent = uint64(0x100), uint64(0x10), uint64(0x11)
				event := func(sid, pid uint64, queue uint32) *Event {
					// Same service, distinct instances: queue identity matters.
					return &Event{TraceID: tid, SpanID: sid, ParentID: pid, ServiceID: 7, DEEQueueID: queue}
				}
				a := encodeDEEQuad(TraceID16(0xaaa), 1, 0xa1, 64, 3, []int{1, 2}, lehmer)
				b := encodeDEEQuad(TraceID16(0xbbb), 1, 0xb2, 64, 3, []int{1, 2}, lehmer)
				h.enqueueDEE(11, 7, a, 0xaaa)
				h.OnStart(event(root, 0, 0), 0)
				h.OnStart(event(parent, root, 11), 1)

				emitChild := func(sid uint64, queue uint32, seq int) SB3Payload {
					t.Helper()
					e := event(sid, parent, queue)
					s := h.OnStart(e, seq)
					end := h.OnEnd(e)
					payload, counted := end.Payload, end.EmitBytes
					if len(s.Payload) != 0 {
						payload, counted = s.Payload, s.EmitBytes
					}
					if len(payload) == 0 {
						t.Fatal("child did not emit checkpoint/leaf payload")
					}
					if counted != BRPropertyNameOverheadBytes+len(payload) {
						t.Fatalf("payload accounting=%d, serialized attribute=%d", counted, BRPropertyNameOverheadBytes+len(payload))
					}
					bloomLen := h.bloomLen
					if geometry.random {
						distance, err := PayloadDistance(payload[0], 2, 8)
						if err != nil {
							t.Fatal(err)
						}
						m, _ := bloom.EstimateParameters(PCRBBloomCapacity(distance), DefaultBloomFPRate)
						bloomLen = int((m + 7) / 8)
					}
					decoded, err := DecodeSB3Payload(payload, 8, bloomLen, 64, lehmer)
					if err != nil {
						t.Fatal(err)
					}
					return decoded
				}

				first := emitChild(0x21, 0, 1)
				if len(first.DEE) != 1 || first.DEE[0].OwnerFP != 0xa1 {
					t.Fatalf("first child must carry inherited DEE A: %+v", first.DEE)
				}
				h.enqueueDEE(12, 7, b, 0xbbb)
				second := emitChild(0x22, 12, 2)
				if len(second.DEE) != 1 || second.DEE[0].OwnerFP != 0xb2 {
					t.Fatalf("second child must carry fresh DEE B without duplicating inherited A: %+v", second.DEE)
				}
				third := emitChild(0x23, 0, 3)
				if len(third.DEE) != 0 {
					t.Fatalf("third child duplicated earlier DEEs: %+v", third.DEE)
				}
				stats := h.DEEStats.Snapshot()
				if stats.EnqueuedRecords != 2 || stats.DequeuedRecords != 2 || stats.BacklogRecords != 0 {
					t.Fatalf("unexpected DEE queue accounting: %+v", stats)
				}
			})
		}
	}
}
