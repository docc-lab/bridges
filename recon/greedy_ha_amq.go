package recon

import "bridges/bridge"

// A pending HA witness already identifies an ancestor, even before the edges
// reaching it have been selected. Every AMQ sharing that unfinished path must
// accept the witness when its depth lies inside the filter's window. Checking
// only materialized edges can otherwise commit a join that can never satisfy
// its mandatory fanout evidence.
func greedyPendingHAAMQCompatible(amq *greedyAMQTracker, ha *sb3HATracker, amqTxn *greedyAMQTxn, haTxn *sb3HATxn) bool {
	if amq == nil || ha == nil {
		return true
	}
	terminals := make(map[uint64]bool)
	if amqTxn != nil {
		for c := range amqTxn.originals {
			if !c.state.done && c.state.terminal != 0 {
				terminals[c.state.terminal] = true
			}
		}
	}
	if haTxn != nil {
		add := func(c *sb3HAConstraint) {
			if c.active && !c.satisfied && c.terminal != 0 {
				terminals[c.terminal] = true
			}
		}
		for c := range haTxn.originals {
			add(c)
		}
		for _, c := range haTxn.added {
			add(c)
		}
	}
	for terminal := range terminals {
		// Multiple carriers can witness the same fanout. Its membership need
		// only be tested once per filter at this terminal.
		seenFanout := make(map[uint64]bool)
		for witness := range ha.waiting[terminal] {
			if seenFanout[witness.fanout] {
				continue
			}
			seenFanout[witness.fanout] = true
			key := bridge.HexOf(witness.fanout)
			for filter := range amq.waiting[terminal] {
				if witness.depth <= filter.floor || witness.depth >= filter.carrierDepth {
					continue // checkpoint roots and the carrier are not in its AMQ
				}
				if !filter.bf.Test(key[:]) {
					return false
				}
			}
		}
	}
	return true
}
