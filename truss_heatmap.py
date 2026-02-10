import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# -----------------------------
# Worst-case sibling constructor
# -----------------------------
def build_worst_case_ordinals(S: int, W: int):
    """
    Create N = S*W children arranged into S sequential waves of width W.

    Ordinal template:
        ord(start of X_{i,j}) = (i-1)*2W + j
        ord(end   of X_{i,j}) = (i-1)*2W + W + j

    Returns a list of dicts:
        id, wave, j, ord_s, ord_e
    """
    children = []
    for i in range(1, S + 1):
        for j in range(1, W + 1):
            ord_s = (i - 1) * 2 * W + j
            ord_e = (i - 1) * 2 * W + W + j
            children.append(
                {"id": f"X_{i}_{j}", "wave": i, "j": j, "ord_s": ord_s, "ord_e": ord_e}
            )
    return children

# -------------------------------------------------------
# Truss-size model based on Algorithm 1 (ordinal annotation)
# -------------------------------------------------------
def avg_truss_bytes_simulated(S: int, W: int, spanid_bytes=8, ordinal_bytes=4):
    """
    Implements Algorithm 1: Minimal-State Ordinal Annotation for Sibling Ordering
    
    Parent state:
    - k: monotonic counter (starts at 0)
    - trusses: list of (child_id, ordinal) pairs (starts empty)
    - delayed_trusses: (trace_id, trusses) - propagated to children at start
    
    When Child A starts:
    - k = k + 1
    - SetAttr(child_start_ord, k)
    - Propagate(delayed_trusses, trusses)  # Child receives both
    - trusses = []  # Clear trusses
    
    When Child A ends:
    - k = k + 1
    - trusses.append((A, k))
    
    delayed_trusses = (trace_id, trusses)  # Updated after each end
    
    Worst-case schedule: wave i: all starts, then all ends
    Returns average bytes per child.
    """
    if S <= 0 or W <= 0:
        return 0.0

    children = build_worst_case_ordinals(S, W)
    bytes_per_pair = spanid_bytes + ordinal_bytes

    # Enforce worst-case schedule: all starts of wave i, then all ends of wave i
    events = []
    for i in range(1, S + 1):
        wave_children = [c for c in children if c["wave"] == i]
        for c in sorted(wave_children, key=lambda x: x["ord_s"]):
            events.append(("start", c))
        for c in sorted(wave_children, key=lambda x: x["ord_e"]):
            events.append(("end", c))

    # Parent state (Algorithm 1)
    k = 0  # Monotonic counter
    trusses = []  # List of (child_id, ordinal) pairs
    delayed_trusses = []  # Starts empty, contains (child_id, ordinal) pairs from previous state
    trace_id_bytes = spanid_bytes  # Size of trace_id (assuming same as spanid)
    
    truss_bytes_at_start = []

    for typ, c in events:
        if typ == "start":
            # Child starts
            k = k + 1
            # SetAttr(child_start_ord, k) - not needed for size calculation
            
            # Propagate(delayed_trusses, trusses)
            # Child receives both delayed_trusses and current trusses
            # delayed_trusses has a trace_id associated with it, so add trace_id size
            delayed_size = trace_id_bytes if delayed_trusses else 0
            delayed_pairs_size = len(delayed_trusses) * bytes_per_pair
            current_pairs_size = len(trusses) * bytes_per_pair
            total_bytes = delayed_size + delayed_pairs_size + current_pairs_size
            truss_bytes_at_start.append(total_bytes)
            
            # Clear trusses
            trusses = []
        else:
            # Child ends
            k = k + 1
            # Add (child_id, ordinal) to trusses
            trusses.append((c["id"], k))
            # Update delayed_trusses = (trace_id, trusses)
            # For next child start, delayed_trusses will contain current trusses
            # (trace_id size will be added when calculating size at next start)
            delayed_trusses = trusses.copy()  # This becomes the delayed_trusses for next start

    return float(np.mean(truss_bytes_at_start))

# -------------------------------------------------------
# Heatmap builder
# -------------------------------------------------------
def build_heatmap_data(S_vals, W_vals, size_fn=avg_truss_bytes_simulated, **size_kwargs):
    data = np.zeros((len(W_vals), len(S_vals)), dtype=float)
    for wi, W in enumerate(W_vals):
        for si, S in enumerate(S_vals):
            data[wi, si] = size_fn(S, W, **size_kwargs)
    return data

# Choose axis ranges
S_vals = [1, 2, 3, 4, 6, 8, 12]   # sequential waves
W_vals = [1, 2, 3, 4, 6, 8, 12]   # concurrency width

data = build_heatmap_data(S_vals, W_vals)

# Optional: tabular view
df = pd.DataFrame(
    data,
    index=[f"W={w}" for w in W_vals],
    columns=[f"S={s}" for s in S_vals],
)
print(df.round(2))

# Plot heatmap (matching style from calculate_ancestry_size.py)
plt.figure(figsize=(12, 8))
ax = sns.heatmap(
    data,
    xticklabels=S_vals,
    yticklabels=W_vals,
    annot=True,
    fmt='.1f',
    cmap='YlOrRd',
    cbar_kws={'label': 'Avg truss bytes per child'}
)

# Reverse y-axis so lower W values are at bottom, higher at top
ax.invert_yaxis()

plt.xlabel('S (sequential waves)', fontsize=12)
plt.ylabel('W (concurrency width)', fontsize=12)
plt.title('Worst-case avg truss bytes per child\nAssuming context = all previously ended siblings', 
          fontsize=14, fontweight='bold')
plt.tight_layout()
plt.savefig('truss_heatmap.pdf', dpi=300, bbox_inches='tight')
print("Heatmap saved to truss_heatmap.pdf")
plt.close()