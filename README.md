# Bridges Project

This repository contains tools for trace reconstruction and analysis, specifically designed to reconstruct traces with data loss using ancestry data stored within span objects. It supports Bloom filter, hash array, and hybrid ancestry modes for intelligent trace reconnection.

## Tools

### 1. Jaeger Trace Loader (`jaeger_trace_loader.go`)

The main tool for loading, analyzing, simulating data loss, and reconstructing traces. It supports loading traces from either:
- Jaeger backend (REST API)
- Local JSON files (folder input)

It includes advanced reconnection algorithms using both Bloom filter and hash array ancestry modes, with detailed metadata tracking for trace size comparisons.

### 2. Trace Tagger (`trace_tagger.go`)

A utility tool for adding priority and ancestry tags to traces. It processes traces from JSON files and adds:
- Priority tags (`prio`: "high" or "low")
- Ancestry mode tags (`ancestry_mode`: "bloom", "hash", or "hybrid")
- Ancestry data tags (`ancestry`: serialized Bloom filter or comma-separated span ID array)

### 3. Trace Generator (`generate_trace.py`)

A Python script for generating synthetic Jaeger/OpenTelemetry traces with configurable depth and width. Creates complete w-ary tree structures for testing trace reconstruction algorithms.

### 4. Plotting Tools

- **`plot_metrics.py`**: Comprehensive plotting tool that loads metadata from reconstructed traces and creates visualizations for ancestry data size, reconnection rates, span differences, and total span sizes across different checkpoint distances and ancestry modes.

- **`plot_simple_stats.py`**: Simple plotting tool for call depth per trace and number of spans per trace using reconstructed metadata.

- **`truss_heatmap.py`**: Generates heatmaps for truss-size model analysis based on ordinal annotation algorithms.

### 5. Ancestry Size Calculator (`calculate_ancestry_size.py`)

Calculates average ancestry data size for synthetic traces in hybrid mode, mirroring the logic of `trace_tagger.go` to accurately calculate ancestry data sizes.

### 6. Batch Processing Scripts

- **`generate_all_traces.sh`**: Generates synthetic traces for all combinations of depth (2-10) and width (1-20) with 20 traces per configuration.

- **`run_bridges_on_all.sh`**: Batch processes all tagged directories in `data/` by running the bridges tool on each directory matching the `tagged-*-*` pattern.

## Features

#### Core Functionality
- **Trace Loading**: Fetches traces from Jaeger backend via REST API
- **Service Discovery**: Automatically discovers available services
- **Bloom Filter Optimization**: Efficient trace ID lookups using bloom filters
- **Trace Caching**: Reduces redundant API calls with intelligent caching
- **Deduplication**: Removes duplicate traces across services

#### Data Loss Detection
- **Parent-Child Relationship Analysis**: Detects missing parent span references
- **CHILD_OF Reference Validation**: Checks References array for proper parent relationships
- **Orphan Span Identification**: Finds spans that reference non-existent parents
- **Severity Classification**: Categorizes data loss as low/medium/high based on affected span percentage
- **Comprehensive Reporting**: Detailed analysis of missing span IDs and orphaned spans

#### Data Loss Simulation
- **Priority-Based Removal**: Only removes low-priority spans, preserving high-priority spans
- **Protection Rules**: Never removes root spans or leaf spans
- **High-Priority Spans**: Root spans, leaf spans, and spans at configurable depth intervals are protected
- **Configurable Loss Percentages**: Test different levels of data loss (default: 100% of low-priority spans)
- **Randomized Simulation**: Each run produces different data loss patterns
- **Automatic Detection**: Simulated traces are automatically analyzed for data loss

#### Trace Reconnection
- **Multiple Ancestry Modes**: Supports Bloom filter, hash array, and hybrid ancestry data
- **Ancestry Data Sources**: Reads from `ancestry`/`ancestry_mode` tags, with fallback to `__bag.*` baggage
- **Hash Array Mode**: Uses comma-separated span ID arrays to reconstruct parent chains
- **Bloom Filter Mode**: Uses probabilistic membership testing to find ancestors
- **Hybrid Mode**: Combines Bloom filter for ancestry with hash arrays for sibling ordering at fan-out points
- **Synthetic Span Creation**: Creates missing intermediate spans with "unknown" service/operation
- **Smart Parent Detection**: Handles cases where parents are already synthesized by other orphans
- **Ancestry Chain Inference**: Derives missing parents from ancestry data when not in references
- **Bridge Edge Creation**: Connects orphaned spans to their most likely ancestors
- **Confidence Scoring**: Provides confidence metrics for each reconnection
- **100% Reconnection Rate**: Successfully reconnects all orphaned spans with valid ancestry data

#### File-Based Storage
- **Original Traces**: Stores complete original traces from Jaeger
- **Lossy Traces**: Saves traces after data loss simulation
- **Reconstructed Traces**: Stores traces after Bloom filter reconnection
- **Jaeger-Compatible Format**: All files can be imported back into Jaeger UI
- **Metadata Tracking**: Comprehensive size and performance metrics

#### Trace Size Analysis
- **Byte-Level Tracking**: Measures trace sizes in JSON serialized bytes
- **Size Reduction Metrics**: Tracks data loss impact on trace size
- **Recovery Analysis**: Measures reconnection effectiveness
- **Summary Statistics**: Aggregated metrics across all processed traces

### Usage

#### Jaeger Trace Loader

```bash
go run jaeger_trace_loader.go

# Or load from a folder of JSON files
go run jaeger_trace_loader.go -input folder -folder data/tagged-hash-3

# With custom Jaeger URL
go run jaeger_trace_loader.go -input jaeger -jaeger-url http://localhost:16686
```

**Command-line flags:**
- `-input`: Source type - "jaeger" (default) or "folder"
- `-folder`: Path to folder containing JSON trace files (required if `input=folder`)
- `-jaeger-url`: Jaeger API URL (default: "http://jaeger-ctr:16686")

#### Trace Tagger

```bash
go run trace_tagger.go -input data/uber -mode hash -depth 3

# With custom output directory
go run trace_tagger.go -input data/uber -output data/tagged-bloom-5 -mode bloom -depth 5

# Hybrid mode
go run trace_tagger.go -input data/uber -mode hybrid -depth 3
```

**Command-line flags:**
- `-input`: Input directory containing trace JSON files (default: ".")
- `-output`: Output directory (default: "data/tagged-{mode}-{depth}")
- `-depth`: Depth interval for high-priority spans (default: 3)
- `-mode`: Ancestry mode - "hash" (default), "bloom", or "hybrid"

#### Trace Generator

```bash
python3 generate_trace.py --depth 5 --width 3 --output-dir synthetic-traces/t-5-3 --num-traces 20
```

**Command-line arguments:**
- `--depth`, `-d`: Call depth (d). Root is at depth 0, leaves at depth d (required)
- `--width`, `-w`: Fan-out width (w). Each node has w children (required)
- `--output-dir`, `-o`: Output directory path. Will be created if it doesn't exist (required)
- `--num-traces`, `-n`: Number of traces to generate (default: 1)

#### Batch Trace Generation

```bash
./generate_all_traces.sh
```

Generates synthetic traces for all combinations of depth (2-10) and width (1-20) with 20 traces per configuration. Output is saved to `synthetic-traces/` directory.

#### Batch Processing

```bash
./run_bridges_on_all.sh
```

Processes all tagged directories in `data/` matching the `tagged-*-*` pattern, running the bridges tool on each directory sequentially.

#### Plotting Metrics

```bash
python3 plot_metrics.py
```

Loads metadata from all `*-reconstructed` directories in `data/` and creates comprehensive plots for:
- Ancestry data size vs checkpoint distance
- Ancestry data size per span vs checkpoint distance
- Reconnection rate vs checkpoint distance
- Span difference vs checkpoint distance
- Total span size vs checkpoint distance

Plots are differentiated by ancestry mode (hash/bloom/hybrid) and saved as PDF files.

#### Simple Statistics Plotting

```bash
python3 plot_simple_stats.py
```

Creates simple plots for call depth per trace and number of spans per trace using reconstructed metadata.

#### Ancestry Size Calculation

```bash
python3 calculate_ancestry_size.py --depth 5 --width 3 --checkpoint-distance 1
```

Calculates average ancestry data size for synthetic traces in hybrid mode, matching the logic used by `trace_tagger.go`.

### Configuration

The loader connects to Jaeger using the service name `jaeger-ctr:16686` (based on Kubernetes service configuration). It loads up to 10 traces per service by default.

### Output

The tool provides:
- **Service Discovery**: List of available services and trace counts
- **Trace Analysis**: Detailed span hierarchy and parent-child relationships
- **Data Loss Detection**: Comprehensive analysis of missing parent references
- **Simulation Results**: Data loss simulation with different loss percentages
- **Reconnection Results**: Trace reconstruction with confidence scores (supports bloom, hash, and hybrid modes)
- **File Storage**: Organized storage of original, lossy, and reconstructed traces
- **Metadata Analysis**: Detailed size comparisons and performance metrics
- **Statistics**: Bloom filter performance and cache metrics

### File Structure

```
bridges/
├── data/
│   ├── lossy/
│   │   ├── traces.json      # Traces after data loss simulation
│   │   └── metadata.json    # Size and data loss metrics
│   ├── reconstructed/
│   │   ├── traces.json      # Traces after reconnection
│   │   └── metadata.json    # Reconnection and size recovery metrics
│   ├── tagged-hash-3/       # Tagged traces (hash mode, depth 3)
│   │   └── *.json           # Processed traces with ancestry tags
│   └── tagged-bloom-5/      # Tagged traces (bloom mode, depth 5)
│       └── *.json           # Processed traces with ancestry tags
├── jaeger_trace_loader.go   # Main trace loader and reconstruction tool
├── trace_tagger.go          # Trace tagging utility
├── generate_trace.py        # Synthetic trace generator
├── generate_all_traces.sh   # Batch trace generation script
├── run_bridges_on_all.sh    # Batch processing script
├── plot_metrics.py          # Comprehensive metrics plotting
├── plot_simple_stats.py     # Simple statistics plotting
├── calculate_ancestry_size.py # Ancestry size calculator
└── truss_heatmap.py         # Truss-size model heatmap generator
```

### Data Loss Detection Algorithm

1. **Span ID Mapping**: Creates a map of all available span IDs in the trace
2. **Reference Validation**: Checks each span's CHILD_OF references against available span IDs
3. **Missing Parent Detection**: Identifies spans that reference non-existent parent SpanIDs
4. **Orphan Classification**: Marks spans with missing parents as orphaned
5. **Severity Assessment**: Calculates loss severity based on percentage of affected spans

### Data Loss Simulation Algorithm

1. **Priority Classification**: Identifies high-priority spans (roots, leaves, depth intervals) and low-priority spans
2. **Protected Spans**: Root spans and leaf spans are never removed
3. **Low-Priority Removal**: Randomly selects and removes low-priority spans based on loss percentage
4. **Randomized Simulation**: Uses random seed to ensure different loss patterns each run
5. **Process Filtering**: Only includes processes referenced by remaining spans
6. **Automatic Analysis**: Processes simulated traces to detect and measure data loss

### Trace Reconnection Algorithm

#### General Flow

1. **Ancestry Data Extraction**: Attempts to get ancestry data from `ancestry`/`ancestry_mode` tags
2. **Descendant Fallback**: If orphan lacks ancestry data, searches for nearest downstream descendant with ancestry
3. **Baggage Fallback**: If no descendant found, falls back to `__bag.hash_array` or `__bag.bloom_filter`
4. **Orphan Identification**: Finds spans with missing parent references
5. **Mode Detection**: Determines ancestry mode (bloom, hash, or hybrid) from tags or trace-wide mode

#### Bloom Filter Mode

1. **Deserialization**: Deserializes Bloom filter from ancestry data
2. **Membership Testing**: Tests all existing span IDs against the Bloom filter
3. **Nearest Ancestor Selection**: Chooses the deepest ancestor that tests positive
4. **Confidence Calculation**: Computes confidence based on Bloom filter false positive rate

#### Hash Array Mode

1. **Array Parsing**: Parses comma-separated span ID ancestry chain (root → ... → current)
2. **Orphan Location**: Finds orphan's position in its own ancestry array
3. **Anchor Finding**: Locates deepest existing ancestor (anchor) in the ancestry chain
4. **Parent Inference**: Derives missing parent from ancestry chain (immediately before orphan)
5. **Synthetic Span Creation**: Creates missing intermediate spans between anchor and parent
6. **Parent Handling**: Handles cases where parent was already synthesized by previous orphan
7. **Reference Update**: Updates orphan's CHILD_OF reference to point to reconnected parent

#### Hybrid Mode

1. **Bloom Filter Ancestry**: Uses Bloom filter for ancestry membership testing (same as Bloom Filter Mode)
2. **Hash Array Sibling Ordering**: Uses hash arrays at fan-out points to maintain sibling ordering
3. **Fan-Out Detection**: Identifies spans with multiple children (fan-out points)
4. **Two-Pass Reconstruction**: First pass uses Bloom filter for ancestry, second pass merges synthetic nodes using hash arrays
5. **Synthetic Node Merging**: Merges synthetic spans created by multiple orphans that share common ancestors
6. **BFS-Based Merging**: Uses breadth-first search to merge synthetic nodes based on hash array entries

#### Synthetic Span Creation

- **Service**: "unknown_service:reconstructed"
- **Operation**: "unknown"
- **Tags**: `bridge.synthetic=true`
- **Timing**: `startTime = child.startTime - 1µs`, `duration = 1µs`
- **Process**: Uses dedicated `p_unknown` process ID

### Metadata Tracking

#### Lossy Trace Metadata
- **Size Metrics**: Original vs lossy trace sizes in bytes
- **Span Counts**: Original vs remaining span counts
- **Data Loss Details**: Missing span IDs, orphaned spans, severity classification
- **Size Reduction**: Percentage reduction from data loss

#### Reconstructed Trace Metadata
- **Reconnection Results**: Bridge edges, confidence scores, success rates
- **Synthetic Span Count**: Number of synthetic spans created per bridge and total
- **Size Recovery**: Original vs reconstructed trace sizes
- **Bridge Details**: Original parent IDs, confidence metrics, reconnection tags
- **Recovery Analysis**: Effectiveness of ancestry-based reconnection

### Example Output

```
=== PROCESSING TRACES ===
Loaded 2 trace(s) from 0a0a4e85a349a110.json
  Trace 1: ID=0a0a4e85a349a110, Spans=117

=== TRACE RECONNECTION FLOW ===
--- Step 1: Simulate Data Loss ---
Data loss result: hasLoss=true, missing=30 spans, orphans=41 spans, severity=high

--- Step 2: Reconnect Trace ---
RECONNECTION - Found 59 orphaned spans in trace 0a0a4e85a349a110
RECONNECTION - Orphan 9735ac845d1f1e63: using own ancestry data (mode=hash)
RECONNECTION - Orphan 9735ac845d1f1e63: hash parts=[dc8ae8ee95af6184 ... 9735ac845d1f1e63]
RECONNECTION - Orphan 9735ac845d1f1e63: anchorIdx=8 anchorSpan=43e222bb14975419
RECONNECTION - Orphan 9735ac845d1f1e63: synthesizing 2 missing ancestors
RECONNECTION - Orphan 9735ac845d1f1e63: original missing parent was f4a6dd5607335ed2; reattaching under f4a6dd5607335ed2
...
RECONNECTION - Successfully reconnected 59/59 orphaned spans (100.0%), created 53 synthetic spans
✅ Reconnected: rate=100.0%, bridges=59, synthetic=53

--- Step 3: Save Traces to Files ---
💾 Saved 2 lossy traces to data/lossy
💾 Saved 2 reconstructed traces to data/reconstructed
```

### Metadata Example

#### Lossy Trace Metadata
```json
{
  "trace_id": "a8111947fcd2cbf997baa2901d39bf68",
  "original_spans": 20,
  "lossy_spans": 20,
  "removed_spans": 3,
  "original_size_bytes": 17575,
  "lossy_size_bytes": 15283,
  "size_reduction_percent": 13.04,
  "data_loss_info": {
    "hasDataLoss": true,
    "missingSpanIDs": ["42ef5117deeebead", "ee9971ac1740dfef", "a6dc3ac22515fdf3"],
    "orphanSpans": ["b4701e8b7c95d04c", "4cb34bcb2df68f91", "760fc00e29045b3e"],
    "lossSeverity": "low"
  }
}
```

#### Reconstructed Trace Metadata
```json
{
  "trace_id": "0a0a4e85a349a110",
  "original_spans": 117,
  "reconstructed_spans": 170,
  "original_size_bytes": 87543,
  "reconstructed_size_bytes": 92015,
  "size_recovery_percent": 105.10,
  "reconnection_result": {
    "reconnectionRate": 100.0,
    "totalSyntheticSpans": 53,
    "success": true,
    "bridgeEdges": [
      {
        "fromSpanID": "f4a6dd5607335ed2",
        "toSpanID": "9735ac845d1f1e63",
        "originalParent": "f4a6dd5607335ed2",
        "confidence": 0.7,
        "syntheticSpansCreated": 2
      }
    ]
  }
}
```

### Jaeger Backend Configuration

The loader expects Jaeger to be deployed with the following service configuration:
- **Service Name**: `jaeger-ctr`
- **API Port**: `16686`
- **OTLP Port**: `4317`
- **Thrift Port**: `14268`

This matches the configuration generated by the blueprint-docc-mod project in `examples/sockshop/build/k8s/`.

### Trace Tagger Priority Rules

The trace tagger assigns priority based on:
- **High Priority**: Root spans (no parent), leaf spans (no children), and spans at depth intervals (depth % d == 0)
- **Low Priority**: All other middle spans
- **Ancestry Data**: Only high-priority spans receive ancestry data tags

#### Ancestry Data Formats

**Hash Mode:**
- `ancestry_mode`: "hash"
- `ancestry`: Comma-separated span IDs from root to current span
- Example: `"dc8ae8ee95af6184,92745ae3dcba25bb,9f01a79ddbc76107"`

**Bloom Filter Mode:**
- `ancestry_mode`: "bloom"
- `ancestry`: Base64-encoded serialized Bloom filter
- Parameters: Capacity=10, Hash functions=7
- Serialization: GobEncode + Base64

**Hybrid Mode:**
- `ancestry_mode`: "hybrid"
- `ancestry`: Base64-encoded serialized Bloom filter (for ancestry)
- Hash arrays stored separately for sibling ordering at fan-out points
- Combines Bloom filter efficiency with hash array precision for sibling ordering

### Dependencies

- Go 1.25.2+
- `github.com/bits-and-blooms/bloom` for efficient trace lookups
- Python 3.x with packages:
  - `matplotlib` for plotting
  - `numpy` for numerical operations
  - `seaborn` (optional) for enhanced visualizations
  - `pandas` (optional) for data analysis

### API Reference

#### Jaeger Trace Loader Methods
- `LoadTraces(service, limit)` - Load traces from a specific service
- `LoadTracesFromFolder(folder)` - Load traces from JSON files in a folder
- `GetServices()` - Discover available services
- `DetectDataLoss(trace)` - Analyze trace for data loss
- `SimulateDataLossWithProtection(trace, percentage)` - Create data loss simulation (priority-based)
- `ReconnectTrace(trace)` - Reconnect orphaned spans using ancestry data (bloom, hash, or hybrid)
- `SaveLossyTraces(traces, metadata)` - Save lossy traces and metadata
- `SaveReconstructedTraces(traces, results)` - Save reconnected traces and results

#### Trace Tagger Methods
- `ProcessTrace(trace)` - Process a single trace, adding priority and ancestry tags
- `buildHashArrayAncestry(path)` - Create comma-separated span ID ancestry string
- `buildBloomFilterAncestry(path)` - Create serialized Bloom filter ancestry

#### Data Structures
- `JaegerTrace` - Complete trace with spans and metadata
- `DataLossInfo` - Data loss analysis results
- `ProcessedTrace` - Trace with analysis results
- `ReconnectionResult` - Bloom filter reconnection results
- `BridgeEdge` - Reconnection bridge with confidence metrics
- `TraceMetadata` - Size and performance metrics for traces
- `StorageMetadata` - Aggregated statistics across all traces
- `TraceStorage` - File-based storage management

#### Ancestry Data Integration

**Bloom Filter Mode:**
- **Serialization**: Uses `GobEncode`/`GobDecode` for efficient storage
- **Base64 Encoding**: Safe transmission in span tags
- **Capacity**: 10 elements with configurable false positive rate
- **Hash Functions**: 7 hash functions for better distribution
- **Containment Queries**: Fast membership testing for ancestry analysis
- **False Positive Rate**: ~1% with default parameters

**Hash Array Mode:**
- **Format**: Comma-separated span IDs from root to current span
- **Ordering**: Root span ID first, current span ID last
- **Storage**: Stored as string in `ancestry` tag
- **Reconstruction**: Direct span ID lookup for exact parent chain reconstruction
- **Synthetic Spans**: Creates missing intermediate spans with unknown service/operation

**Hybrid Mode:**
- **Bloom Filter Ancestry**: Uses Bloom filter for ancestry membership (same as Bloom Filter Mode)
- **Hash Array Sibling Ordering**: Uses hash arrays at fan-out points for sibling ordering
- **Fan-Out Detection**: Automatically detects spans with multiple children
- **Two-Pass Reconstruction**: First pass uses Bloom filter, second pass merges synthetic nodes using hash arrays

**Tag Structure:**
- `ancestry_mode`: "bloom", "hash", or "hybrid"
- `ancestry`: Serialized data (Base64 for bloom/hybrid, comma-separated IDs for hash)
- `prio`: "high" or "low"
- `bridge.reconnected`: Boolean tag on reconnected spans
- `bridge.original_parent`: Original missing parent span ID
- `bridge.confidence`: Confidence score (0-1)
- `bridge.synthetic`: Boolean tag on synthetic spans