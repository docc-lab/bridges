# Bridges Project

This repository contains tools for trace reconstruction and analysis, specifically designed to reconstruct traces with data loss using ancestry data stored within span objects and Bloom filters for intelligent trace reconnection.

## Jaeger Trace Loader

The `jaeger_trace_loader.go` tool provides comprehensive functionality to load, analyze, simulate data loss, and reconstruct traces from a Jaeger backend deployed in Kubernetes. It includes advanced Bloom filter-based reconnection algorithms and detailed metadata tracking for trace size comparisons.

### Features

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
- **Smart Span Removal**: Only removes non-root, non-leaf spans (middle nodes)
- **Realistic Simulation**: Creates actual data loss scenarios by removing critical hierarchy nodes
- **Configurable Loss Percentages**: Test different levels of data loss (10%, 20%, 30%, 50%)
- **Automatic Detection**: Simulated traces are automatically analyzed for data loss
- **Forced Data Loss**: Guarantees data loss by aggressively removing middle spans when needed

#### Trace Reconnection
- **Bloom Filter Analysis**: Extracts and deserializes Bloom filters from span baggage
- **Ancestry Reconstruction**: Uses Bloom filters to find nearest existing ancestors
- **Bridge Edge Creation**: Connects orphaned spans to their most likely ancestors
- **Confidence Scoring**: Provides confidence metrics for each reconnection
- **100% Reconnection Rate**: Successfully reconnects all orphaned spans in test scenarios

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

```bash
cd /users/dhuye/bridges
go run jaeger_trace_loader.go
```

### Configuration

The loader connects to Jaeger using the service name `jaeger-ctr:16686` (based on Kubernetes service configuration). It loads up to 10 traces per service by default.

### Output

The tool provides:
- **Service Discovery**: List of available services and trace counts
- **Trace Analysis**: Detailed span hierarchy and parent-child relationships
- **Data Loss Detection**: Comprehensive analysis of missing parent references
- **Simulation Results**: Data loss simulation with different loss percentages
- **Reconnection Results**: Bloom filter-based trace reconstruction with confidence scores
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
│   └── reconstructed/
│       ├── traces.json      # Traces after Bloom filter reconnection
│       └── metadata.json    # Reconnection and size recovery metrics
└── jaeger_trace_loader.go   # Main application
```

### Data Loss Detection Algorithm

1. **Span ID Mapping**: Creates a map of all available span IDs in the trace
2. **Reference Validation**: Checks each span's CHILD_OF references against available span IDs
3. **Missing Parent Detection**: Identifies spans that reference non-existent parent SpanIDs
4. **Orphan Classification**: Marks spans with missing parents as orphaned
5. **Severity Assessment**: Calculates loss severity based on percentage of affected spans

### Data Loss Simulation Algorithm

1. **Span Classification**: Identifies root spans (no parents) and leaf spans (no children)
2. **Eligible Span Selection**: Only selects middle nodes (spans with both parents and children)
3. **Smart Removal**: Randomly removes selected middle nodes to create realistic data loss
4. **Forced Data Loss**: If no data loss detected, aggressively removes middle spans to guarantee loss
5. **Automatic Analysis**: Processes simulated traces to detect and measure data loss

### Trace Reconnection Algorithm

1. **Bloom Filter Extraction**: Deserializes Bloom filters from span baggage attributes
2. **Orphan Identification**: Finds spans with missing parent references
3. **Ancestry Analysis**: For each orphan, queries Bloom filters to find contained span IDs
4. **Nearest Ancestor Selection**: Chooses the deepest (most specific) ancestor from available spans
5. **Bridge Creation**: Creates new CHILD_OF references with confidence metadata
6. **Validation**: Verifies reconnected traces have no remaining data loss

### Metadata Tracking

#### Lossy Trace Metadata
- **Size Metrics**: Original vs lossy trace sizes in bytes
- **Span Counts**: Original vs remaining span counts
- **Data Loss Details**: Missing span IDs, orphaned spans, severity classification
- **Size Reduction**: Percentage reduction from data loss

#### Reconstructed Trace Metadata
- **Reconnection Results**: Bridge edges, confidence scores, success rates
- **Size Recovery**: Original vs reconstructed trace sizes
- **Bridge Details**: Original parent IDs, confidence metrics, reconnection tags
- **Recovery Analysis**: Effectiveness of Bloom filter-based reconnection

### Example Output

```
=== PROCESSING TRACES ===
DEBUG - Trace a8111947fcd2cbf997baa2901d39bf68: Analyzing 23 spans
DEBUG - *** DATA LOSS DETECTED *** Span b4701e8b7c95d04c references missing parent 42ef5117deeebead (CHILD_OF)
Data loss result: hasLoss=true, missing=[42ef5117deeebead ee9971ac1740dfef a6dc3ac22515fdf3], orphans=[b4701e8b7c95d04c 4cb34bcb2df68f91 760fc00e29045b3e], severity=low

=== TRACE RECONNECTION FLOW ===
--- Step 1: Simulate Data Loss ---
FORCED SIMULATION - Removed 3 middle spans (30.0% of middle spans) from trace a8111947fcd2cbf997baa2901d39bf68
Simulated trace: a8111947fcd2cbf997baa2901d39bf68 (20 spans)

--- Step 2: Detect Data Loss ---
Data loss detected: true
Missing span IDs: [42ef5117deeebead ee9971ac1740dfef a6dc3ac22515fdf3]
Orphaned spans: [b4701e8b7c95d04c 4cb34bcb2df68f91 760fc00e29045b3e]

--- Step 3: Reconnect Trace Using Bloom Filters ---
RECONNECTION - Found 3 orphaned spans in trace a8111947fcd2cbf997baa2901d39bf68
RECONNECTION - Successfully reconnected 3/3 orphaned spans (100.0%)
✅ Reconnection successful!
Bridge edges created: 3
  Bridge 1: 7cf967b1 -> b4701e8b (original parent: 42ef5117, confidence: 0.70)
  Bridge 2: 64a8734a -> 4cb34bcb (original parent: ee9971ac, confidence: 0.70)
  Bridge 3: 64a8734a -> 760fc00e (original parent: a6dc3ac2, confidence: 0.70)

--- Step 4: Verify Reconnected Trace ---
Reconnected trace data loss: false
✅ All spans successfully reconnected!

--- Step 5: Save Traces to Files ---
💾 Saved 1 lossy traces to data/lossy
💾 Saved 1 reconstructed traces to data/reconstructed
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
  "trace_id": "a8111947fcd2cbf997baa2901d39bf68",
  "original_spans": 20,
  "reconstructed_spans": 20,
  "original_size_bytes": 15283,
  "reconstructed_size_bytes": 15850,
  "size_recovery_percent": 103.71,
  "reconnection_result": {
    "reconnectionRate": 100,
    "success": true,
    "bridgeEdges": [
      {
        "fromSpanID": "7cf967b1d3aeabe8",
        "toSpanID": "b4701e8b7c95d04c",
        "originalParent": "42ef5117deeebead",
        "confidence": 0.7
      }
    ]
  }
}
```

### Dependencies

- Go 1.25.2+
- `github.com/bits-and-blooms/bloom` for efficient trace lookups

### Jaeger Backend Configuration

The loader expects Jaeger to be deployed with the following service configuration:
- **Service Name**: `jaeger-ctr`
- **API Port**: `16686`
- **OTLP Port**: `4317`
- **Thrift Port**: `14268`

This matches the configuration generated by the blueprint-docc-mod project in `examples/sockshop/build/k8s/`.

### API Reference

#### Core Methods
- `LoadTraces(service, limit)` - Load traces from a specific service
- `GetServices()` - Discover available services
- `DetectDataLoss(trace)` - Analyze trace for data loss
- `SimulateDataLoss(trace, percentage)` - Create data loss simulation
- `ForceDataLoss(trace, percentage)` - Guarantee data loss by aggressive span removal
- `ReconnectTrace(trace)` - Reconnect orphaned spans using Bloom filters
- `SaveLossyTraces(traces, metadata)` - Save lossy traces and metadata
- `SaveReconstructedTraces(traces, results)` - Save reconnected traces and results

#### Data Structures
- `JaegerTrace` - Complete trace with spans and metadata
- `DataLossInfo` - Data loss analysis results
- `ProcessedTrace` - Trace with analysis results
- `ReconnectionResult` - Bloom filter reconnection results
- `BridgeEdge` - Reconnection bridge with confidence metrics
- `TraceMetadata` - Size and performance metrics for traces
- `StorageMetadata` - Aggregated statistics across all traces
- `TraceStorage` - File-based storage management

#### Bloom Filter Integration
- **Serialization**: Uses `GobEncode`/`GobDecode` for efficient storage
- **Base64 Encoding**: Safe transmission in span baggage attributes
- **Capacity**: 10 elements with configurable false positive rate
- **Hash Functions**: Multiple hash functions for better distribution
- **Containment Queries**: Fast membership testing for ancestry analysis