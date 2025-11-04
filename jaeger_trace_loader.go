package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bits-and-blooms/bloom"
)

// JaegerTrace represents a simplified trace structure
type JaegerTrace struct {
	TraceID   string                 `json:"traceID"`
	Spans     []JaegerSpan          `json:"spans"`
	Processes map[string]Process   `json:"processes"`
	Warnings  []string              `json:"warnings"`
}

// JaegerSpan represents a span in the trace
type JaegerSpan struct {
	TraceID       string            `json:"traceID"`
	SpanID        string            `json:"spanID"`
	ParentSpanID  string            `json:"parentSpanID,omitempty"`
	ProcessID     string            `json:"processID"`
	OperationName string            `json:"operationName"`
	StartTime     int64             `json:"startTime"`
	Duration      int64             `json:"duration"`
	Tags          []Tag             `json:"tags"`
	Logs          []Log             `json:"logs"`
	References    []Reference       `json:"references"`
	Flags         int               `json:"flags"`
}

// Process represents process information
type Process struct {
	ServiceName string `json:"serviceName"`
	Tags        []Tag  `json:"tags"`
}

// Tag represents a key-value tag
type Tag struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
	Type  string      `json:"type"`
}

// Log represents a log entry
type Log struct {
	Timestamp int64 `json:"timestamp"`
	Fields    []Tag `json:"fields"`
}

// Reference represents a span reference
type Reference struct {
	RefType string `json:"refType"`
	TraceID string `json:"traceID"`
	SpanID  string `json:"spanID"`
}

// DataLossInfo represents information about data loss in a trace
type DataLossInfo struct {
	HasDataLoss     bool     `json:"hasDataLoss"`
	MissingSpanIDs  []string `json:"missingSpanIDs"`
	OrphanSpans     []string `json:"orphanSpans"`
	LossSeverity    string   `json:"lossSeverity"` // "low", "medium", "high"
	AffectedSpans   int      `json:"affectedSpans"`
}

// BaggageInfo represents extracted baggage information
type BaggageInfo struct {
	TraceID        string            `json:"traceID"`
	BaggageData    map[string]string `json:"baggageData"`
	ExtractedInfo  map[string]interface{} `json:"extractedInfo"`
	ProcessingTime time.Duration     `json:"processingTime"`
}

// ProcessedTrace represents a trace with processing results
type ProcessedTrace struct {
	Trace       *JaegerTrace   `json:"trace"`
	DataLoss    *DataLossInfo  `json:"dataLoss"`
	Baggage     *BaggageInfo   `json:"baggage"`
	ProcessedAt time.Time      `json:"processedAt"`
}

// BridgeEdge represents a reconnected edge in the trace
type BridgeEdge struct {
	FromSpanID          string  `json:"fromSpanID"`
	ToSpanID            string  `json:"toSpanID"`
	OriginalParent      string  `json:"originalParent"` // The missing parent that was being referenced
	Depth               int     `json:"depth"`          // Depth of the reconnected span
	Confidence          float64 `json:"confidence"`     // Confidence in the reconnection (0-1)
	SyntheticSpansCreated int   `json:"syntheticSpansCreated"` // Number of synthetic spans created for this edge
}

// ReconnectionResult represents the result of trace reconnection
type ReconnectionResult struct {
	OriginalTrace        *JaegerTrace   `json:"originalTrace"`        // Lossy trace (after data loss simulation, before reconnection)
	TaggedTrace          *JaegerTrace   `json:"taggedTrace,omitempty"` // Original tagged trace (before data loss simulation)
	ReconnectedTrace     *JaegerTrace   `json:"reconnectedTrace"`
	BridgeEdges          []BridgeEdge   `json:"bridgeEdges"`
	ReconnectionRate     float64        `json:"reconnectionRate"` // Percentage of orphaned spans reconnected
	Success              bool           `json:"success"`
	TotalSyntheticSpans  int            `json:"totalSyntheticSpans"` // Total synthetic spans created during reconnection
}

// TraceStorage handles file-based storage of traces
type TraceStorage struct {
	dataDir           string
	lossyDir          string
	reconstructedDir  string
}

// TraceMetadata represents metadata for a single trace
type TraceMetadata struct {
	TraceID        string        `json:"trace_id"`
	OriginalSpans  int           `json:"original_spans"`
	LossySpans     int           `json:"lossy_spans,omitempty"`
	ReconstructedSpans int       `json:"reconstructed_spans,omitempty"`
	RemovedSpans   int           `json:"removed_spans,omitempty"`
	DataLossInfo   *DataLossInfo `json:"data_loss_info,omitempty"`
	ReconnectionResult *ReconnectionResult `json:"reconnection_result,omitempty"`
	// Size comparison metrics
	OriginalSizeBytes   int64 `json:"original_size_bytes,omitempty"`
	LossySizeBytes      int64 `json:"lossy_size_bytes,omitempty"`
	ReconstructedSizeBytes int64 `json:"reconstructed_size_bytes,omitempty"`
	SizeReductionPercent float64 `json:"size_reduction_percent,omitempty"`
	SizeRecoveryPercent  float64 `json:"size_recovery_percent,omitempty"`
	// Ancestry metrics
	CheckpointDistance          int     `json:"checkpoint_distance,omitempty"`
	AncestryDataSizeBytes       int64   `json:"ancestry_data_size_bytes,omitempty"`
	AncestryDataSizeBytesPerSpan float64 `json:"ancestry_data_size_bytes_per_span,omitempty"`
}

// StorageMetadata represents metadata for all traces
type StorageMetadata struct {
	CreatedAt     time.Time        `json:"created_at"`
	TotalTraces   int              `json:"total_traces"`
	Traces        []TraceMetadata  `json:"traces"`
	// Summary statistics
	TotalOriginalSpans     int     `json:"total_original_spans"`
	TotalLossySpans        int     `json:"total_lossy_spans,omitempty"`
	TotalReconstructedSpans int    `json:"total_reconstructed_spans,omitempty"`
	TotalOriginalSizeBytes  int64  `json:"total_original_size_bytes"`
	TotalLossySizeBytes     int64  `json:"total_lossy_size_bytes,omitempty"`
	TotalReconstructedSizeBytes int64 `json:"total_reconstructed_size_bytes,omitempty"`
	AverageSizeReductionPercent float64 `json:"average_size_reduction_percent,omitempty"`
	AverageSizeRecoveryPercent  float64 `json:"average_size_recovery_percent,omitempty"`
	// Ancestry summary metrics
	CheckpointDistance          int     `json:"checkpoint_distance,omitempty"`
	TotalAncestryDataSizeBytes  int64   `json:"total_ancestry_data_size_bytes,omitempty"`
	AverageAncestryDataSizeBytesPerSpan float64 `json:"average_ancestry_data_size_bytes_per_span,omitempty"`
}

// calculateTraceSize calculates the size of a trace in bytes when serialized to JSON
func calculateTraceSize(trace *JaegerTrace) (int64, error) {
	data, err := json.Marshal(trace)
	if err != nil {
		return 0, err
	}
	return int64(len(data)), nil
}

// extractCheckpointDistance extracts the checkpoint distance from a directory name
// Examples: "tagged-hash-3" -> 3, "tagged-hash-15" -> 15
func extractCheckpointDistance(dirPath string) (int, error) {
	dirName := filepath.Base(dirPath)
	parts := strings.Split(dirName, "-")
	if len(parts) == 0 {
		return 0, fmt.Errorf("invalid directory name format: %s", dirName)
	}
	// Get the last part after the last hyphen
	lastPart := parts[len(parts)-1]
	var distance int
	_, err := fmt.Sscanf(lastPart, "%d", &distance)
	if err != nil {
		return 0, fmt.Errorf("could not parse checkpoint distance from directory name %s: %w", dirName, err)
	}
	return distance, nil
}

// calculateAncestryDataSize calculates the actual binary data size of ancestry and ancestry_mode tags
func calculateAncestryDataSize(trace *JaegerTrace) int64 {
	var totalSize int64
	for _, span := range trace.Spans {
		var ancestryMode string
		for _, tag := range span.Tags {
			if tag.Key == "ancestry_mode" {
				if str, ok := tag.Value.(string); ok {
					ancestryMode = str
					// ancestry_mode string itself (e.g., "hash" or "bloom") - count as string bytes
					totalSize += int64(len(str))
				}
			}
		}
		
		for _, tag := range span.Tags {
			if tag.Key == "ancestry" {
				if str, ok := tag.Value.(string); ok {
					if ancestryMode == "hash" {
						// For hash mode: count 8 bytes per span ID (64 bits each)
						// Parse comma-separated hex span IDs
						parts := strings.Split(str, ",")
						for _, part := range parts {
							part = strings.TrimSpace(part)
							if len(part) == 16 { // Valid hex span ID is 16 hex chars = 8 bytes
								totalSize += 8
							}
						}
					} else if ancestryMode == "bloom" {
						// For bloom mode: count the actual bit array size (capacity in bytes)
						// Deserialize the bloom filter to get its capacity
						bf, err := deserializeBloomFilter(str)
						if err == nil {
							// Capacity is in bits, convert to bytes (round up)
							capacityBits := bf.Cap()
							capacityBytes := int64((capacityBits + 7) / 8) // Ceiling division
							totalSize += capacityBytes
						} else {
							// If deserialization fails, fall back to decoded Gob size
							decoded, err := base64.StdEncoding.DecodeString(str)
							if err == nil {
								totalSize += int64(len(decoded))
							} else {
								// Last resort: count as string size
								totalSize += int64(len(str))
							}
						}
					} else {
						// Unknown mode, count as string
						totalSize += int64(len(str))
					}
				}
			}
		}
	}
	return totalSize
}

// JaegerTraceLoader handles loading traces from Jaeger backend
type JaegerTraceLoader struct {
	jaegerURL    string
	httpClient   *http.Client
	bloomFilter  *bloom.BloomFilter
	traceCache   map[string]*JaegerTrace
    processedTraces map[string]*ProcessedTrace
}

// NewJaegerTraceLoader creates a new trace loader
func NewJaegerTraceLoader(jaegerURL string) *JaegerTraceLoader {
	// Create bloom filter for efficient trace ID lookups
	// Estimate 10000 traces with 0.01 false positive rate
	filter := bloom.NewWithEstimates(10000, 0.01)
	
    return &JaegerTraceLoader{
		jaegerURL:        jaegerURL,
		httpClient:       &http.Client{Timeout: 30 * time.Second},
		bloomFilter:      filter,
		traceCache:       make(map[string]*JaegerTrace),
        processedTraces:  make(map[string]*ProcessedTrace),
	}
}

// LoadTraces loads traces from Jaeger backend
func (loader *JaegerTraceLoader) LoadTraces(ctx context.Context, service string, limit int) ([]*JaegerTrace, error) {
	// Build the Jaeger API URL for traces
	url := fmt.Sprintf("%s/api/traces?service=%s&limit=%d", loader.jaegerURL, service, limit)
	
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	
	req.Header.Set("Accept", "application/json")
	
	resp, err := loader.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("jaeger API returned status %d: %s", resp.StatusCode, string(body))
	}
	
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}
	
	// Jaeger API returns {"data": [...]} structure
	var response struct {
		Data []*JaegerTrace `json:"data"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		// Fallback: try direct unmarshaling for backward compatibility
		var traces []*JaegerTrace
		if err2 := json.Unmarshal(body, &traces); err2 != nil {
			return nil, fmt.Errorf("failed to unmarshal traces: %w", err)
		}
		return traces, nil
	}
	
	traces := response.Data
	
	// Update bloom filter and cache
	for _, trace := range traces {
		loader.bloomFilter.Add([]byte(trace.TraceID))
		loader.traceCache[trace.TraceID] = trace
	}
	
	return traces, nil
}

// GetTraceByID retrieves a specific trace by ID
func (loader *JaegerTraceLoader) GetTraceByID(ctx context.Context, traceID string) (*JaegerTrace, error) {
	// Check bloom filter first for efficiency
	if !loader.bloomFilter.Test([]byte(traceID)) {
		return nil, fmt.Errorf("trace %s not found", traceID)
	}
	
	// Check cache first
	if trace, exists := loader.traceCache[traceID]; exists {
		return trace, nil
	}
	
	// Load from Jaeger API
	url := fmt.Sprintf("%s/api/traces/%s", loader.jaegerURL, traceID)
	
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	
	req.Header.Set("Accept", "application/json")
	
	resp, err := loader.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("jaeger API returned status %d: %s", resp.StatusCode, string(body))
	}
	
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}
	
	var traces []*JaegerTrace
	if err := json.Unmarshal(body, &traces); err != nil {
		return nil, fmt.Errorf("failed to unmarshal trace: %w", err)
	}
	
	if len(traces) == 0 {
		return nil, fmt.Errorf("trace %s not found", traceID)
	}
	
	trace := traces[0]
	loader.traceCache[traceID] = trace
	
	return trace, nil
}

// GetServices retrieves available services from Jaeger
func (loader *JaegerTraceLoader) GetServices(ctx context.Context) ([]string, error) {
	url := fmt.Sprintf("%s/api/services", loader.jaegerURL)
	
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	
	req.Header.Set("Accept", "application/json")
	
	resp, err := loader.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("jaeger API returned status %d: %s", resp.StatusCode, string(body))
	}
	
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}
	
	// Jaeger API returns an object with a "data" field containing the services array
	var response struct {
		Data []string `json:"data"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal services: %w", err)
	}
	
	return response.Data, nil
}

// GetTraceStats returns statistics about loaded traces
func (loader *JaegerTraceLoader) GetTraceStats() map[string]interface{} {
	return map[string]interface{}{
		"cached_traces": len(loader.traceCache),
		"bloom_filter_capacity": loader.bloomFilter.Cap(),
		"bloom_filter_estimated_count": loader.bloomFilter.K(),
		"bloom_filter_false_positive_rate": loader.bloomFilter.EstimateFalsePositiveRate(uint(len(loader.traceCache))),
	}
}

// DetectDataLoss analyzes a trace for data loss (missing parent spans)
func (loader *JaegerTraceLoader) DetectDataLoss(trace *JaegerTrace) *DataLossInfo {
	// Create a set of all span IDs in the trace
	spanIDs := make(map[string]bool)
	for _, span := range trace.Spans {
		spanIDs[span.SpanID] = true
	}
	
	var missingSpanIDs []string
	var orphanSpans []string
	
	// Debug: Print all span IDs and their parents
	fmt.Printf("  DEBUG - Trace %s: Analyzing %d spans\n", trace.TraceID, len(trace.Spans))
	fmt.Printf("  DEBUG - Available span IDs: %v\n", getKeys(spanIDs))
	
	// Check each span for missing parent references
	for i, span := range trace.Spans {
		fmt.Printf("  DEBUG - Span %d: ID=%s, ParentID='%s' (len=%d)\n", i+1, span.SpanID, span.ParentSpanID, len(span.ParentSpanID))
		fmt.Printf("  DEBUG - Span %d References: %+v\n", i+1, span.References)
		
		// Check for CHILD_OF references (the actual parent-child relationships)
		hasParent := false
		for j, ref := range span.References {
			fmt.Printf("  DEBUG - Reference %d: Type=%s, TraceID=%s, SpanID=%s\n", j+1, ref.RefType, ref.TraceID, ref.SpanID)
			
			if ref.RefType == "CHILD_OF" {
				hasParent = true
				// Check if the parent span exists in this trace
				if !spanIDs[ref.SpanID] {
					fmt.Printf("  DEBUG - *** DATA LOSS DETECTED *** Span %s references missing parent %s (CHILD_OF)\n", span.SpanID, ref.SpanID)
					missingSpanIDs = append(missingSpanIDs, ref.SpanID)
					orphanSpans = append(orphanSpans, span.SpanID)
				} else {
					fmt.Printf("  DEBUG - Span %s has valid parent %s (CHILD_OF)\n", span.SpanID, ref.SpanID)
				}
			}
		}
		
		// Also check the old ParentSpanID field for backward compatibility
		if span.ParentSpanID != "" && !spanIDs[span.ParentSpanID] {
			fmt.Printf("  DEBUG - *** DATA LOSS DETECTED *** Span %s references missing parent %s (ParentSpanID)\n", span.SpanID, span.ParentSpanID)
			missingSpanIDs = append(missingSpanIDs, span.ParentSpanID)
			orphanSpans = append(orphanSpans, span.SpanID)
		} else if span.ParentSpanID != "" {
			fmt.Printf("  DEBUG - Span %s has valid parent %s (ParentSpanID)\n", span.SpanID, span.ParentSpanID)
		} else if !hasParent {
			fmt.Printf("  DEBUG - Span %s is a root span (no parent)\n", span.SpanID)
		}
	}
	
	// Remove duplicates
	missingSpanIDs = removeDuplicates(missingSpanIDs)
	orphanSpans = removeDuplicates(orphanSpans)
	
	hasDataLoss := len(missingSpanIDs) > 0
	
	// Determine severity
	severity := "low"
	if hasDataLoss {
		affectedPercentage := float64(len(orphanSpans)) / float64(len(trace.Spans)) * 100
		if affectedPercentage > 50 {
			severity = "high"
		} else if affectedPercentage > 20 {
			severity = "medium"
		}
	}
	
	fmt.Printf("  DEBUG - Data loss result: hasLoss=%v, missing=%v, orphans=%v, severity=%s\n", 
		hasDataLoss, missingSpanIDs, orphanSpans, severity)
	
	return &DataLossInfo{
		HasDataLoss:    hasDataLoss,
		MissingSpanIDs: missingSpanIDs,
		OrphanSpans:    orphanSpans,
		LossSeverity:   severity,
		AffectedSpans:  len(orphanSpans),
	}
}

// ExtractBaggageInfo extracts baggage information from a trace (placeholder implementation)
func (loader *JaegerTraceLoader) ExtractBaggageInfo(trace *JaegerTrace) *BaggageInfo {
	startTime := time.Now()
	
	// TODO: Implement actual baggage extraction logic
	// This is a placeholder for the baggage processing you mentioned
	baggageData := make(map[string]string)
	extractedInfo := make(map[string]interface{})
	
	// Placeholder: Extract from span tags that might contain baggage
	for _, span := range trace.Spans {
		for _, tag := range span.Tags {
			if tag.Key == "baggage" || tag.Key == "trace.baggage" {
				if str, ok := tag.Value.(string); ok {
					baggageData[tag.Key] = str
				}
			}
		}
	}
	
	// Placeholder: Basic extracted info
	extractedInfo["traceID"] = trace.TraceID
	extractedInfo["spanCount"] = len(trace.Spans)
	extractedInfo["baggageCount"] = len(baggageData)
	
	processingTime := time.Since(startTime)
	
	return &BaggageInfo{
		TraceID:        trace.TraceID,
		BaggageData:    baggageData,
		ExtractedInfo:  extractedInfo,
		ProcessingTime: processingTime,
	}
}

// ProcessTrace processes a single trace for data loss and baggage extraction
func (loader *JaegerTraceLoader) ProcessTrace(trace *JaegerTrace) *ProcessedTrace {
	// Detect data loss
	dataLoss := loader.DetectDataLoss(trace)
	
	// Extract baggage info (only if there's data loss)
	var baggage *BaggageInfo
	if dataLoss.HasDataLoss {
		baggage = loader.ExtractBaggageInfo(trace)
	}
	
	processedTrace := &ProcessedTrace{
		Trace:       trace,
		DataLoss:    dataLoss,
		Baggage:     baggage,
		ProcessedAt: time.Now(),
	}
	
	// Cache the processed trace
	loader.processedTraces[trace.TraceID] = processedTrace
	
	return processedTrace
}

// ProcessTraces processes multiple traces
func (loader *JaegerTraceLoader) ProcessTraces(traces []*JaegerTrace) []*ProcessedTrace {
	var processedTraces []*ProcessedTrace
	
	for _, trace := range traces {
        processed := loader.ProcessTrace(trace)
		processedTraces = append(processedTraces, processed)
	}
	
	return processedTraces
}

// GetDataLossSummary returns a summary of data loss across all processed traces
func (loader *JaegerTraceLoader) GetDataLossSummary() map[string]interface{} {
	totalTraces := len(loader.processedTraces)
	tracesWithDataLoss := 0
	highSeverityCount := 0
	mediumSeverityCount := 0
	lowSeverityCount := 0
	
	for _, processed := range loader.processedTraces {
		if processed.DataLoss.HasDataLoss {
			tracesWithDataLoss++
			switch processed.DataLoss.LossSeverity {
			case "high":
				highSeverityCount++
			case "medium":
				mediumSeverityCount++
			case "low":
				lowSeverityCount++
			}
		}
	}
	
	return map[string]interface{}{
		"total_traces":           totalTraces,
		"traces_with_data_loss":  tracesWithDataLoss,
		"data_loss_percentage":   float64(tracesWithDataLoss) / float64(totalTraces) * 100,
		"high_severity_count":    highSeverityCount,
		"medium_severity_count":  mediumSeverityCount,
		"low_severity_count":     lowSeverityCount,
	}
}

// GetProcessedTracesWithDataLoss returns all processed traces that have data loss
func (loader *JaegerTraceLoader) GetProcessedTracesWithDataLoss() []*ProcessedTrace {
	var tracesWithDataLoss []*ProcessedTrace
	
	for _, processed := range loader.processedTraces {
		if processed.DataLoss.HasDataLoss {
			tracesWithDataLoss = append(tracesWithDataLoss, processed)
		}
	}
	
	return tracesWithDataLoss
}

// ClearCache clears the trace cache and bloom filter
func (loader *JaegerTraceLoader) ClearCache() {
	loader.traceCache = make(map[string]*JaegerTrace)
	loader.processedTraces = make(map[string]*ProcessedTrace)
	loader.bloomFilter = bloom.NewWithEstimates(10000, 0.01)
}

// Helper function to get keys from a map
func getKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// Helper function to remove duplicates from a slice
func removeDuplicates(slice []string) []string {
	keys := make(map[string]bool)
	var result []string
	
	for _, item := range slice {
		if !keys[item] {
			keys[item] = true
			result = append(result, item)
		}
	}
	
	return result
}

// getAncestryFromTags returns ancestry_mode and ancestry payload from span tags (no fallback)
func getAncestryFromTags(span JaegerSpan) (string, string) {
    var mode, payload string
    for _, t := range span.Tags {
        if t.Key == "ancestry_mode" {
            if s, ok := t.Value.(string); ok {
                mode = s
            }
        } else if t.Key == "ancestry" {
            if s, ok := t.Value.(string); ok {
                payload = s
            }
        }
    }
    return mode, payload
}

// getHashFromTags returns the hash array from span tags (for hybrid mode)
func getHashFromTags(span JaegerSpan) string {
    for _, t := range span.Tags {
        if t.Key == "hash" {
            if s, ok := t.Value.(string); ok {
                return s
            }
        }
    }
    return ""
}

// parseHashEntry parses a hash entry in format "spanID:depth" and returns spanID and depth
func parseHashEntry(entry string) (spanID string, depth int, err error) {
    parts := strings.Split(entry, ":")
    if len(parts) != 2 {
        return "", 0, fmt.Errorf("invalid hash entry format: %s (expected spanID:depth)", entry)
    }
    spanID = strings.TrimSpace(parts[0])
    _, err = fmt.Sscanf(strings.TrimSpace(parts[1]), "%d", &depth)
    if err != nil {
        return "", 0, fmt.Errorf("invalid depth in hash entry %s: %w", entry, err)
    }
    return spanID, depth, nil
}

// getAncestryFromLeafBaggage returns ancestry from legacy __bag.* keys for leaves
func getAncestryFromLeafBaggage(span JaegerSpan) (string, string) {
    var mode, payload string
    // Prefer hash array when available
    for _, t := range span.Tags {
        if t.Key == "__bag.hash_array" {
            if s, ok := t.Value.(string); ok && s != "" {
                return "hash", s
            }
        }
    }
    for _, t := range span.Tags {
        if t.Key == "__bag.bloom_filter" {
            if s, ok := t.Value.(string); ok && s != "" {
                return "bloom", s
            }
        }
    }
    return mode, payload
}

// getAncestryFlexible tries normal tags; if absent and span is a leaf, falls back to __bag.*
func getAncestryFlexible(span JaegerSpan, children map[string][]string) (string, string) {
    mode, payload := getAncestryFromTags(span)
    if payload != "" {
        return mode, payload
    }
    // Leaf fallback allowed
    if len(children[span.SpanID]) == 0 {
        return getAncestryFromLeafBaggage(span)
    }
    return "", ""
}

// findNearestDescendantWithAncestry does BFS to locate the closest descendant having ancestry tags (ancestry/ancestry_mode only, no __bag.*)
func findNearestDescendantWithAncestry(startID string, children map[string][]string, byID map[string]JaegerSpan) *JaegerSpan {
    visited := make(map[string]bool)
    q := []string{startID}
    visited[startID] = true
    // Do not consider the start node itself; we want a descendant
    for len(q) > 0 {
        cur := q[0]
        q = q[1:]
        for _, child := range children[cur] {
            if visited[child] {
                continue
            }
            visited[child] = true
            if s, ok := byID[child]; ok {
                // Only check ancestry/ancestry_mode tags, no __bag.* fallback
                _, payload := getAncestryFromTags(s)
                if payload != "" {
                    // nearest found
                    scopy := s
                    return &scopy
                }
            }
            q = append(q, child)
        }
    }
    return nil
}

// getTraceAncestryMode determines the ancestry_mode used in the trace by checking any span with ancestry_mode tag
func getTraceAncestryMode(trace *JaegerTrace) string {
    for _, span := range trace.Spans {
        for _, tag := range span.Tags {
            if tag.Key == "ancestry_mode" {
                if mode, ok := tag.Value.(string); ok && mode != "" {
                    return mode
                }
            }
        }
    }
    return "" // No ancestry_mode found in trace
}

// getBaggageAncestryByMode gets ancestry from __bag.* attributes based on the specified mode
func getBaggageAncestryByMode(span JaegerSpan, mode string) string {
    var bagKey string
    if mode == "hash" {
        bagKey = "__bag.hash_array"
    } else if mode == "bloom" {
        bagKey = "__bag.bloom_filter"
    } else {
        return "" // Unknown mode
    }
    
    for _, tag := range span.Tags {
        if tag.Key == bagKey {
            if payload, ok := tag.Value.(string); ok && payload != "" {
                return payload
            }
        }
    }
    return ""
}

// (removed ancestry tagging helpers)

// isHighPrioritySpan returns true if span has prio=="high" or __bag.prio==1
func isHighPrioritySpan(span JaegerSpan) bool {
    for _, t := range span.Tags {
        if t.Key == "prio" {
            if s, ok := t.Value.(string); ok && s == "high" {
                return true
            }
        } else if t.Key == "__bag.prio" {
            // Jaeger JSON stores numbers as float64 when decoded into interface{}
            switch v := t.Value.(type) {
            case int:
                if v == 1 { return true }
            case int64:
                if v == 1 { return true }
            case float64:
                if int(v) == 1 { return true }
            }
        }
    }
    return false
}

func isLowPrioritySpan(span JaegerSpan) bool { return !isHighPrioritySpan(span) }

func main() {
	// Seed random number generator for different data loss simulation each run
	rand.Seed(time.Now().UnixNano())
	
	// Parse command line flags
	var (
		inputSource = flag.String("input", "jaeger", "Input source: 'jaeger' or 'folder' (default: jaeger)")
		inputFolder = flag.String("folder", "", "Folder path when input=folder (required if input=folder)")
		jaegerURL   = flag.String("jaeger-url", "http://jaeger-ctr:16686", "Jaeger URL (used when input=jaeger)")
	)
	flag.Parse()
	
	// Validate input source
	if *inputSource != "jaeger" && *inputSource != "folder" {
		fmt.Fprintf(os.Stderr, "Error: input must be 'jaeger' or 'folder'\n")
		os.Exit(1)
	}
	
	// Validate folder path if using folder input
	if *inputSource == "folder" && *inputFolder == "" {
		fmt.Fprintf(os.Stderr, "Error: folder path required when input=folder\n")
		os.Exit(1)
	}
	
	loader := NewJaegerTraceLoader(*jaegerURL)
	ctx := context.Background()
	
	// Determine output base name based on input source
	var outputBaseName string
	if *inputSource == "folder" {
		// Extract base name from folder path (e.g., "data/tagged-hash-3" -> "tagged-hash-3")
		outputBaseName = filepath.Base(*inputFolder)
	} else {
		// Use "jaeger" for Jaeger input
		outputBaseName = "jaeger"
	}
	
	// Initialize trace storage
	dataDir := "./data"
	storage := NewTraceStorage(dataDir, outputBaseName)
	if err := storage.Initialize(); err != nil {
		fmt.Printf("Error initializing storage: %v\n", err)
		return
	}
	
	// Load traces based on input source
	var allTraces []*JaegerTrace
	var totalTraces int
	var checkpointDistance int = 0 // Default to 0 if not from folder
	
	if *inputSource == "jaeger" {
		// Load from Jaeger (original implementation)
		fmt.Println("Fetching available services from Jaeger...")
		services, err := loader.GetServices(ctx)
		if err != nil {
			fmt.Printf("Error fetching services: %v\n", err)
			return
		}
		
		fmt.Printf("Available services: %v\n", services)
		
		// Load and process traces for each service
		for _, service := range services {
			fmt.Printf("\nLoading traces for service: %s\n", service)
			traces, err := loader.LoadTraces(ctx, service, 10) // Limit to 10 traces per service
			if err != nil {
				fmt.Printf("Error loading traces for %s: %v\n", service, err)
				continue
			}
			
			serviceTraces := len(traces)
			totalTraces += serviceTraces
			allTraces = append(allTraces, traces...)
			fmt.Printf("Loaded %d traces for service %s\n", serviceTraces, service)
			
			// Print trace details
			for i, trace := range traces {
				if i >= 3 { // Limit output to first 3 traces
					break
				}
				fmt.Printf("  Trace %d: ID=%s, Spans=%d\n", i+1, trace.TraceID, len(trace.Spans))
			}
		}
	} else {
		// Load from folder
		fmt.Printf("Loading traces from folder: %s\n", *inputFolder)
		
		// Extract checkpoint distance from folder name
		var err error
		checkpointDistance, err = extractCheckpointDistance(*inputFolder)
		if err != nil {
			fmt.Printf("Warning: could not extract checkpoint distance: %v (defaulting to 0)\n", err)
			checkpointDistance = 0
		} else {
			fmt.Printf("Extracted checkpoint distance: %d\n", checkpointDistance)
		}
		
		files, err := filepath.Glob(filepath.Join(*inputFolder, "*.json"))
		if err != nil {
			fmt.Printf("Error finding JSON files: %v\n", err)
			return
		}
		
		if len(files) == 0 {
			fmt.Printf("No JSON files found in %s\n", *inputFolder)
			return
		}
		
		fmt.Printf("Found %d JSON file(s)\n", len(files))
		
		for _, filePath := range files {
			fileName := filepath.Base(filePath)
			fmt.Printf("\nLoading file: %s\n", fileName)
			
			traces, err := loadTracesFromFile(filePath)
			if err != nil {
				fmt.Printf("Error loading %s: %v\n", fileName, err)
				continue
			}
			
			totalTraces += len(traces)
			allTraces = append(allTraces, traces...)
			fmt.Printf("Loaded %d trace(s) from %s\n", len(traces), fileName)
			
			// Print trace details
			for i, trace := range traces {
				if i >= 3 { // Limit output to first 3 traces
					break
				}
				fmt.Printf("  Trace %d: ID=%s, Spans=%d\n", i+1, trace.TraceID, len(trace.Spans))
			}
		}
	}
	
	if len(allTraces) == 0 {
		fmt.Printf("\nNo traces loaded. Exiting.\n")
		return
	}
	
	// Process all traces for data loss and baggage extraction
	fmt.Printf("\n=== PROCESSING TRACES ===\n")
	processedTraces := loader.ProcessTraces(allTraces)
	
	// Print data loss analysis
	dataLossSummary := loader.GetDataLossSummary()
	fmt.Printf("\nData Loss Analysis:\n")
	for key, value := range dataLossSummary {
		fmt.Printf("  %s: %v\n", key, value)
	}
	
	// Show traces with data loss
	tracesWithDataLoss := loader.GetProcessedTracesWithDataLoss()
	if len(tracesWithDataLoss) > 0 {
		fmt.Printf("\nTraces with Data Loss (%d):\n", len(tracesWithDataLoss))
		for i, processed := range tracesWithDataLoss {
			if i >= 5 { // Limit to first 5 traces with data loss
				fmt.Printf("  ... and %d more\n", len(tracesWithDataLoss)-5)
				break
			}
			fmt.Printf("  Trace %s: Severity=%s, Missing=%d, Orphans=%d\n", 
				processed.Trace.TraceID, 
				processed.DataLoss.LossSeverity,
				len(processed.DataLoss.MissingSpanIDs),
				len(processed.DataLoss.OrphanSpans))
		}
	}
	
	// Print total traces loaded
	fmt.Printf("\n=== SUMMARY ===\n")
	fmt.Printf("Total traces loaded: %d\n", totalTraces)
	fmt.Printf("Total traces processed: %d\n", len(processedTraces))
	fmt.Printf("Traces with data loss: %d\n", len(tracesWithDataLoss))
	
	// Print statistics
	stats := loader.GetTraceStats()
	fmt.Printf("\nTrace Statistics:\n")
	for key, value := range stats {
		fmt.Printf("  %s: %v\n", key, value)
	}
	
	// Test complete trace reconnection flow on all traces
	fmt.Printf("\n=== TRACE RECONNECTION FLOW ===\n")
	fmt.Printf("Processing %d traces for data loss simulation and reconnection\n", len(processedTraces))
	
	// Collect all traces for batch saving
	var allLossyTraces []*JaegerTrace
	var allLossyMetadata []*DataLossInfo
	var allReconstructedTraces []*JaegerTrace
	var allReconnectionResults []*ReconnectionResult
	
	for idx, processedTrace := range processedTraces {
		if processedTrace == nil || processedTrace.Trace == nil || len(processedTrace.Trace.Spans) <= 1 {
			continue // Skip traces with 1 or fewer spans
		}
		
		trace := processedTrace.Trace
		fmt.Printf("\n--- Trace %d/%d: %s (%d spans) ---\n", idx+1, len(processedTraces), trace.TraceID, len(trace.Spans))
		
		// Step 1: Simulate data loss
		fmt.Printf("  Step 1: Simulating data loss...\n")
		simulatedTrace := loader.ForceDataLoss(trace, 1.0) // Force 100% data loss
		fmt.Printf("  Simulated trace: %s (%d spans)\n", simulatedTrace.TraceID, len(simulatedTrace.Spans))
		
		// Step 2: Detect data loss
		dataLossInfo := loader.DetectDataLoss(simulatedTrace)
		if !dataLossInfo.HasDataLoss {
			fmt.Printf("  ⚠️  No data loss detected - skipping reconnection\n")
			continue
		}
		
		fmt.Printf("  Data loss detected: missing=%d spans, orphans=%d spans\n", 
			len(dataLossInfo.MissingSpanIDs), len(dataLossInfo.OrphanSpans))
		
		// Step 3: Reconnect trace
		fmt.Printf("  Step 2: Reconnecting trace...\n")
		reconnectionResult := loader.ReconnectTrace(simulatedTrace)
		
		// Store the original tagged trace (before data loss simulation) in the reconnection result
		// This ensures original_spans refers to the trace BEFORE data loss, not after
		originalTaggedTrace := &JaegerTrace{
			TraceID:   trace.TraceID,
			Spans:     make([]JaegerSpan, len(trace.Spans)),
			Processes: make(map[string]Process),
			Warnings:  trace.Warnings,
		}
		copy(originalTaggedTrace.Spans, trace.Spans)
		for k, v := range trace.Processes {
			originalTaggedTrace.Processes[k] = v
		}
		reconnectionResult.TaggedTrace = originalTaggedTrace
		
		if reconnectionResult.Success {
			fmt.Printf("  ✅ Reconnected: rate=%.1f%%, bridges=%d, synthetic=%d\n", 
				reconnectionResult.ReconnectionRate, len(reconnectionResult.BridgeEdges), 
				reconnectionResult.TotalSyntheticSpans)
			
			// Add to batch collections
			allLossyTraces = append(allLossyTraces, simulatedTrace)
			allLossyMetadata = append(allLossyMetadata, dataLossInfo)
			allReconstructedTraces = append(allReconstructedTraces, reconnectionResult.ReconnectedTrace)
			allReconnectionResults = append(allReconnectionResults, reconnectionResult)
		} else {
			fmt.Printf("  ❌ Reconnection failed\n")
			// Still save lossy trace even if reconnection failed
			allLossyTraces = append(allLossyTraces, simulatedTrace)
			allLossyMetadata = append(allLossyMetadata, dataLossInfo)
		}
	}
	
	// Step 4: Save all traces to files
	if len(allLossyTraces) > 0 {
		fmt.Printf("\n--- Saving Results ---\n")
		fmt.Printf("Saving %d lossy traces...\n", len(allLossyTraces))
		if err := storage.SaveLossyTraces(allLossyTraces, allLossyMetadata, checkpointDistance); err != nil {
			fmt.Printf("Error saving lossy traces: %v\n", err)
		} else {
			fmt.Printf("✅ Saved lossy traces\n")
		}
		
		if len(allReconstructedTraces) > 0 {
			fmt.Printf("Saving %d reconstructed traces...\n", len(allReconstructedTraces))
			if err := storage.SaveReconstructedTraces(allReconstructedTraces, allReconnectionResults, checkpointDistance); err != nil {
				fmt.Printf("Error saving reconstructed traces: %v\n", err)
			} else {
				fmt.Printf("✅ Saved reconstructed traces\n")
			}
		}
	} else {
		fmt.Printf("\n⚠️  No traces with data loss to save\n")
	}
}

// deserializeBloomFilter converts a base64-encoded string back to a bloom filter
func deserializeBloomFilter(serialized string) (*bloom.BloomFilter, error) {
	data, err := base64.StdEncoding.DecodeString(serialized)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64: %w", err)
	}
	
	bf := &bloom.BloomFilter{}
	err = bf.GobDecode(data)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize bloom filter: %w", err)
	}
	
	return bf, nil
}

// SimulateDataLossWithProtection removes random spans while protecting root and leaf spans
func (loader *JaegerTraceLoader) SimulateDataLossWithProtection(trace *JaegerTrace, lossPercentage float64) *JaegerTrace {
	// Create a copy of the trace
	simulatedTrace := &JaegerTrace{
		TraceID:   trace.TraceID + "_simulated_" + fmt.Sprintf("%.0f", lossPercentage*100),
		Spans:     make([]JaegerSpan, 0, len(trace.Spans)),
		Processes: trace.Processes,
		Warnings:  trace.Warnings,
	}
	
	// Build span ID map and parent-child relationships
	spanIDMap := make(map[string]bool)
	parentChildMap := make(map[string][]string) // parent -> children
	childParentMap := make(map[string]string)   // child -> parent
	var rootSpans []string
	var leafSpans []string
	
	for _, span := range trace.Spans {
		spanIDMap[span.SpanID] = true
		
		// Check if this is a root span (no CHILD_OF references)
		isRoot := true
		for _, ref := range span.References {
			if ref.RefType == "CHILD_OF" {
				isRoot = false
				parentChildMap[ref.SpanID] = append(parentChildMap[ref.SpanID], span.SpanID)
				childParentMap[span.SpanID] = ref.SpanID
			}
		}
		if isRoot {
			rootSpans = append(rootSpans, span.SpanID)
		}
	}
	
	// Find leaf spans (spans that are not referenced by any other span)
	for spanID := range spanIDMap {
		if len(parentChildMap[spanID]) == 0 && childParentMap[spanID] != "" {
			leafSpans = append(leafSpans, spanID)
		}
	}
	
	// Initialize eligible spans list
	var eligibleSpans []string
	
    // No relaxation: leaf spans are always protected and never eligible for removal
	
    // Identify eligible spans for removal (non-root, non-leaf, low-priority only)
	for spanID := range spanIDMap {
		isRoot := false
		isLeaf := false
		
		for _, rootID := range rootSpans {
			if spanID == rootID {
				isRoot = true
				break
			}
		}
		
		for _, leafID := range leafSpans {
			if spanID == leafID {
				isLeaf = true
				break
			}
		}
		
        if !isRoot && !isLeaf {
            // only low-priority spans are eligible
            for _, sp := range trace.Spans {
                if sp.SpanID == spanID && isLowPrioritySpan(sp) {
                    eligibleSpans = append(eligibleSpans, spanID)
                    break
                }
            }
		}
	}
	
	// Calculate number of spans to remove
	numToRemove := int(float64(len(eligibleSpans)) * lossPercentage)
	if numToRemove > len(eligibleSpans) {
		numToRemove = len(eligibleSpans)
	}
	
	// Randomly select spans to remove
	removedSpans := make(map[string]bool)
	for i := 0; i < numToRemove; i++ {
		idx := rand.Intn(len(eligibleSpans))
		removedSpans[eligibleSpans[idx]] = true
		// Remove from eligible list to avoid duplicates
		eligibleSpans = append(eligibleSpans[:idx], eligibleSpans[idx+1:]...)
	}
	
	// Add spans that are not removed
	for _, span := range trace.Spans {
		if !removedSpans[span.SpanID] {
			simulatedTrace.Spans = append(simulatedTrace.Spans, span)
		}
	}
	
	fmt.Printf("  SIMULATION - Removed %d non-root/non-leaf spans (%.1f%% of eligible) from trace %s\n", 
		numToRemove, lossPercentage*100, trace.TraceID)
	fmt.Printf("  SIMULATION - Root spans: %d, Leaf spans: %d, Eligible for removal: %d\n", 
		len(rootSpans), len(leafSpans), len(eligibleSpans))
	
	return simulatedTrace
}

// ForceDataLoss aggressively removes spans to guarantee data loss
func (loader *JaegerTraceLoader) ForceDataLoss(trace *JaegerTrace, lossPercentage float64) *JaegerTrace {
	// Create a copy of the trace
	simulatedTrace := &JaegerTrace{
		TraceID:   trace.TraceID, // Keep original trace ID
		Spans:     make([]JaegerSpan, 0, len(trace.Spans)),
		Processes: make(map[string]Process), // Will be populated with only used process IDs
		Warnings:  trace.Warnings,
	}
	
    // Build parent-child relationships and span index
	parentChildMap := make(map[string][]string) // parent -> children
	var rootSpans []string
    byID := make(map[string]JaegerSpan)
	
	for _, span := range trace.Spans {
        byID[span.SpanID] = span
		// Check if this is a root span (no CHILD_OF references)
		isRoot := true
		for _, ref := range span.References {
			if ref.RefType == "CHILD_OF" {
				isRoot = false
				parentChildMap[ref.SpanID] = append(parentChildMap[ref.SpanID], span.SpanID)
			}
		}
		if isRoot {
			rootSpans = append(rootSpans, span.SpanID)
		}
	}
	
    // Find spans that have children (middle spans) and are low priority only, excluding roots
    var middleSpans []string
    rootSet := make(map[string]bool)
    for _, r := range rootSpans { rootSet[r] = true }
    for spanID, children := range parentChildMap {
        if len(children) > 0 && !rootSet[spanID] {
            if sp, ok := byID[spanID]; ok && isLowPrioritySpan(sp) {
                middleSpans = append(middleSpans, spanID)
            }
        }
    }
	
	// Calculate number of middle spans to remove
	numToRemove := int(float64(len(middleSpans)) * lossPercentage)
	if numToRemove > len(middleSpans) {
		numToRemove = len(middleSpans)
	}
	if numToRemove == 0 && len(middleSpans) > 0 {
		numToRemove = 1 // Remove at least one middle span
	}
	
    // Randomly select low-priority middle spans to remove
	removedSpans := make(map[string]bool)
	for i := 0; i < numToRemove; i++ {
		idx := rand.Intn(len(middleSpans))
		removedSpans[middleSpans[idx]] = true
		// Remove from list to avoid duplicates
		middleSpans = append(middleSpans[:idx], middleSpans[idx+1:]...)
	}
	
	// Add spans that are not removed
	for _, span := range trace.Spans {
		if !removedSpans[span.SpanID] {
			simulatedTrace.Spans = append(simulatedTrace.Spans, span)
		}
	}
	
    // Populate processes map with only the process IDs used by remaining spans
	usedProcessIDs := make(map[string]bool)
	for _, span := range simulatedTrace.Spans {
		usedProcessIDs[span.ProcessID] = true
	}
	
	// Copy only the used processes from the original trace
	for processID := range usedProcessIDs {
		if process, exists := trace.Processes[processID]; exists {
			simulatedTrace.Processes[processID] = process
		}
	}
	
	fmt.Printf("  FORCED SIMULATION - Removed %d middle spans (%.1f%% of %d eligible middle spans) from trace %s\n", 
		numToRemove, lossPercentage*100, len(middleSpans), trace.TraceID)
	fmt.Printf("  FORCED SIMULATION - Root spans: %d, Eligible middle spans (low-priority, non-root, with children): %d\n", 
		len(rootSpans), len(middleSpans))
	
    return simulatedTrace
}

// ReconnectTrace attempts to reconnect orphaned spans using bloom filters
func (loader *JaegerTraceLoader) ReconnectTrace(trace *JaegerTrace) *ReconnectionResult {
	result := &ReconnectionResult{
		OriginalTrace: trace,
		BridgeEdges:   make([]BridgeEdge, 0),
		Success:       false,
	}
	
	// Create a copy of the trace for reconnection
	reconnectedTrace := &JaegerTrace{
		TraceID:   trace.TraceID, // Keep original trace ID
		Spans:     make([]JaegerSpan, len(trace.Spans)),
		Processes: make(map[string]Process), // Will be populated with only used process IDs
		Warnings:  trace.Warnings,
	}
	copy(reconnectedTrace.Spans, trace.Spans)
	
	// Populate processes map with only the process IDs used by spans
	usedProcessIDs := make(map[string]bool)
	for _, span := range reconnectedTrace.Spans {
		usedProcessIDs[span.ProcessID] = true
	}
	
	// Copy only the used processes from the original trace
	for processID := range usedProcessIDs {
		if process, exists := trace.Processes[processID]; exists {
			reconnectedTrace.Processes[processID] = process
		}
	}
	
	// Build span ID map and find root spans
	spanIDMap := make(map[string]bool)
	var rootSpans []string
	spanDepthMap := make(map[string]int)
	
	for _, span := range trace.Spans {
		spanIDMap[span.SpanID] = true
		
		// Check if this is a root span
		isRoot := true
		for _, ref := range span.References {
			if ref.RefType == "CHILD_OF" {
				isRoot = false
			}
		}
		if isRoot {
			rootSpans = append(rootSpans, span.SpanID)
		}
	}
	
	// Calculate depths using BFS from root spans
	// Treat CHILD_OF and FOLLOWS_FROM as equal for traversal
	queue := make([]string, 0)
	for _, rootID := range rootSpans {
		spanDepthMap[rootID] = 0
		queue = append(queue, rootID)
	}
	
	for len(queue) > 0 {
		currentID := queue[0]
		queue = queue[1:]
		currentDepth := spanDepthMap[currentID]
		
		// Find children of current span (both CHILD_OF and FOLLOWS_FROM)
		for _, span := range trace.Spans {
			for _, ref := range span.References {
				if (ref.RefType == "CHILD_OF" || ref.RefType == "FOLLOWS_FROM") && ref.SpanID == currentID {
					spanDepthMap[span.SpanID] = currentDepth + 1
					queue = append(queue, span.SpanID)
				}
			}
		}
	}
	
	// Build children index and span lookup for depth inference
	children := make(map[string][]string)
	byID := make(map[string]JaegerSpan)
	for _, s := range trace.Spans {
		byID[s.SpanID] = s
	}
	for _, s := range trace.Spans {
		for _, ref := range s.References {
			if ref.RefType == "CHILD_OF" {
				children[ref.SpanID] = append(children[ref.SpanID], s.SpanID)
			}
		}
	}
	
	// For orphans with missing parents, calculate depth from high-priority descendant's depth tag
	// Do multiple passes to handle dependencies (orphans with descendants that are also orphans)
	maxPasses := 3
	for pass := 0; pass < maxPasses; pass++ {
		updated := false
		for _, span := range trace.Spans {
			if _, hasDepth := spanDepthMap[span.SpanID]; !hasDepth {
			// Orphan without calculated depth - use depth tag from nearest high-priority descendant
			// Find nearest descendant with ancestry tags (high-priority span)
			descendant := findNearestDescendantWithAncestry(span.SpanID, children, byID)
			if descendant != nil {
				fmt.Printf("  DEBUG - Orphan %s: found descendant %s with ancestry tags\n", span.SpanID, descendant.SpanID)
				// Get depth tag from the high-priority descendant
				var descendantDepthTag int = -1
				for _, tag := range descendant.Tags {
					if tag.Key == "depth" {
						if str, ok := tag.Value.(string); ok {
							fmt.Sscanf(str, "%d", &descendantDepthTag)
							fmt.Printf("  DEBUG - Orphan %s: descendant %s has depth tag = %d\n", span.SpanID, descendant.SpanID, descendantDepthTag)
							break
						}
					}
				}
				
				if descendantDepthTag >= 0 {
					// The descendant's depth tag tells us its reset depth from its checkpoint (which is the orphan)
					// If descendant has depth_tag = N, that means there are N levels from the orphan (checkpoint) to the descendant
					// So: descendantAbsoluteDepth = orphanDepth + N
					// Therefore: orphanDepth = descendantAbsoluteDepth - N
					//
					// But we don't know descendantAbsoluteDepth yet (it's also an orphan).
					// However, we can check if the descendant's depth has been calculated already.
					// If it has, we can use it directly. Otherwise, we'll need to estimate.
					
					descendantAbsoluteDepth := -1
					if depth, exists := spanDepthMap[descendant.SpanID]; exists {
						descendantAbsoluteDepth = depth
					}
					
					if descendantAbsoluteDepth >= 0 {
						// We know the descendant's absolute depth, so we can calculate orphan depth directly
						orphanDepth := descendantAbsoluteDepth - descendantDepthTag
						if orphanDepth < 0 {
							orphanDepth = 1 // At least depth 1
						}
						spanDepthMap[span.SpanID] = orphanDepth
						updated = true
						fmt.Printf("  DEBUG - Orphan %s: inferred depth %d from descendant %s (descendant absolute depth=%d, descendant reset depth tag=%d)\n", 
							span.SpanID, orphanDepth, descendant.SpanID, descendantAbsoluteDepth, descendantDepthTag)
						continue
					} else {
						// Descendant's depth not calculated yet - use orphan's ancestry to find checkpoint and estimate
						checkpointDepth := -1
						
						// Get orphan's ancestry data (bloom filter) to find checkpoint
						mode, payload := getAncestryFromTags(span)
						if mode == "hybrid" || mode == "bloom" {
							bf, err := deserializeBloomFilter(payload)
							if err == nil {
								// Find deepest checkpoint in the orphan's bloom filter
								for existingSpanID := range spanIDMap {
									if bf.Test([]byte(existingSpanID)) {
										if currentSpan, exists := byID[existingSpanID]; exists {
											if isHighPrioritySpan(currentSpan) {
												if depth, exists := spanDepthMap[existingSpanID]; exists {
													if depth > checkpointDepth {
														checkpointDepth = depth
													}
												}
											}
										}
									}
								}
							}
						}
						
						if checkpointDepth >= 0 {
							// Estimate: the descendant's depth_tag tells us how many levels from orphan to descendant
							// If descendant has depth_tag = N, and we know orphan is somewhere after checkpoint,
							// we can estimate: orphanDepth = checkpointDepth + (some estimate based on depth_tag)
							// For now, use: orphanDepth = checkpointDepth + descendantDepthTag
							// This assumes the orphan is at the level where descendantDepthTag levels lead to the descendant
							orphanDepth := checkpointDepth + descendantDepthTag
							if orphanDepth < checkpointDepth + 1 {
								orphanDepth = checkpointDepth + 1
							}
							spanDepthMap[span.SpanID] = orphanDepth
							updated = true
							fmt.Printf("  DEBUG - Orphan %s: estimated depth %d from checkpoint %d and descendant depth tag %d (descendant depth not yet calculated)\n", 
								span.SpanID, orphanDepth, checkpointDepth, descendantDepthTag)
							continue
						}
					}
				} else {
					fmt.Printf("  DEBUG - Orphan %s: descendant %s has no depth tag\n", span.SpanID, descendant.SpanID)
				}
			} else {
				fmt.Printf("  DEBUG - Orphan %s: no descendant with ancestry tags found (children: %v)\n", span.SpanID, children[span.SpanID])
			}
			
			// Fallback: can't infer, default to 1 (assume it's a direct child of root)
			spanDepthMap[span.SpanID] = 1
			updated = true
			fmt.Printf("  DEBUG - Orphan %s: could not infer depth, defaulting to 1\n", span.SpanID)
			}
		}
		// If no updates in this pass, we're done
		if !updated {
			break
		}
	}
	
	// Find orphaned spans (spans with missing parents)
	// A span is orphaned if it has CHILD_OF or FOLLOWS_FROM references but none of them point to existing spans
	var orphanedSpans []JaegerSpan
	for _, span := range trace.Spans {
		hasValidParent := false
		for _, ref := range span.References {
			// Check both CHILD_OF and FOLLOWS_FROM as valid parent relationships
			if ref.RefType == "CHILD_OF" || ref.RefType == "FOLLOWS_FROM" {
				if spanIDMap[ref.SpanID] {
					hasValidParent = true
					break
				}
			}
		}
		if !hasValidParent && len(span.References) > 0 {
			orphanedSpans = append(orphanedSpans, span)
		}
	}
	
	fmt.Printf("  RECONNECTION - Found %d orphaned spans in trace %s\n", len(orphanedSpans), trace.TraceID)
	
	// Build children index for descendant search (already built above, but ensure it includes all reference types)
	for _, s := range trace.Spans {
		for _, ref := range s.References {
			if ref.RefType == "CHILD_OF" || ref.RefType == "FOLLOWS_FROM" {
				children[ref.SpanID] = append(children[ref.SpanID], s.SpanID)
			}
		}
	}

    // Determine trace's ancestry_mode for fallback
    traceMode := getTraceAncestryMode(trace)
    
    // For hybrid mode: track synthetic nodes by (ancestorID, depth) and store orphan info for merging
    type syntheticNodeInfo struct {
        spanID      string
        ancestorID  string
        depth       int
        isFromHash  bool // true if this node was created from a hash entry
    }
    syntheticNodesByKey := make(map[string]*syntheticNodeInfo) // key: "ancestorID:depth" -> node info
    type orphanInfo struct {
        span        JaegerSpan
        bloomFilter *bloom.BloomFilter
        hashArray   string
        ancestorID  string
        orphanDepth int
    }
    hybridOrphans := make([]orphanInfo, 0)
    
    // Attempt to reconnect each orphaned span
    reconnectedCount := 0
    for _, orphanedSpan := range orphanedSpans {
        fmt.Printf("  RECONNECTION - Orphan %s: attempting ancestry extraction from ancestry/ancestry_mode tags\n", orphanedSpan.SpanID)
        
        // Try to get ancestry from the orphan's own tags
        mode, payload := getAncestryFromTags(orphanedSpan)
        
        // If orphan doesn't have ancestry data, find nearest descendant
        if payload == "" {
            fmt.Printf("    RECONNECTION - Orphan %s: no ancestry data found, searching for nearest descendant...\n", orphanedSpan.SpanID)
            descendant := findNearestDescendantWithAncestry(orphanedSpan.SpanID, children, byID)
            if descendant == nil {
                // Fallback: use __bag.* attributes on orphan based on trace's ancestry_mode
                if traceMode != "" {
                    fmt.Printf("    RECONNECTION - Orphan %s: no descendant found, falling back to __bag.* (trace mode=%s)\n", orphanedSpan.SpanID, traceMode)
                    payload = getBaggageAncestryByMode(orphanedSpan, traceMode)
                    if payload != "" {
                        mode = traceMode
                        fmt.Printf("    RECONNECTION - Orphan %s: using __bag.* fallback (mode=%s)\n", orphanedSpan.SpanID, mode)
                    } else {
                        fmt.Printf("    RECONNECTION - Orphan %s: no __bag.* fallback available, skipping\n", orphanedSpan.SpanID)
                        continue
                    }
                } else {
                    fmt.Printf("    RECONNECTION - Orphan %s: no descendant with ancestry data found and no trace mode for fallback, skipping\n", orphanedSpan.SpanID)
                    continue
                }
            } else {
                mode, payload = getAncestryFromTags(*descendant)
                fmt.Printf("    RECONNECTION - Orphan %s: using ancestry from descendant %s (mode=%s)\n", orphanedSpan.SpanID, descendant.SpanID, mode)
            }
        } else {
            fmt.Printf("    RECONNECTION - Orphan %s: using own ancestry data (mode=%s)\n", orphanedSpan.SpanID, mode)
        }
        
        if payload == "" || mode == "" {
            fmt.Printf("    RECONNECTION - Orphan %s: ancestry data incomplete (mode=%s, payload empty=%v), skipping\n", orphanedSpan.SpanID, mode, payload == "")
            continue
        }
		if mode == "bloom" {
            bf, err := deserializeBloomFilter(payload)
			if err != nil {
                fmt.Printf("    RECONNECTION - Orphan %s: failed to deserialize bloom filter: %v\n", orphanedSpan.SpanID, err)
				continue
			}

			// Find the missing parent ID
			// Check CHILD_OF first, then FOLLOWS_FROM as fallback
			var missingParentID string
			for _, ref := range orphanedSpan.References {
				if ref.RefType == "CHILD_OF" && !spanIDMap[ref.SpanID] {
					missingParentID = ref.SpanID
					break
				}
			}
			// If no CHILD_OF missing parent, check FOLLOWS_FROM
			if missingParentID == "" {
				for _, ref := range orphanedSpan.References {
					if ref.RefType == "FOLLOWS_FROM" && !spanIDMap[ref.SpanID] {
						missingParentID = ref.SpanID
						break
					}
				}
			}
            if missingParentID == "" {
                fmt.Printf("    RECONNECTION - Orphan %s: could not determine missing parent ID from references\n", orphanedSpan.SpanID)
                continue
            }
			var bestAncestor string
			var bestDepth = -1
			var bestConfidence float64
			for existingSpanID := range spanIDMap {
				if bf.Test([]byte(existingSpanID)) {
					depth := spanDepthMap[existingSpanID]
					if depth > bestDepth {
						bestDepth = depth
						bestAncestor = existingSpanID
						bestConfidence = loader.calculateReconnectionConfidence(bf, existingSpanID, orphanedSpan.SpanID)
					}
				}
			}
			if bestAncestor != "" {
				fmt.Printf("    RECONNECTION - Orphan %s: selected ancestor %s (depth=%d, conf=%.3f)\n", orphanedSpan.SpanID, bestAncestor, bestDepth, bestConfidence)
				bridgeEdge := BridgeEdge{FromSpanID: bestAncestor, ToSpanID: orphanedSpan.SpanID, OriginalParent: missingParentID, Depth: spanDepthMap[orphanedSpan.SpanID], Confidence: bestConfidence, SyntheticSpansCreated: 0}
				result.BridgeEdges = append(result.BridgeEdges, bridgeEdge)
				for i := range reconnectedTrace.Spans {
					if reconnectedTrace.Spans[i].SpanID == orphanedSpan.SpanID {
						for j := range reconnectedTrace.Spans[i].References {
							// Update both CHILD_OF and FOLLOWS_FROM references
							if (reconnectedTrace.Spans[i].References[j].RefType == "CHILD_OF" || reconnectedTrace.Spans[i].References[j].RefType == "FOLLOWS_FROM") && reconnectedTrace.Spans[i].References[j].SpanID == missingParentID {
								reconnectedTrace.Spans[i].References[j].SpanID = bestAncestor
							}
						}
						reconnectedTrace.Spans[i].Tags = append(reconnectedTrace.Spans[i].Tags, Tag{Key: "bridge.reconnected", Value: true, Type: "bool"})
						reconnectedTrace.Spans[i].Tags = append(reconnectedTrace.Spans[i].Tags, Tag{Key: "bridge.original_parent", Value: missingParentID, Type: "string"})
						reconnectedTrace.Spans[i].Tags = append(reconnectedTrace.Spans[i].Tags, Tag{Key: "bridge.confidence", Value: bestConfidence, Type: "float64"})
						break
					}
				}
                reconnectedCount++
            } else {
                fmt.Printf("    RECONNECTION - Orphan %s: no candidate ancestor from bloom filter test\n", orphanedSpan.SpanID)
            }
			continue
		}

        if mode == "hybrid" {
            // Hybrid mode reconstruction algorithm:
            // 1. Find nearest collected ancestor using bloom filter
            // 2. Find nearest high-priority descendant to get depth tag
            // 3. Find nearest high-priority ancestor (checkpoint)
            // 4. Calculate missing nodes using depth tag
            // 5. Create synthetic nodes (will be labeled with hash entries in second pass)
            
            bf, err := deserializeBloomFilter(payload)
            if err != nil {
                fmt.Printf("    RECONNECTION - Orphan %s: failed to deserialize bloom filter: %v\n", orphanedSpan.SpanID, err)
                continue
            }

            // Step 1: Find nearest collected ancestor using bloom filter
            var bestAncestor string
            var bestDepth = -1
            for existingSpanID := range spanIDMap {
                if bf.Test([]byte(existingSpanID)) {
                    depth := spanDepthMap[existingSpanID]
                    if depth > bestDepth {
                        bestDepth = depth
                        bestAncestor = existingSpanID
                    }
                }
            }
            
            if bestAncestor == "" {
                fmt.Printf("    RECONNECTION - Orphan %s: no candidate ancestor from bloom filter test\n", orphanedSpan.SpanID)
                continue
            }
            
            // Step 2: Find nearest high-priority descendant to get depth tag
            // If orphan is itself high-priority, use its own depth tag
            var orphanDepthTag int = -1
            for _, tag := range orphanedSpan.Tags {
                if tag.Key == "depth" {
                    if str, ok := tag.Value.(string); ok {
                        fmt.Sscanf(str, "%d", &orphanDepthTag)
                        break
                    }
                }
            }
            
            descendant := findNearestDescendantWithAncestry(orphanedSpan.SpanID, children, byID)
            var descendantDepthTag int = -1
            var descendantHashArray string = ""
            if descendant != nil {
                // Get depth tag and hash array from descendant
                for _, tag := range descendant.Tags {
                    if tag.Key == "depth" {
                        if str, ok := tag.Value.(string); ok {
                            fmt.Sscanf(str, "%d", &descendantDepthTag)
                        }
                    } else if tag.Key == "hash" {
                        if str, ok := tag.Value.(string); ok {
                            descendantHashArray = str
                        }
                    }
                }
            }
            
            // Use orphan's own depth tag if available, otherwise use descendant's
            if orphanDepthTag >= 0 {
                descendantDepthTag = orphanDepthTag
                fmt.Printf("    RECONNECTION - Orphan %s: using own depth tag=%d\n", orphanedSpan.SpanID, orphanDepthTag)
            }
            
            // If orphan has its own hash array, use it; otherwise borrow from descendant
            orphanHashArray := getHashFromTags(orphanedSpan)
            if orphanHashArray == "" && descendantHashArray != "" {
                orphanHashArray = descendantHashArray
                fmt.Printf("    RECONNECTION - Orphan %s: borrowing hash array from descendant\n", orphanedSpan.SpanID)
            }
            
            // Step 3: Find nearest high-priority ancestor (checkpoint) by traversing up from bestAncestor
            checkpointDepth := -1
            checkpointID := ""
            currentSpanID := bestAncestor
            visited := make(map[string]bool)
            
            for currentSpanID != "" && !visited[currentSpanID] {
                visited[currentSpanID] = true
                if currentSpan, exists := byID[currentSpanID]; exists {
                    // Check if this span is a checkpoint (high priority)
                    if isHighPrioritySpan(currentSpan) {
                        checkpointDepth = spanDepthMap[currentSpanID]
                        checkpointID = currentSpanID
                        break
                    }
                    // Move to parent
                    currentSpanID = ""
                    for _, ref := range currentSpan.References {
                        if ref.RefType == "CHILD_OF" || ref.RefType == "FOLLOWS_FROM" {
                            if spanIDMap[ref.SpanID] {
                                currentSpanID = ref.SpanID
                                break
                            }
                        }
                    }
                } else {
                    break
                }
            }
            
            // If no checkpoint found, use bestAncestor as checkpoint
            if checkpointDepth == -1 {
                checkpointDepth = bestDepth
                checkpointID = bestAncestor
            }
            
            // Step 4: Calculate number of missing nodes using depth tag
            // Count: (checkpoint to bestAncestor) + (descendant to orphan)
            // The discrepancy from depth tag tells us how many unknown nodes to add
            nodesFromCheckpointToAncestor := bestDepth - checkpointDepth
            nodesFromDescendantToOrphan := 0
            if descendantDepthTag >= 0 {
                // The descendant's depth tag tells us its reset depth from checkpoint
                // If descendant is at depth tag = N, and orphan is ancestor of descendant,
                // we need to estimate nodes from descendant to orphan
                // For now, use: nodesFromDescendantToOrphan = 0 (orphan is direct ancestor of descendant)
                // Actually, we can't know this exactly without more info
                nodesFromDescendantToOrphan = 0
            }
            
            // Total nodes from checkpoint to orphan (via collected ancestor)
            totalNodesFromCheckpoint := nodesFromCheckpointToAncestor + nodesFromDescendantToOrphan
            expectedNodesFromDepthTag := descendantDepthTag
            if expectedNodesFromDepthTag < 0 {
                expectedNodesFromDepthTag = 0
            }
            
            // The discrepancy is the number of unknown nodes to add
            missingNodes := expectedNodesFromDepthTag - totalNodesFromCheckpoint
            if missingNodes < 0 {
                missingNodes = 0
            }
            
            fmt.Printf("    RECONNECTION - Orphan %s: checkpoint=%s (depth=%d), ancestor=%s (depth=%d), descendant depth tag=%d, missing nodes=%d\n", 
                orphanedSpan.SpanID, checkpointID, checkpointDepth, bestAncestor, bestDepth, descendantDepthTag, missingNodes)
            
            // Store orphan info for second pass (hash labeling and merging)
            hybridOrphans = append(hybridOrphans, orphanInfo{
                span:        orphanedSpan,
                bloomFilter: bf,
                hashArray:   orphanHashArray,
                ancestorID:  bestAncestor,
                orphanDepth: spanDepthMap[orphanedSpan.SpanID],
            })
            
            // Create synthetic nodes for missing depths (starting from checkpoint)
            // These will be labeled with hash entries in second pass
            lastParentID := bestAncestor
            syntheticCount := 0
            
            // Calculate orphan depth: checkpointDepth + descendantDepthTag
            // The descendantDepthTag tells us how many levels from checkpoint to orphan
            orphanDepth := checkpointDepth + descendantDepthTag
            if orphanDepth <= bestDepth {
                // Fallback: use spanDepthMap if available
                if depth, exists := spanDepthMap[orphanedSpan.SpanID]; exists && depth > bestDepth {
                    orphanDepth = depth
                } else {
                    orphanDepth = bestDepth + missingNodes + 1
                }
            }
            
            fmt.Printf("    RECONNECTION - Orphan %s: creating synthetic nodes from depth %d to %d (orphan depth=%d)\n", 
                orphanedSpan.SpanID, bestDepth + 1, orphanDepth - 1, orphanDepth)
            
            for d := bestDepth + 1; d < orphanDepth; d++ {
                // Create placeholder ID (will be labeled with hash in second pass)
                synthID := fmt.Sprintf("synth_%s_%d", orphanedSpan.SpanID, d)
                
                // Check if synthetic node already exists at this (ancestor, depth)
                // Only reuse if the existing node's spanID is in this orphan's bloom filter
                nodeKey := fmt.Sprintf("%s:%d", bestAncestor, d)
                if existingNode, exists := syntheticNodesByKey[nodeKey]; exists {
                    // Check if existing node's spanID is in this orphan's bloom filter
                    // If yes, reuse it; if no, create a new one (will be merged later if needed)
                    if bf.Test([]byte(existingNode.spanID)) {
                        lastParentID = existingNode.spanID
                        fmt.Printf("    RECONNECTION - Orphan %s: using existing synthetic node %s at depth %d (in bloom filter)\n", 
                            orphanedSpan.SpanID, existingNode.spanID, d)
                        continue
                    } else {
                        // Existing node is not in bloom filter, create new one with different key
                        nodeKey = fmt.Sprintf("%s:%d:%s", bestAncestor, d, orphanedSpan.SpanID)
                        fmt.Printf("    RECONNECTION - Orphan %s: existing node %s at depth %d not in bloom filter, creating new node\n", 
                            orphanedSpan.SpanID, existingNode.spanID, d)
                    }
                }
                
                // Create new synthetic node
                unknownPID := "p_unknown"
                if _, ok := reconnectedTrace.Processes[unknownPID]; !ok {
                    reconnectedTrace.Processes[unknownPID] = Process{ServiceName: "unknown_service:reconstructed", Tags: nil}
                }
                
                synth := JaegerSpan{
                    TraceID:       trace.TraceID,
                    SpanID:        synthID,
                    ProcessID:     unknownPID,
                    OperationName: "unknown",
                    StartTime:     orphanedSpan.StartTime - 1000,
                    Duration:      1000,
                    Tags:          []Tag{{Key: "bridge.synthetic", Value: true, Type: "bool"}},
                    References:    []Reference{{RefType: "CHILD_OF", TraceID: trace.TraceID, SpanID: lastParentID}},
                    Flags:         0,
                }
                reconnectedTrace.Spans = append(reconnectedTrace.Spans, synth)
                spanIDMap[synthID] = true
                byID[synthID] = synth
                spanDepthMap[synthID] = d
                syntheticCount++
                
                // Track synthetic node
                syntheticNodesByKey[nodeKey] = &syntheticNodeInfo{
                    spanID:     synthID,
                    ancestorID: bestAncestor,
                    depth:      d,
                    isFromHash: false, // Will be set in second pass
                }
                
                fmt.Printf("    RECONNECTION - Orphan %s: created synthetic node %s at depth %d\n", 
                    orphanedSpan.SpanID, synthID, d)
                
                lastParentID = synthID
            }
            
            // Connect orphan to last parent
            missingParentID := ""
            for _, ref := range orphanedSpan.References {
                if (ref.RefType == "CHILD_OF" || ref.RefType == "FOLLOWS_FROM") && !spanIDMap[ref.SpanID] {
                    missingParentID = ref.SpanID
                    break
                }
            }
            
            for i := range reconnectedTrace.Spans {
                if reconnectedTrace.Spans[i].SpanID == orphanedSpan.SpanID {
                    for j := range reconnectedTrace.Spans[i].References {
                        if (reconnectedTrace.Spans[i].References[j].RefType == "CHILD_OF" || reconnectedTrace.Spans[i].References[j].RefType == "FOLLOWS_FROM") && 
                           (missingParentID == "" || reconnectedTrace.Spans[i].References[j].SpanID == missingParentID) {
                            reconnectedTrace.Spans[i].References[j].SpanID = lastParentID
                        }
                    }
                    reconnectedTrace.Spans[i].Tags = append(reconnectedTrace.Spans[i].Tags, Tag{Key: "bridge.reconnected", Value: true, Type: "bool"})
                    if missingParentID != "" {
                        reconnectedTrace.Spans[i].Tags = append(reconnectedTrace.Spans[i].Tags, Tag{Key: "bridge.original_parent", Value: missingParentID, Type: "string"})
                    }
                    reconnectedTrace.Spans[i].Tags = append(reconnectedTrace.Spans[i].Tags, Tag{Key: "bridge.confidence", Value: 0.8, Type: "float64"})
                    break
                }
            }
            
            bridgeEdge := BridgeEdge{
                FromSpanID:          lastParentID,
                ToSpanID:            orphanedSpan.SpanID,
                OriginalParent:      missingParentID,
                Depth:               orphanDepth,
                Confidence:          0.8,
                SyntheticSpansCreated: syntheticCount,
            }
            result.BridgeEdges = append(result.BridgeEdges, bridgeEdge)
            reconnectedCount++
            continue
        }

        if mode == "hash" {
			// Parse ancestry array
            // Ancestry format: [nearest_high_priority_ancestor, ..., parent]
            // Does NOT include the current span
            parts := strings.Split(payload, ",")
            fmt.Printf("    RECONNECTION - Orphan %s: hash parts=%v\n", orphanedSpan.SpanID, parts)
            
            if len(parts) == 0 {
                fmt.Printf("    RECONNECTION - Orphan %s: empty ancestry array; skipping\n", orphanedSpan.SpanID)
                continue
            }
            
            // The parent is always the last element in ancestry (since current span is not included)
            parentChain := parts // entire ancestry chain
            // Find deepest existing ancestor in chain
            anchorIdx := -1
            for i := len(parentChain) - 1; i >= 0; i-- {
                if spanIDMap[parentChain[i]] {
                    anchorIdx = i
                    break
                }
            }
            fmt.Printf("    RECONNECTION - Orphan %s: anchorIdx=%d anchorSpan=%s\n", orphanedSpan.SpanID, anchorIdx, func() string { if anchorIdx>=0 { return parentChain[anchorIdx] } ; return "" }())
            
			// Determine original missing parent before adding synthetic spans
			originalSpanSet := make(map[string]bool, len(spanIDMap))
			for k, v := range spanIDMap { originalSpanSet[k] = v }
			originalParent := ""
			// Check CHILD_OF first, then FOLLOWS_FROM as fallback
			for _, ref := range orphanedSpan.References {
				if ref.RefType == "CHILD_OF" && !originalSpanSet[ref.SpanID] {
					originalParent = ref.SpanID
					break
				}
			}
			// If no CHILD_OF missing parent, check FOLLOWS_FROM
			if originalParent == "" {
				for _, ref := range orphanedSpan.References {
					if ref.RefType == "FOLLOWS_FROM" && !originalSpanSet[ref.SpanID] {
						originalParent = ref.SpanID
						break
					}
				}
			}
            // If originalParent not found in references, derive it from ancestry chain
            // Parent is the last element in ancestry (since current span is not included)
            if originalParent == "" && len(parts) > 0 {
                candidateParent := parts[len(parts)-1]
                if !originalSpanSet[candidateParent] {
                    originalParent = candidateParent
                    fmt.Printf("    RECONNECTION - Orphan %s: inferred missing parent %s from ancestry chain (not in references)\n", orphanedSpan.SpanID, originalParent)
                } else {
                    // Parent already exists - might have been synthesized by a previous orphan
                    // In this case, we can still reconnect if we have a valid anchor
                    fmt.Printf("    RECONNECTION - Orphan %s: parent %s already exists (likely synthesized), using ancestry chain for reconnection\n", orphanedSpan.SpanID, candidateParent)
                    originalParent = candidateParent // Use it for tracking, even though it exists
                }
            }
            if originalParent == "" {
                fmt.Printf("    RECONNECTION - Orphan %s: could not determine original missing parent; skipping\n", orphanedSpan.SpanID)
                continue
            }
			// Ensure unknown process exists
			unknownPID := "p_unknown"
			if _, ok := reconnectedTrace.Processes[unknownPID]; !ok {
				reconnectedTrace.Processes[unknownPID] = Process{ServiceName: "unknown_service:reconstructed", Tags: nil}
			}
			// Create missing spans between anchor and orphan's parent
			var lastParentID string
			if anchorIdx >= 0 {
				lastParentID = parentChain[anchorIdx]
			} else {
				// If none exists, attach under root if available in trace, else skip
				// Prefer the first element as the root from ancestry
				if spanIDMap[parentChain[0]] {
					lastParentID = parentChain[0]
					anchorIdx = 0
				} else {
					continue
				}
			}
			
			// Check if originalParent already exists (was synthesized by a previous orphan)
			parentAlreadyExists := spanIDMap[originalParent]
			
			// Determine which spans need to be synthesized
			// parentChain is the entire ancestry (parts), so originalParent should be at parentChain[len-1]
			parentIdxInChain := len(parentChain) - 1
			var missing []string
			
			if parentAlreadyExists && parentIdxInChain >= anchorIdx {
				// Parent already exists and is between anchor and orphan
				// Only synthesize spans between anchor and parent (not including parent)
				if parentIdxInChain > anchorIdx {
					missing = parentChain[anchorIdx+1:parentIdxInChain]
					lastParentID = originalParent // Use existing parent
				} else {
					// Parent is the anchor, no synthesis needed
					missing = []string{}
					lastParentID = originalParent
				}
			} else {
				// Parent doesn't exist yet, synthesize all missing spans including parent
				missing = parentChain[anchorIdx+1:]
				// Ensure originalParent is in missing if it's not already there
				if len(missing) == 0 || missing[len(missing)-1] != originalParent {
					missing = append(missing, originalParent)
				}
			}
			
			syntheticCount := len(missing)
            if syntheticCount > 0 {
                fmt.Printf("    RECONNECTION - Orphan %s: synthesizing %d missing ancestors between %s and orphan\n", orphanedSpan.SpanID, syntheticCount, lastParentID)
            } else if parentAlreadyExists {
                fmt.Printf("    RECONNECTION - Orphan %s: parent %s already exists, updating reference directly\n", orphanedSpan.SpanID, originalParent)
            }
			// Create synthetic spans in order
			for _, synthID := range missing {
				synth := JaegerSpan{
					TraceID:       trace.TraceID,
					SpanID:        synthID,
					ProcessID:     unknownPID,
					OperationName: "unknown",
					StartTime:     orphanedSpan.StartTime - 1000, // 1µs before child end (A strategy)
					Duration:      1000,
					Tags:          []Tag{{Key: "bridge.synthetic", Value: true, Type: "bool"}},
					References:    []Reference{{RefType: "CHILD_OF", TraceID: trace.TraceID, SpanID: lastParentID}},
					Flags:         0,
				}
				reconnectedTrace.Spans = append(reconnectedTrace.Spans, synth)
				spanIDMap[synthID] = true
				byID[synthID] = synth
				lastParentID = synthID
			}
			// Update orphan to point to lastParentID (parent)
            fmt.Printf("    RECONNECTION - Orphan %s: original missing parent was %s; reattaching under %s\n", orphanedSpan.SpanID, originalParent, lastParentID)
			for i := range reconnectedTrace.Spans {
				if reconnectedTrace.Spans[i].SpanID == orphanedSpan.SpanID {
					for j := range reconnectedTrace.Spans[i].References {
						// Update both CHILD_OF and FOLLOWS_FROM references
						if (reconnectedTrace.Spans[i].References[j].RefType == "CHILD_OF" || reconnectedTrace.Spans[i].References[j].RefType == "FOLLOWS_FROM") && reconnectedTrace.Spans[i].References[j].SpanID == originalParent {
							reconnectedTrace.Spans[i].References[j].SpanID = lastParentID
						}
					}
					reconnectedTrace.Spans[i].Tags = append(reconnectedTrace.Spans[i].Tags, Tag{Key: "bridge.reconnected", Value: true, Type: "bool"})
					reconnectedTrace.Spans[i].Tags = append(reconnectedTrace.Spans[i].Tags, Tag{Key: "bridge.original_parent", Value: originalParent, Type: "string"})
					reconnectedTrace.Spans[i].Tags = append(reconnectedTrace.Spans[i].Tags, Tag{Key: "bridge.confidence", Value: 0.7, Type: "float64"})
					break
				}
			}
			result.BridgeEdges = append(result.BridgeEdges, BridgeEdge{FromSpanID: lastParentID, ToSpanID: orphanedSpan.SpanID, OriginalParent: originalParent, Depth: spanDepthMap[orphanedSpan.SpanID], Confidence: 0.7, SyntheticSpansCreated: syntheticCount})
			reconnectedCount++
			continue
		}
	}
	
	// Second pass for hybrid mode: Label nodes with hash entries, then merge using BFS
	if len(hybridOrphans) > 0 {
		fmt.Printf("\n  RECONNECTION - Second pass: Labeling synthetic nodes with hash arrays\n")
		
		// Step 1: Annotate all nodes with hashed spanID from hash arrays
		// Hash depth is interpreted relative to nearest high-priority ancestor (checkpoint)
		for _, orphanInfo := range hybridOrphans {
			if orphanInfo.hashArray == "" {
				continue // No hash array, skip labeling
			}
			
			// Find nearest high-priority ancestor (checkpoint) for this orphan
			checkpointDepth := -1
			currentSpanID := orphanInfo.ancestorID
			visited := make(map[string]bool)
			
			for currentSpanID != "" && !visited[currentSpanID] {
				visited[currentSpanID] = true
				if currentSpan, exists := byID[currentSpanID]; exists {
					if isHighPrioritySpan(currentSpan) {
						checkpointDepth = spanDepthMap[currentSpanID]
						break
					}
					// Move to parent
					currentSpanID = ""
					for _, ref := range currentSpan.References {
						if ref.RefType == "CHILD_OF" || ref.RefType == "FOLLOWS_FROM" {
							if spanIDMap[ref.SpanID] {
								currentSpanID = ref.SpanID
								break
							}
						}
					}
				} else {
					break
				}
			}
			
			if checkpointDepth == -1 {
				checkpointDepth = spanDepthMap[orphanInfo.ancestorID]
			}
			
			// Parse hash entries
			hashParts := strings.Split(orphanInfo.hashArray, ",")
			for _, part := range hashParts {
				part = strings.TrimSpace(part)
				if part == "" {
					continue
				}
				
				hashSpanID, hashDepth, err := parseHashEntry(part)
				if err != nil {
					fmt.Printf("    RECONNECTION - Label: failed to parse hash entry %s: %v\n", part, err)
					continue
				}
				
				// Hash depth is relative to checkpoint, so absolute depth = checkpointDepth + hashDepth
				absoluteDepth := checkpointDepth + hashDepth
				
				// Find synthetic node at this (ancestor, absolute depth)
				nodeKey := fmt.Sprintf("%s:%d", orphanInfo.ancestorID, absoluteDepth)
				syntheticNode, exists := syntheticNodesByKey[nodeKey]
				
				if !exists {
					// Try to find by depth only (might be from different ancestor path)
					for key, node := range syntheticNodesByKey {
						if node.depth == absoluteDepth {
							syntheticNode = node
							nodeKey = key
							exists = true
							break
						}
					}
				}
				
				if !exists {
					fmt.Printf("    RECONNECTION - Label: no synthetic node found at depth %d (checkpoint=%d + hash=%d) for orphan %s\n", 
						absoluteDepth, checkpointDepth, hashDepth, orphanInfo.span.SpanID)
					continue
				}
				
				// Label the synthetic node with hash spanID
				if syntheticNode.spanID != hashSpanID {
					fmt.Printf("    RECONNECTION - Label: labeling synthetic node %s at depth %d with hash spanID %s\n", 
						syntheticNode.spanID, absoluteDepth, hashSpanID)
					
					// Update the synthetic node tracking
					oldSpanID := syntheticNode.spanID
					syntheticNode.spanID = hashSpanID
					syntheticNode.isFromHash = true
					
					// Update the span in reconnectedTrace
					for i := range reconnectedTrace.Spans {
						if reconnectedTrace.Spans[i].SpanID == oldSpanID {
							reconnectedTrace.Spans[i].SpanID = hashSpanID
							// Update maps
							delete(spanIDMap, oldSpanID)
							spanIDMap[hashSpanID] = true
							byID[hashSpanID] = reconnectedTrace.Spans[i]
							delete(byID, oldSpanID)
							spanDepthMap[hashSpanID] = absoluteDepth
							break
						}
					}
					
					// Update all references
					for i := range reconnectedTrace.Spans {
						for j := range reconnectedTrace.Spans[i].References {
							if reconnectedTrace.Spans[i].References[j].SpanID == oldSpanID {
								reconnectedTrace.Spans[i].References[j].SpanID = hashSpanID
							}
						}
					}
					
					// Update bridge edges
					for i := range result.BridgeEdges {
						if result.BridgeEdges[i].FromSpanID == oldSpanID {
							result.BridgeEdges[i].FromSpanID = hashSpanID
						}
						if result.BridgeEdges[i].ToSpanID == oldSpanID {
							result.BridgeEdges[i].ToSpanID = hashSpanID
						}
					}
				}
			}
		}
		
		// Step 2: BFS merge traversal - look for labeled synthetic nodes and merge with siblings
		fmt.Printf("\n  RECONNECTION - Third pass: BFS merge traversal\n")
		
		// Build parent-child map for BFS
		parentToChildren := make(map[string][]string)
		childToParent := make(map[string]string)
		for _, span := range reconnectedTrace.Spans {
			for _, ref := range span.References {
				if ref.RefType == "CHILD_OF" || ref.RefType == "FOLLOWS_FROM" {
					parentToChildren[ref.SpanID] = append(parentToChildren[ref.SpanID], span.SpanID)
					childToParent[span.SpanID] = ref.SpanID
				}
			}
		}
		
		fmt.Printf("  RECONNECTION - BFS: Built parent-child map with %d parents\n", len(parentToChildren))
		
		// Find root spans for BFS
		visited := make(map[string]bool)
		var roots []string
		for _, span := range reconnectedTrace.Spans {
			isRoot := true
			for _, ref := range span.References {
				if ref.RefType == "CHILD_OF" || ref.RefType == "FOLLOWS_FROM" {
					isRoot = false
					break
				}
			}
			if isRoot {
				roots = append(roots, span.SpanID)
			}
		}
		
		fmt.Printf("  RECONNECTION - BFS: Found %d root(s): %v\n", len(roots), roots)
		
		// BFS traversal
		queue := make([]string, 0)
		for _, root := range roots {
			queue = append(queue, root)
			visited[root] = true
		}
		
		processedCount := 0
		for len(queue) > 0 {
			currentID := queue[0]
			queue = queue[1:]
			processedCount++
			
			// Check if current node is a labeled synthetic node (from hash)
			currentSpan := byID[currentID]
			if currentSpan.SpanID == "" {
				fmt.Printf("  RECONNECTION - BFS: Skipping %s (not in byID)\n", currentID)
				continue
			}
			
			currentDepth := spanDepthMap[currentID]
			isLabeledSynthetic := false
			for _, tag := range currentSpan.Tags {
				if tag.Key == "bridge.synthetic" {
					isLabeledSynthetic = true
					break
				}
			}
			
			fmt.Printf("  RECONNECTION - BFS: Processing %s (depth=%d, synthetic=%v, isLabeled=%v, isPlaceholder=%v)\n", 
				currentID, currentDepth, isLabeledSynthetic, isLabeledSynthetic && !strings.HasPrefix(currentID, "synth_"), strings.HasPrefix(currentID, "synth_"))
			
			// Check if this is a synthetic node (labeled or placeholder)
			if isLabeledSynthetic {
				isPlaceholder := strings.HasPrefix(currentID, "synth_")
				if !isPlaceholder {
					fmt.Printf("  RECONNECTION - BFS: %s is a labeled synthetic node, looking for siblings\n", currentID)
					// This is a labeled synthetic node - look for siblings to merge
					parentID := childToParent[currentID]
					if parentID != "" {
						fmt.Printf("  RECONNECTION - BFS: %s has parent %s, checking siblings\n", currentID, parentID)
						// Get siblings (children of parent) - need to get fresh list in case parentToChildren was updated
						siblings := make([]string, 0)
						for _, childID := range parentToChildren[parentID] {
							siblings = append(siblings, childID)
						}
						fmt.Printf("  RECONNECTION - BFS: Found %d siblings of %s: %v\n", len(siblings), currentID, siblings)
					
					for _, siblingID := range siblings {
						if siblingID == currentID {
							continue // Skip self
						}
						
						// Check if siblingID was already merged (deleted from byID)
						actualSiblingID := siblingID
						if _, exists := byID[siblingID]; !exists {
							// Sibling was merged - check if it was merged into currentID
							if spanDepthMap[currentID] == spanDepthMap[siblingID] {
								// Same depth, might have been merged
								fmt.Printf("  RECONNECTION - BFS: Sibling %s was already merged, checking if it's now %s\n", siblingID, currentID)
								// Check if any references point from siblingID to currentID (meaning they were merged)
								merged := false
								for _, span := range reconnectedTrace.Spans {
									for _, ref := range span.References {
										if ref.SpanID == currentID {
											// Check if this span was originally connected to siblingID
											// by looking at the span's current parent
											if span.SpanID != currentID && span.SpanID != siblingID {
												merged = true
												break
											}
										}
									}
									if merged {
										break
									}
								}
								if merged {
									fmt.Printf("  RECONNECTION - BFS: Sibling %s was already merged into %s, skipping\n", siblingID, currentID)
									continue
								}
							}
							// Sibling was deleted but not merged into currentID - skip it
							fmt.Printf("  RECONNECTION - BFS: Skipping sibling %s (not in byID, may have been merged elsewhere)\n", siblingID)
							continue
						}
						
						siblingSpan := byID[actualSiblingID]
						if siblingSpan.SpanID == "" {
							fmt.Printf("  RECONNECTION - BFS: Skipping sibling %s (span has empty SpanID)\n", actualSiblingID)
							continue
						}
						
						siblingDepth := spanDepthMap[siblingID]
						// Check if sibling is also a synthetic node at the same depth
						isSiblingSynthetic := false
						for _, tag := range siblingSpan.Tags {
							if tag.Key == "bridge.synthetic" {
								isSiblingSynthetic = true
								break
							}
						}
						
						fmt.Printf("  RECONNECTION - BFS: Checking sibling %s (depth=%d, synthetic=%v)\n", 
							siblingID, siblingDepth, isSiblingSynthetic)
						
						if isSiblingSynthetic && siblingDepth == currentDepth {
							fmt.Printf("  RECONNECTION - BFS: Sibling %s is synthetic at same depth %d, checking bloom filters\n", 
								siblingID, siblingDepth)
							// Found a sibling synthetic node at same depth - check if it should be merged
							// Find orphan info that corresponds to this sibling
							siblingOrphanInfo := orphanInfo{}
							foundSiblingOrphan := false
							for _, oi := range hybridOrphans {
								// Check if this sibling's path leads back to an orphan
								if oi.span.SpanID == siblingID || childToParent[siblingID] == "" {
									// Try to find the orphan that created this sibling
									// by checking if sibling is descendant of orphan's path
									foundSiblingOrphan = true
									siblingOrphanInfo = oi
									break
								}
							}
							
							// Actually, we need to find which orphan created this sibling
							// The siblingID is a placeholder like "synth_X_Y", so we can extract the orphan ID
							if !foundSiblingOrphan {
								// Try to find orphan by matching the sibling's placeholder ID pattern
								for _, oi := range hybridOrphans {
									if strings.Contains(siblingID, oi.span.SpanID) {
										siblingOrphanInfo = oi
										foundSiblingOrphan = true
										fmt.Printf("  RECONNECTION - BFS: Found orphan %s that created sibling %s\n", 
											oi.span.SpanID, siblingID)
										break
									}
								}
							}
							
							if foundSiblingOrphan && siblingOrphanInfo.bloomFilter != nil {
								fmt.Printf("  RECONNECTION - BFS: Checking if labeled node %s is in sibling orphan %s bloom filter\n", 
									currentID, siblingOrphanInfo.span.SpanID)
								if siblingOrphanInfo.bloomFilter.Test([]byte(currentID)) {
									// Current labeled node is in sibling's bloom filter - merge
									fmt.Printf("  RECONNECTION - Merge: labeled node %s found in sibling orphan %s bloom filter, merging\n", 
										currentID, siblingOrphanInfo.span.SpanID)
									
									// Merge: replace sibling with current labeled node
									oldSiblingID := siblingID
									newSiblingID := currentID
									
									fmt.Printf("  RECONNECTION - Merge: Replacing %s with %s\n", oldSiblingID, newSiblingID)
									
									// Find and remove the old sibling span from reconnectedTrace
									foundSiblingSpan := false
									spanToRemoveIdx := -1
									for i := range reconnectedTrace.Spans {
										if reconnectedTrace.Spans[i].SpanID == oldSiblingID {
											foundSiblingSpan = true
											spanToRemoveIdx = i
											break
										}
									}
									
									if foundSiblingSpan {
										// Remove the old span from reconnectedTrace.Spans
										reconnectedTrace.Spans = append(reconnectedTrace.Spans[:spanToRemoveIdx], reconnectedTrace.Spans[spanToRemoveIdx+1:]...)
										fmt.Printf("  RECONNECTION - Merge: Removed redundant span %s from reconnectedTrace\n", oldSiblingID)
										
										// Update maps
										delete(spanIDMap, oldSiblingID)
										delete(byID, oldSiblingID)
										// Note: newSiblingID should already exist in maps since it's the labeled node
										if !spanIDMap[newSiblingID] {
											spanIDMap[newSiblingID] = true
										}
										if _, exists := byID[newSiblingID]; !exists {
											// Find the newSiblingID span and add to byID
											for _, span := range reconnectedTrace.Spans {
												if span.SpanID == newSiblingID {
													byID[newSiblingID] = span
													break
												}
											}
										}
										// Update depth map if needed
										if oldDepth, exists := spanDepthMap[oldSiblingID]; exists {
											if _, exists := spanDepthMap[newSiblingID]; !exists {
												spanDepthMap[newSiblingID] = oldDepth
											}
										}
										fmt.Printf("  RECONNECTION - Merge: Updated maps (removed %s, keeping %s)\n", oldSiblingID, newSiblingID)
									} else {
										fmt.Printf("  RECONNECTION - Merge: WARNING - Could not find sibling span %s in reconnectedTrace\n", oldSiblingID)
									}
									
									// Update all references
									refUpdateCount := 0
									for i := range reconnectedTrace.Spans {
										for j := range reconnectedTrace.Spans[i].References {
											if reconnectedTrace.Spans[i].References[j].SpanID == oldSiblingID {
												reconnectedTrace.Spans[i].References[j].SpanID = newSiblingID
												refUpdateCount++
											}
										}
									}
									fmt.Printf("  RECONNECTION - Merge: Updated %d references from %s -> %s\n", refUpdateCount, oldSiblingID, newSiblingID)
									
									// Update bridge edges
									edgeUpdateCount := 0
									for i := range result.BridgeEdges {
										if result.BridgeEdges[i].FromSpanID == oldSiblingID {
											result.BridgeEdges[i].FromSpanID = newSiblingID
											edgeUpdateCount++
										}
										if result.BridgeEdges[i].ToSpanID == oldSiblingID {
											result.BridgeEdges[i].ToSpanID = newSiblingID
											edgeUpdateCount++
										}
									}
									fmt.Printf("  RECONNECTION - Merge: Updated %d bridge edges from %s -> %s\n", edgeUpdateCount, oldSiblingID, newSiblingID)
									
									// Update parentToChildren map - move children from oldSiblingID to newSiblingID
									if children, exists := parentToChildren[oldSiblingID]; exists {
										// Add oldSiblingID's children to newSiblingID's children (merge the lists)
										if existingChildren, exists := parentToChildren[newSiblingID]; exists {
											// Merge children lists, avoiding duplicates
											childSet := make(map[string]bool)
											for _, c := range existingChildren {
												childSet[c] = true
											}
											for _, c := range children {
												if !childSet[c] {
													parentToChildren[newSiblingID] = append(parentToChildren[newSiblingID], c)
												}
											}
										} else {
											parentToChildren[newSiblingID] = children
										}
										delete(parentToChildren, oldSiblingID)
										fmt.Printf("  RECONNECTION - Merge: Updated parentToChildren map (moved %d children from %s to %s)\n", 
											len(children), oldSiblingID, newSiblingID)
									}
									
									// Update childToParent map for all children of oldSiblingID
									childUpdateCount := 0
									for childID, parentID := range childToParent {
										if parentID == oldSiblingID {
											childToParent[childID] = newSiblingID
											childUpdateCount++
										}
									}
									fmt.Printf("  RECONNECTION - Merge: Updated %d children to point to new parent %s\n", childUpdateCount, newSiblingID)
									
									// Remove oldSiblingID from visited set if it was there (so we don't try to process it)
									delete(visited, oldSiblingID)
									break
								} else {
									fmt.Printf("  RECONNECTION - BFS: Labeled node %s NOT in sibling orphan %s bloom filter, no merge\n", 
										currentID, siblingOrphanInfo.span.SpanID)
								}
							} else {
								if !foundSiblingOrphan {
									fmt.Printf("  RECONNECTION - BFS: Could not find orphan info for sibling %s\n", siblingID)
								} else {
									fmt.Printf("  RECONNECTION - BFS: Sibling orphan %s has no bloom filter\n", siblingOrphanInfo.span.SpanID)
								}
							}
						} else {
							fmt.Printf("  RECONNECTION - BFS: Sibling %s not synthetic or different depth (%d vs %d), skipping\n", 
								siblingID, siblingDepth, currentDepth)
						}
					}
				} else {
					fmt.Printf("  RECONNECTION - BFS: %s has no parent, skipping sibling check\n", currentID)
				}
			} else if isLabeledSynthetic && strings.HasPrefix(currentID, "synth_") {
				// This is a placeholder synthetic node - check if there's a labeled sibling we should merge into
				fmt.Printf("  RECONNECTION - BFS: %s is a placeholder synthetic node, checking for labeled siblings\n", currentID)
				parentID := childToParent[currentID]
				if parentID != "" {
					// Get all siblings (children of parent) - check both current siblings and any that might have been merged
					siblings := parentToChildren[parentID]
					fmt.Printf("  RECONNECTION - BFS: Placeholder %s has parent %s, found %d siblings: %v\n", 
						currentID, parentID, len(siblings), siblings)
					
					// Also check all spans at the same depth from the same parent to find labeled nodes
					labeledSiblingFound := ""
					for _, span := range reconnectedTrace.Spans {
						// Check if this span is a labeled synthetic node at the same depth
						isLabeled := false
						for _, tag := range span.Tags {
							if tag.Key == "bridge.synthetic" {
								isLabeled = true
								break
							}
						}
						if isLabeled && !strings.HasPrefix(span.SpanID, "synth_") {
							// Check if this labeled node is at the same depth and has the same parent
							if spanDepth, exists := spanDepthMap[span.SpanID]; exists && spanDepth == currentDepth {
								// Check if it has the same parent
								spanParent := ""
								for _, ref := range span.References {
									if ref.RefType == "CHILD_OF" || ref.RefType == "FOLLOWS_FROM" {
										spanParent = ref.SpanID
										break
									}
								}
								if spanParent == parentID {
									labeledSiblingFound = span.SpanID
									fmt.Printf("  RECONNECTION - BFS: Found labeled sibling %s at same depth %d with same parent %s\n", 
										labeledSiblingFound, currentDepth, parentID)
									break
								}
							}
						}
					}
					
					// Check both current siblings and the found labeled sibling
					allSiblings := make(map[string]bool)
					for _, s := range siblings {
						allSiblings[s] = true
					}
					if labeledSiblingFound != "" {
						allSiblings[labeledSiblingFound] = true
					}
					
					for siblingID := range allSiblings {
						if siblingID == currentID {
							continue
						}
						
						// Check if sibling exists in byID (might have been merged already)
						siblingSpan, exists := byID[siblingID]
						if !exists {
							fmt.Printf("  RECONNECTION - BFS: Sibling %s not in byID, may have been merged, skipping\n", siblingID)
							continue
						}
						
						// Check if sibling is a labeled synthetic node at same depth
						isSiblingLabeled := false
						for _, tag := range siblingSpan.Tags {
							if tag.Key == "bridge.synthetic" {
								isSiblingLabeled = true
								break
							}
						}
						
						fmt.Printf("  RECONNECTION - BFS: Checking sibling %s (labeled=%v, depth=%d)\n", 
							siblingID, isSiblingLabeled && !strings.HasPrefix(siblingID, "synth_"), spanDepthMap[siblingID])
						
						if isSiblingLabeled && !strings.HasPrefix(siblingID, "synth_") && spanDepthMap[siblingID] == currentDepth {
							// Found a labeled sibling - check if it's in our bloom filter
							// Find our orphan info
							for _, oi := range hybridOrphans {
								if strings.Contains(currentID, oi.span.SpanID) {
									fmt.Printf("  RECONNECTION - BFS: Checking if labeled sibling %s is in orphan %s bloom filter\n", 
										siblingID, oi.span.SpanID)
									if oi.bloomFilter != nil && oi.bloomFilter.Test([]byte(siblingID)) {
										fmt.Printf("  RECONNECTION - Merge: Placeholder %s merging into labeled sibling %s (found in bloom filter)\n", 
											currentID, siblingID)
											// Merge this placeholder into the labeled sibling
											oldSiblingID := currentID
											newSiblingID := siblingID
											
											// Find and remove the old placeholder span from reconnectedTrace
											spanToRemoveIdx := -1
											for i := range reconnectedTrace.Spans {
												if reconnectedTrace.Spans[i].SpanID == oldSiblingID {
													spanToRemoveIdx = i
													break
												}
											}
											
											if spanToRemoveIdx >= 0 {
												// Remove the old span from reconnectedTrace.Spans
												reconnectedTrace.Spans = append(reconnectedTrace.Spans[:spanToRemoveIdx], reconnectedTrace.Spans[spanToRemoveIdx+1:]...)
												fmt.Printf("  RECONNECTION - Merge: Removed redundant placeholder span %s from reconnectedTrace\n", oldSiblingID)
												
												// Update maps
												delete(spanIDMap, oldSiblingID)
												delete(byID, oldSiblingID)
												// Note: newSiblingID should already exist in maps since it's the labeled node
												if !spanIDMap[newSiblingID] {
													spanIDMap[newSiblingID] = true
												}
												if _, exists := byID[newSiblingID]; !exists {
													// Find the newSiblingID span and add to byID
													for _, span := range reconnectedTrace.Spans {
														if span.SpanID == newSiblingID {
															byID[newSiblingID] = span
															break
														}
													}
												}
												// Update depth map if needed
												if oldDepth, exists := spanDepthMap[oldSiblingID]; exists {
													if _, exists := spanDepthMap[newSiblingID]; !exists {
														spanDepthMap[newSiblingID] = oldDepth
													}
												}
												fmt.Printf("  RECONNECTION - Merge: Updated maps (removed %s, keeping %s)\n", oldSiblingID, newSiblingID)
											} else {
												fmt.Printf("  RECONNECTION - Merge: WARNING - Could not find placeholder span %s in reconnectedTrace\n", oldSiblingID)
											}
											
											// Update references
											for i := range reconnectedTrace.Spans {
												for j := range reconnectedTrace.Spans[i].References {
													if reconnectedTrace.Spans[i].References[j].SpanID == oldSiblingID {
														reconnectedTrace.Spans[i].References[j].SpanID = newSiblingID
													}
												}
											}
											
											// Update parentToChildren
											if children, exists := parentToChildren[oldSiblingID]; exists {
												if existingChildren, exists := parentToChildren[newSiblingID]; exists {
													childSet := make(map[string]bool)
													for _, c := range existingChildren {
														childSet[c] = true
													}
													for _, c := range children {
														if !childSet[c] {
															parentToChildren[newSiblingID] = append(parentToChildren[newSiblingID], c)
														}
													}
												} else {
													parentToChildren[newSiblingID] = children
												}
												delete(parentToChildren, oldSiblingID)
											}
											
											// Update childToParent
											for childID, pid := range childToParent {
												if pid == oldSiblingID {
													childToParent[childID] = newSiblingID
												}
											}
											
											delete(visited, oldSiblingID)
											break
										}
									}
								}
							}
						}
					}
				}
			} else {
				fmt.Printf("  RECONNECTION - BFS: %s is not a synthetic node, skipping merge check\n", currentID)
			}
			
			// Add children to queue
			children := parentToChildren[currentID]
			fmt.Printf("  RECONNECTION - BFS: Adding %d children of %s to queue: %v\n", len(children), currentID, children)
			for _, childID := range children {
				if !visited[childID] {
					visited[childID] = true
					queue = append(queue, childID)
				} else {
					fmt.Printf("  RECONNECTION - BFS: Skipping already visited child %s\n", childID)
				}
			}
		}
		
		fmt.Printf("  RECONNECTION - BFS: Processed %d nodes during BFS traversal\n", processedCount)
	}
	
	// Calculate total synthetic spans created across all bridge edges
	totalSynthetic := 0
	for _, edge := range result.BridgeEdges {
		totalSynthetic += edge.SyntheticSpansCreated
	}
	
    result.ReconnectedTrace = reconnectedTrace
	result.ReconnectionRate = float64(reconnectedCount) / float64(len(orphanedSpans)) * 100
	result.Success = reconnectedCount > 0
	result.TotalSyntheticSpans = totalSynthetic
	
	fmt.Printf("  RECONNECTION - Successfully reconnected %d/%d orphaned spans (%.1f%%), created %d synthetic spans\n", 
		reconnectedCount, len(orphanedSpans), result.ReconnectionRate, totalSynthetic)
	
	return result
}

// calculateReconnectionConfidence calculates confidence in a reconnection based on bloom filter analysis
func (loader *JaegerTraceLoader) calculateReconnectionConfidence(bf *bloom.BloomFilter, ancestorID, orphanedID string) float64 {
	// Simple confidence calculation based on bloom filter properties
	// In a real implementation, this could be more sophisticated
	
	// Test if both IDs are in the bloom filter
	ancestorInBF := bf.Test([]byte(ancestorID))
	orphanedInBF := bf.Test([]byte(orphanedID))
	
	if !ancestorInBF || !orphanedInBF {
		return 0.0
	}
	
	// Base confidence starts at 0.5
	confidence := 0.5
	
	// Increase confidence if the bloom filter contains many spans (more context)
	// This is a rough estimate - in practice, you'd want to track the actual count
	// For now, we'll use a simple heuristic
	confidence += 0.2 // Add some confidence for successful containment
	
	// Cap at 1.0
	if confidence > 1.0 {
		confidence = 1.0
	}
	
	return confidence
}

// NewTraceStorage creates a new trace storage instance
// outputBaseName: base name for output directories (e.g., "tagged-hash-3" or "jaeger")
// dataDir: parent directory where output directories will be created
func NewTraceStorage(dataDir, outputBaseName string) *TraceStorage {
	return &TraceStorage{
		dataDir:          dataDir,
		lossyDir:         filepath.Join(dataDir, outputBaseName+"-lossy"),
		reconstructedDir: filepath.Join(dataDir, outputBaseName+"-reconstructed"),
	}
}

// Initialize creates the necessary directories and clears existing files
func (ts *TraceStorage) Initialize() error {
	// Create directories if they don't exist
	dirs := []string{ts.dataDir, ts.lossyDir, ts.reconstructedDir}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}
	
	// Clear existing files
	if err := ts.clearDirectory(ts.lossyDir); err != nil {
		return fmt.Errorf("failed to clear lossy directory: %w", err)
	}
	if err := ts.clearDirectory(ts.reconstructedDir); err != nil {
		return fmt.Errorf("failed to clear reconstructed directory: %w", err)
	}
	
	return nil
}

// clearDirectory removes all files from a directory
func (ts *TraceStorage) clearDirectory(dir string) error {
	files, err := filepath.Glob(filepath.Join(dir, "*"))
	if err != nil {
		return err
	}
	for _, file := range files {
		if err := os.Remove(file); err != nil {
			return err
		}
	}
	return nil
}

// SaveLossyTraces saves lossy traces and their metadata
func (ts *TraceStorage) SaveLossyTraces(traces []*JaegerTrace, metadata []*DataLossInfo, checkpointDistance int) error {
	// Save traces in Jaeger format
	tracesFile := filepath.Join(ts.lossyDir, "traces.json")
	if err := ts.saveTraces(traces, tracesFile); err != nil {
		return fmt.Errorf("failed to save lossy traces: %w", err)
	}
	
	// Save metadata
	metadataFile := filepath.Join(ts.lossyDir, "metadata.json")
	if err := ts.saveLossyMetadata(traces, metadata, metadataFile, checkpointDistance); err != nil {
		return fmt.Errorf("failed to save lossy metadata: %w", err)
	}
	
	fmt.Printf("💾 Saved %d lossy traces to %s\n", len(traces), ts.lossyDir)
	return nil
}

// SaveReconstructedTraces saves reconstructed traces and their metadata
func (ts *TraceStorage) SaveReconstructedTraces(traces []*JaegerTrace, results []*ReconnectionResult, checkpointDistance int) error {
	// Save traces in Jaeger format
	tracesFile := filepath.Join(ts.reconstructedDir, "traces.json")
	if err := ts.saveTraces(traces, tracesFile); err != nil {
		return fmt.Errorf("failed to save reconstructed traces: %w", err)
	}
	
	// Save metadata
	metadataFile := filepath.Join(ts.reconstructedDir, "metadata.json")
	if err := ts.saveReconstructedMetadata(traces, results, metadataFile, checkpointDistance); err != nil {
		return fmt.Errorf("failed to save reconstructed metadata: %w", err)
	}
	
	fmt.Printf("💾 Saved %d reconstructed traces to %s\n", len(traces), ts.reconstructedDir)
	return nil
}

// saveTraces saves traces in Jaeger format ({"data": [...]})
func (ts *TraceStorage) saveTraces(traces []*JaegerTrace, filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	
	// Wrap traces in Jaeger API format
	response := struct {
		Data []*JaegerTrace `json:"data"`
	}{
		Data: traces,
	}
	
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(response)
}

// saveLossyMetadata saves metadata for lossy traces
func (ts *TraceStorage) saveLossyMetadata(traces []*JaegerTrace, metadata []*DataLossInfo, filePath string, checkpointDistance int) error {
	storageMetadata := &StorageMetadata{
		CreatedAt:   time.Now(),
		TotalTraces: len(traces),
		Traces:      make([]TraceMetadata, len(traces)),
		CheckpointDistance: checkpointDistance,
	}
	
	var totalOriginalSize, totalLossySize, totalAncestryDataSize int64
	var totalSpans int
	
	for i, trace := range traces {
		// Calculate sizes
		lossySize, _ := calculateTraceSize(trace)
		originalSize := lossySize // For lossy traces, we don't have the original size
		
		// Calculate ancestry data size
		ancestryDataSize := calculateAncestryDataSize(trace)
		ancestryDataSizePerSpan := float64(0)
		if len(trace.Spans) > 0 {
			ancestryDataSizePerSpan = float64(ancestryDataSize) / float64(len(trace.Spans))
		}
		
		tm := TraceMetadata{
			TraceID:       trace.TraceID,
			OriginalSpans: len(trace.Spans),
			LossySpans:    len(trace.Spans),
			LossySizeBytes: lossySize,
			CheckpointDistance: checkpointDistance,
			AncestryDataSizeBytes: ancestryDataSize,
			AncestryDataSizeBytesPerSpan: ancestryDataSizePerSpan,
		}
		
		if i < len(metadata) && metadata[i] != nil {
			tm.DataLossInfo = metadata[i]
			tm.RemovedSpans = metadata[i].AffectedSpans
			// Estimate original size based on removed spans
			originalSize = int64(float64(lossySize) * float64(len(trace.Spans) + metadata[i].AffectedSpans) / float64(len(trace.Spans)))
			tm.OriginalSizeBytes = originalSize
			tm.SizeReductionPercent = float64(originalSize - lossySize) / float64(originalSize) * 100
		}
		
		totalOriginalSize += originalSize
		totalLossySize += lossySize
		totalAncestryDataSize += ancestryDataSize
		totalSpans += len(trace.Spans)
		storageMetadata.Traces[i] = tm
	}
	
	// Calculate summary statistics
	storageMetadata.TotalOriginalSizeBytes = totalOriginalSize
	storageMetadata.TotalLossySizeBytes = totalLossySize
	storageMetadata.TotalAncestryDataSizeBytes = totalAncestryDataSize
	if totalOriginalSize > 0 {
		storageMetadata.AverageSizeReductionPercent = float64(totalOriginalSize - totalLossySize) / float64(totalOriginalSize) * 100
	}
	if totalSpans > 0 {
		storageMetadata.AverageAncestryDataSizeBytesPerSpan = float64(totalAncestryDataSize) / float64(totalSpans)
	}
	
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(storageMetadata)
}

// saveReconstructedMetadata saves metadata for reconstructed traces
func (ts *TraceStorage) saveReconstructedMetadata(traces []*JaegerTrace, results []*ReconnectionResult, filePath string, checkpointDistance int) error {
	storageMetadata := &StorageMetadata{
		CreatedAt:   time.Now(),
		TotalTraces: len(traces),
		Traces:      make([]TraceMetadata, len(traces)),
		CheckpointDistance: checkpointDistance,
	}
	
	var totalOriginalSize, totalReconstructedSize, totalAncestryDataSize int64
	var totalSpans int
	
	for i, trace := range traces {
		// Calculate sizes
		reconstructedSize, _ := calculateTraceSize(trace)
		originalSize := reconstructedSize // Default to reconstructed size
		
		// Calculate ancestry data size from original tagged trace if available, otherwise use reconstructed
		var ancestryDataSize int64
		var numSpansForAncestry int
		var traceForAncestry *JaegerTrace
		if i < len(results) && results[i] != nil {
			if results[i].TaggedTrace != nil {
				traceForAncestry = results[i].TaggedTrace
			} else if results[i].OriginalTrace != nil {
				traceForAncestry = results[i].OriginalTrace
			}
		}
		if traceForAncestry != nil {
			ancestryDataSize = calculateAncestryDataSize(traceForAncestry)
			numSpansForAncestry = len(traceForAncestry.Spans)
		} else {
			ancestryDataSize = calculateAncestryDataSize(trace)
			numSpansForAncestry = len(trace.Spans)
		}
		ancestryDataSizePerSpan := float64(0)
		if numSpansForAncestry > 0 {
			ancestryDataSizePerSpan = float64(ancestryDataSize) / float64(numSpansForAncestry)
		}
		
		tm := TraceMetadata{
			TraceID:            trace.TraceID,
			ReconstructedSpans: len(trace.Spans),
			ReconstructedSizeBytes: reconstructedSize,
			CheckpointDistance: checkpointDistance,
			AncestryDataSizeBytes: ancestryDataSize,
			AncestryDataSizeBytesPerSpan: ancestryDataSizePerSpan,
		}
		
		if i < len(results) && results[i] != nil {
			tm.ReconnectionResult = results[i]
			// Use TaggedTrace (before data loss) for original_spans if available, otherwise fall back to OriginalTrace (lossy)
			var originalTraceForMetadata *JaegerTrace
			if results[i].TaggedTrace != nil {
				originalTraceForMetadata = results[i].TaggedTrace
			} else if results[i].OriginalTrace != nil {
				originalTraceForMetadata = results[i].OriginalTrace
			}
			
			if originalTraceForMetadata != nil {
				tm.OriginalSpans = len(originalTraceForMetadata.Spans)
				originalSize, _ = calculateTraceSize(originalTraceForMetadata)
				tm.OriginalSizeBytes = originalSize
				
				// Calculate recovery percentage
				if originalSize > 0 {
					tm.SizeRecoveryPercent = float64(reconstructedSize) / float64(originalSize) * 100
				}
			}
		}
		
		totalOriginalSize += originalSize
		totalReconstructedSize += reconstructedSize
		totalAncestryDataSize += ancestryDataSize
		totalSpans += numSpansForAncestry
		storageMetadata.Traces[i] = tm
	}
	
	// Calculate summary statistics
	storageMetadata.TotalOriginalSizeBytes = totalOriginalSize
	storageMetadata.TotalReconstructedSizeBytes = totalReconstructedSize
	storageMetadata.TotalAncestryDataSizeBytes = totalAncestryDataSize
	if totalOriginalSize > 0 {
		storageMetadata.AverageSizeRecoveryPercent = float64(totalReconstructedSize) / float64(totalOriginalSize) * 100
	}
	if totalSpans > 0 {
		storageMetadata.AverageAncestryDataSizeBytesPerSpan = float64(totalAncestryDataSize) / float64(totalSpans)
	}
	
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(storageMetadata)
}

// LoadLossyTraces loads lossy traces from file
func (ts *TraceStorage) LoadLossyTraces() ([]*JaegerTrace, error) {
	tracesFile := filepath.Join(ts.lossyDir, "traces.json")
	return ts.loadTraces(tracesFile)
}

// LoadReconstructedTraces loads reconstructed traces from file
func (ts *TraceStorage) LoadReconstructedTraces() ([]*JaegerTrace, error) {
	tracesFile := filepath.Join(ts.reconstructedDir, "traces.json")
	return ts.loadTraces(tracesFile)
}

// loadTraces loads traces from a JSON file
func (ts *TraceStorage) loadTraces(filePath string) ([]*JaegerTrace, error) {
	return loadTracesFromFile(filePath)
}

// loadTracesFromFile loads traces from a JSON file (standalone function for use outside TraceStorage)
func loadTracesFromFile(filePath string) ([]*JaegerTrace, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	
	// Try Jaeger API format first
	var response struct {
		Data []*JaegerTrace `json:"data"`
	}
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&response); err != nil {
		// Fallback: try direct array format
		var traces []*JaegerTrace
		file.Seek(0, 0)
		decoder = json.NewDecoder(file)
		if err2 := decoder.Decode(&traces); err2 != nil {
			return nil, fmt.Errorf("failed to unmarshal traces (tried both formats): %w, %w", err, err2)
		}
		return traces, nil
	}
	
	return response.Data, nil
}

