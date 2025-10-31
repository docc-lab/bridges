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
	OriginalTrace        *JaegerTrace   `json:"originalTrace"`
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
}

// calculateTraceSize calculates the size of a trace in bytes when serialized to JSON
func calculateTraceSize(trace *JaegerTrace) (int64, error) {
	data, err := json.Marshal(trace)
	if err != nil {
		return 0, err
	}
	return int64(len(data)), nil
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
	
	// Initialize trace storage
	storage := NewTraceStorage("./data")
	if err := storage.Initialize(); err != nil {
		fmt.Printf("Error initializing storage: %v\n", err)
		return
	}
	
	// Load traces based on input source
	var allTraces []*JaegerTrace
	var totalTraces int
	
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
		if err := storage.SaveLossyTraces(allLossyTraces, allLossyMetadata); err != nil {
			fmt.Printf("Error saving lossy traces: %v\n", err)
		} else {
			fmt.Printf("✅ Saved lossy traces\n")
		}
		
		if len(allReconstructedTraces) > 0 {
			fmt.Printf("Saving %d reconstructed traces...\n", len(allReconstructedTraces))
			if err := storage.SaveReconstructedTraces(allReconstructedTraces, allReconnectionResults); err != nil {
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
	queue := make([]string, 0)
	for _, rootID := range rootSpans {
		spanDepthMap[rootID] = 0
		queue = append(queue, rootID)
	}
	
	for len(queue) > 0 {
		currentID := queue[0]
		queue = queue[1:]
		currentDepth := spanDepthMap[currentID]
		
		// Find children of current span
		for _, span := range trace.Spans {
			for _, ref := range span.References {
				if ref.RefType == "CHILD_OF" && ref.SpanID == currentID {
					spanDepthMap[span.SpanID] = currentDepth + 1
					queue = append(queue, span.SpanID)
				}
			}
		}
	}
	
	// Find orphaned spans (spans with missing parents)
	var orphanedSpans []JaegerSpan
	for _, span := range trace.Spans {
		hasValidParent := false
		for _, ref := range span.References {
			if ref.RefType == "CHILD_OF" {
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
	
	// Build children index and span lookup for descendant search
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

    // Determine trace's ancestry_mode for fallback
    traceMode := getTraceAncestryMode(trace)
    
    // Attempt to reconnect each orphaned span
    reconnectedCount := 0
    for _, orphanedSpan := range orphanedSpans {
        fmt.Printf("  RECONNECTION - Orphan %s: attempting ancestry extraction from ancestry/ancestry_mode tags\n", orphanedSpan.SpanID)
        
        // Try to get ancestry from the orphan's own tags
        mode, payload := getAncestryFromTags(orphanedSpan)
        donorSpan := orphanedSpan
        
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
                donorSpan = *descendant
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
			var missingParentID string
			for _, ref := range orphanedSpan.References {
				if ref.RefType == "CHILD_OF" && !spanIDMap[ref.SpanID] {
					missingParentID = ref.SpanID
					break
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
							if reconnectedTrace.Spans[i].References[j].RefType == "CHILD_OF" && reconnectedTrace.Spans[i].References[j].SpanID == missingParentID {
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

        if mode == "hash" {
			// Parse ancestry array
            parts := strings.Split(payload, ",")
            fmt.Printf("    RECONNECTION - Orphan %s: hash parts=%v\n", orphanedSpan.SpanID, parts)
            // Do not require len>=2; proceed and validate below
			// Locate orphan in ancestry chain
			idx := -1
			for i := len(parts) - 1; i >= 0; i-- {
				if parts[i] == orphanedSpan.SpanID {
					idx = i
					break
				}
			}
            if idx <= 0 { // not found or no parent
                if idx == -1 {
                    fmt.Printf("    RECONNECTION - Orphan %s: not found in ancestry array (donor=%s); skipping\n", orphanedSpan.SpanID, donorSpan.SpanID)
                } else {
                    fmt.Printf("    RECONNECTION - Orphan %s: found at idx=0 (no parent); skipping\n", orphanedSpan.SpanID)
                }
                continue
            }
			parentChain := parts[:idx] // up to but not including orphan
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
            for _, ref := range orphanedSpan.References {
                if ref.RefType == "CHILD_OF" && !originalSpanSet[ref.SpanID] {
                    originalParent = ref.SpanID
                    break
                }
            }
            // If originalParent not found in references, derive it from ancestry chain
            // (This can happen when a previous orphan already synthesized the missing parent)
            if originalParent == "" && idx > 0 {
                // The parent should be the span immediately before the orphan in the ancestry chain
                candidateParent := parts[idx-1]
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
			// parentChain is parts[:idx], so originalParent should be at parentChain[idx-1] (which is parts[idx-1])
			parentIdxInChain := idx - 1
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
						if reconnectedTrace.Spans[i].References[j].RefType == "CHILD_OF" && reconnectedTrace.Spans[i].References[j].SpanID == originalParent {
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
func NewTraceStorage(dataDir string) *TraceStorage {
	return &TraceStorage{
		dataDir:          dataDir,
		lossyDir:         filepath.Join(dataDir, "lossy"),
		reconstructedDir: filepath.Join(dataDir, "reconstructed"),
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
func (ts *TraceStorage) SaveLossyTraces(traces []*JaegerTrace, metadata []*DataLossInfo) error {
	// Save traces in Jaeger format
	tracesFile := filepath.Join(ts.lossyDir, "traces.json")
	if err := ts.saveTraces(traces, tracesFile); err != nil {
		return fmt.Errorf("failed to save lossy traces: %w", err)
	}
	
	// Save metadata
	metadataFile := filepath.Join(ts.lossyDir, "metadata.json")
	if err := ts.saveLossyMetadata(traces, metadata, metadataFile); err != nil {
		return fmt.Errorf("failed to save lossy metadata: %w", err)
	}
	
	fmt.Printf("💾 Saved %d lossy traces to %s\n", len(traces), ts.lossyDir)
	return nil
}

// SaveReconstructedTraces saves reconstructed traces and their metadata
func (ts *TraceStorage) SaveReconstructedTraces(traces []*JaegerTrace, results []*ReconnectionResult) error {
	// Save traces in Jaeger format
	tracesFile := filepath.Join(ts.reconstructedDir, "traces.json")
	if err := ts.saveTraces(traces, tracesFile); err != nil {
		return fmt.Errorf("failed to save reconstructed traces: %w", err)
	}
	
	// Save metadata
	metadataFile := filepath.Join(ts.reconstructedDir, "metadata.json")
	if err := ts.saveReconstructedMetadata(traces, results, metadataFile); err != nil {
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
func (ts *TraceStorage) saveLossyMetadata(traces []*JaegerTrace, metadata []*DataLossInfo, filePath string) error {
	storageMetadata := &StorageMetadata{
		CreatedAt:   time.Now(),
		TotalTraces: len(traces),
		Traces:      make([]TraceMetadata, len(traces)),
	}
	
	var totalOriginalSize, totalLossySize int64
	
	for i, trace := range traces {
		// Calculate sizes
		lossySize, _ := calculateTraceSize(trace)
		originalSize := lossySize // For lossy traces, we don't have the original size
		
		tm := TraceMetadata{
			TraceID:       trace.TraceID,
			OriginalSpans: len(trace.Spans),
			LossySpans:    len(trace.Spans),
			LossySizeBytes: lossySize,
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
		storageMetadata.Traces[i] = tm
	}
	
	// Calculate summary statistics
	storageMetadata.TotalOriginalSizeBytes = totalOriginalSize
	storageMetadata.TotalLossySizeBytes = totalLossySize
	if totalOriginalSize > 0 {
		storageMetadata.AverageSizeReductionPercent = float64(totalOriginalSize - totalLossySize) / float64(totalOriginalSize) * 100
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
func (ts *TraceStorage) saveReconstructedMetadata(traces []*JaegerTrace, results []*ReconnectionResult, filePath string) error {
	storageMetadata := &StorageMetadata{
		CreatedAt:   time.Now(),
		TotalTraces: len(traces),
		Traces:      make([]TraceMetadata, len(traces)),
	}
	
	var totalOriginalSize, totalReconstructedSize int64
	
	for i, trace := range traces {
		// Calculate sizes
		reconstructedSize, _ := calculateTraceSize(trace)
		originalSize := reconstructedSize // Default to reconstructed size
		
		tm := TraceMetadata{
			TraceID:            trace.TraceID,
			ReconstructedSpans: len(trace.Spans),
			ReconstructedSizeBytes: reconstructedSize,
		}
		
		if i < len(results) && results[i] != nil {
			tm.ReconnectionResult = results[i]
			if results[i].OriginalTrace != nil {
				tm.OriginalSpans = len(results[i].OriginalTrace.Spans)
				originalSize, _ = calculateTraceSize(results[i].OriginalTrace)
				tm.OriginalSizeBytes = originalSize
				
				// Calculate recovery percentage
				if originalSize > 0 {
					tm.SizeRecoveryPercent = float64(reconstructedSize) / float64(originalSize) * 100
				}
			}
		}
		
		totalOriginalSize += originalSize
		totalReconstructedSize += reconstructedSize
		storageMetadata.Traces[i] = tm
	}
	
	// Calculate summary statistics
	storageMetadata.TotalOriginalSizeBytes = totalOriginalSize
	storageMetadata.TotalReconstructedSizeBytes = totalReconstructedSize
	if totalOriginalSize > 0 {
		storageMetadata.AverageSizeRecoveryPercent = float64(totalReconstructedSize) / float64(totalOriginalSize) * 100
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

