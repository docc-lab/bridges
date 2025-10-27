package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
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
	FromSpanID     string  `json:"fromSpanID"`
	ToSpanID       string  `json:"toSpanID"`
	OriginalParent string  `json:"originalParent"` // The missing parent that was being referenced
	Depth          int     `json:"depth"`          // Depth of the reconnected span
	Confidence     float64 `json:"confidence"`     // Confidence in the reconnection (0-1)
}

// ReconnectionResult represents the result of trace reconnection
type ReconnectionResult struct {
	OriginalTrace    *JaegerTrace   `json:"originalTrace"`
	ReconnectedTrace *JaegerTrace   `json:"reconnectedTrace"`
	BridgeEdges      []BridgeEdge   `json:"bridgeEdges"`
	ReconnectionRate float64        `json:"reconnectionRate"` // Percentage of orphaned spans reconnected
	Success          bool           `json:"success"`
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

func main() {
	// Jaeger URL - adjust the IP and port based on your Kubernetes setup
	// Port 16686 is the Jaeger UI port, but we need the API port
	// Based on the service config, we should use the service name and port
	jaegerURL := "http://jaeger-ctr:16686"
	
	loader := NewJaegerTraceLoader(jaegerURL)
	ctx := context.Background()
	
	// Initialize trace storage
	storage := NewTraceStorage("./data")
	if err := storage.Initialize(); err != nil {
		fmt.Printf("Error initializing storage: %v\n", err)
		return
	}
	
	// Get available services
	fmt.Println("Fetching available services...")
	services, err := loader.GetServices(ctx)
	if err != nil {
		fmt.Printf("Error fetching services: %v\n", err)
		return
	}
	
	fmt.Printf("Available services: %v\n", services)
	
	// Load and process traces for each service
	totalTraces := 0
	var allTraces []*JaegerTrace
	
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
	
	// Test complete trace reconnection flow
	fmt.Printf("\nDEBUG: processedTraces count: %d\n", len(processedTraces))
	if len(processedTraces) > 0 {
		firstTrace := processedTraces[0]
		fmt.Printf("DEBUG: firstTrace spans: %d\n", len(firstTrace.Trace.Spans))
		
		// Print the full original trace JSON for comparison
		fmt.Println("\n=== ORIGINAL TRACE DATA ===")
		originalJSON, _ := json.MarshalIndent(firstTrace.Trace, "", "  ")
		fmt.Println(string(originalJSON))
		fmt.Println("=== END ORIGINAL TRACE DATA ===\n")
		
		if len(firstTrace.Trace.Spans) > 1 {
			fmt.Printf("\n=== TRACE RECONNECTION FLOW ===\n")
			fmt.Printf("Testing trace reconnection on: %s (%d spans)\n", firstTrace.Trace.TraceID, len(firstTrace.Trace.Spans))
			
			// Step 1: Simulate data loss with protection
			fmt.Printf("\n--- Step 1: Simulate Data Loss ---\n")
			simulatedTrace := loader.ForceDataLoss(firstTrace.Trace, 0.3) // Force 30% data loss
			fmt.Printf("Simulated trace: %s (%d spans)\n", simulatedTrace.TraceID, len(simulatedTrace.Spans))
			
			// Step 2: Detect data loss in simulated trace
			fmt.Printf("\n--- Step 2: Detect Data Loss ---\n")
			dataLossInfo := loader.DetectDataLoss(simulatedTrace)
			fmt.Printf("Data loss detected: %v\n", dataLossInfo.HasDataLoss)
			if dataLossInfo.HasDataLoss {
				fmt.Printf("Missing span IDs: %v\n", dataLossInfo.MissingSpanIDs)
				fmt.Printf("Orphaned spans: %v\n", dataLossInfo.OrphanSpans)
				fmt.Printf("Loss severity: %s\n", dataLossInfo.LossSeverity)
			}
			
			// Step 3: Attempt trace reconnection using bloom filters
			if dataLossInfo.HasDataLoss {
				fmt.Printf("\n--- Step 3: Reconnect Trace Using Bloom Filters ---\n")
				reconnectionResult := loader.ReconnectTrace(simulatedTrace)
				
				if reconnectionResult.Success {
					fmt.Printf("✅ Reconnection successful!\n")
					fmt.Printf("Reconnection rate: %.1f%%\n", reconnectionResult.ReconnectionRate)
					fmt.Printf("Bridge edges created: %d\n", len(reconnectionResult.BridgeEdges))
					
					// Show bridge edges
					for i, edge := range reconnectionResult.BridgeEdges {
						if i >= 5 { // Limit output
							fmt.Printf("  ... and %d more bridge edges\n", len(reconnectionResult.BridgeEdges)-5)
							break
						}
						fmt.Printf("  Bridge %d: %s -> %s (original parent: %s, confidence: %.2f)\n", 
							i+1, edge.FromSpanID[:8], edge.ToSpanID[:8], edge.OriginalParent[:8], edge.Confidence)
					}
					
					// Verify reconnected trace
					fmt.Printf("\n--- Step 4: Verify Reconnected Trace ---\n")
					reconnectedDataLoss := loader.DetectDataLoss(reconnectionResult.ReconnectedTrace)
					fmt.Printf("Reconnected trace data loss: %v\n", reconnectedDataLoss.HasDataLoss)
					if reconnectedDataLoss.HasDataLoss {
						fmt.Printf("Remaining orphaned spans: %d\n", len(reconnectedDataLoss.OrphanSpans))
					} else {
						fmt.Printf("✅ All spans successfully reconnected!\n")
					}
					
					// Save traces to files
					fmt.Printf("\n--- Step 5: Save Traces to Files ---\n")
					
					// Save lossy trace
					lossyTraces := []*JaegerTrace{simulatedTrace}
					lossyMetadata := []*DataLossInfo{dataLossInfo}
					if err := storage.SaveLossyTraces(lossyTraces, lossyMetadata); err != nil {
						fmt.Printf("Error saving lossy traces: %v\n", err)
					}
					
					// Save reconstructed trace
					reconstructedTraces := []*JaegerTrace{reconnectionResult.ReconnectedTrace}
					reconnectionResults := []*ReconnectionResult{reconnectionResult}
					if err := storage.SaveReconstructedTraces(reconstructedTraces, reconnectionResults); err != nil {
						fmt.Printf("Error saving reconstructed traces: %v\n", err)
					}
					
				} else {
					fmt.Printf("❌ Reconnection failed - no bridge edges could be created\n")
				}
			} else {
				fmt.Printf("No data loss detected - skipping reconnection\n")
			}
		}
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
	
	// If we have too many leaf spans, be more aggressive and allow removing some middle spans
	// by reducing the leaf span protection
	if len(leafSpans) > len(spanIDMap)/2 {
		fmt.Printf("  SIMULATION - Too many leaf spans (%d), reducing leaf protection\n", len(leafSpans))
		// Allow removing up to 50% of leaf spans to force some middle span removal
		maxLeafRemoval := int(float64(len(leafSpans)) * 0.5)
		if maxLeafRemoval > 0 {
			// Randomly select some leaf spans to make eligible for removal
			for i := 0; i < maxLeafRemoval && i < len(leafSpans); i++ {
				idx := rand.Intn(len(leafSpans))
				// Remove from leaf spans and add to eligible
				leafSpanID := leafSpans[idx]
				leafSpans = append(leafSpans[:idx], leafSpans[idx+1:]...)
				eligibleSpans = append(eligibleSpans, leafSpanID)
			}
		}
	}
	
	// Identify eligible spans for removal (non-root, non-leaf)
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
			eligibleSpans = append(eligibleSpans, spanID)
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
	
	// Build parent-child relationships
	parentChildMap := make(map[string][]string) // parent -> children
	var rootSpans []string
	
	for _, span := range trace.Spans {
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
	
	// Find spans that have children (middle spans) - these will create orphans when removed
	var middleSpans []string
	for spanID, children := range parentChildMap {
		if len(children) > 0 {
			middleSpans = append(middleSpans, spanID)
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
	
	// Randomly select middle spans to remove
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
	
	fmt.Printf("  FORCED SIMULATION - Removed %d middle spans (%.1f%% of middle spans) from trace %s\n", 
		numToRemove, lossPercentage*100, trace.TraceID)
	fmt.Printf("  FORCED SIMULATION - Root spans: %d, Middle spans: %d\n", 
		len(rootSpans), len(parentChildMap))
	
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
	
	// Attempt to reconnect each orphaned span
	reconnectedCount := 0
	for _, orphanedSpan := range orphanedSpans {
		// Extract bloom filter from orphaned span
		var bloomFilterStr string
		for _, tag := range orphanedSpan.Tags {
			if tag.Key == "__bag.bloom_filter" {
				bloomFilterStr = tag.Value.(string)
				break
			}
		}
		
		if bloomFilterStr == "" {
			continue // No bloom filter available
		}
		
		// Deserialize bloom filter
		bf, err := deserializeBloomFilter(bloomFilterStr)
		if err != nil {
			continue // Failed to deserialize
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
			continue // No missing parent found
		}
		
		// Find the lowest (deepest) existing span that is contained in the bloom filter
		var bestAncestor string
		var bestDepth = -1
		var bestConfidence float64
		
		for existingSpanID := range spanIDMap {
			if bf.Test([]byte(existingSpanID)) {
				depth := spanDepthMap[existingSpanID]
				if depth > bestDepth {
					bestDepth = depth
					bestAncestor = existingSpanID
					// Calculate confidence based on bloom filter containment
					bestConfidence = loader.calculateReconnectionConfidence(bf, existingSpanID, orphanedSpan.SpanID)
				}
			}
		}
		
		if bestAncestor != "" {
			// Create bridge edge
			bridgeEdge := BridgeEdge{
				FromSpanID:     bestAncestor,
				ToSpanID:       orphanedSpan.SpanID,
				OriginalParent: missingParentID,
				Depth:          spanDepthMap[orphanedSpan.SpanID],
				Confidence:     bestConfidence,
			}
			result.BridgeEdges = append(result.BridgeEdges, bridgeEdge)
			
			// Update the orphaned span's references to point to the best ancestor
			for i := range reconnectedTrace.Spans {
				if reconnectedTrace.Spans[i].SpanID == orphanedSpan.SpanID {
					// Update the reference
					for j := range reconnectedTrace.Spans[i].References {
						if reconnectedTrace.Spans[i].References[j].RefType == "CHILD_OF" && 
						   reconnectedTrace.Spans[i].References[j].SpanID == missingParentID {
							reconnectedTrace.Spans[i].References[j].SpanID = bestAncestor
						}
					}
					
					// Add a special tag indicating this is a bridge connection
					reconnectedTrace.Spans[i].Tags = append(reconnectedTrace.Spans[i].Tags, Tag{
						Key:   "bridge.reconnected",
						Value: true,
						Type:  "bool",
					})
					reconnectedTrace.Spans[i].Tags = append(reconnectedTrace.Spans[i].Tags, Tag{
						Key:   "bridge.original_parent",
						Value: missingParentID,
						Type:  "string",
					})
					reconnectedTrace.Spans[i].Tags = append(reconnectedTrace.Spans[i].Tags, Tag{
						Key:   "bridge.confidence",
						Value: bestConfidence,
						Type:  "float64",
					})
					break
				}
			}
			
			reconnectedCount++
		}
	}
	
	result.ReconnectedTrace = reconnectedTrace
	result.ReconnectionRate = float64(reconnectedCount) / float64(len(orphanedSpans)) * 100
	result.Success = reconnectedCount > 0
	
	fmt.Printf("  RECONNECTION - Successfully reconnected %d/%d orphaned spans (%.1f%%)\n", 
		reconnectedCount, len(orphanedSpans), result.ReconnectionRate)
	
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
		file.Seek(0, 0)
		var traces []*JaegerTrace
		decoder = json.NewDecoder(file)
		if err2 := decoder.Decode(&traces); err2 != nil {
			return nil, fmt.Errorf("failed to unmarshal traces (tried both formats): %w, %w", err, err2)
		}
		return traces, nil
	}
	
	return response.Data, nil
}
