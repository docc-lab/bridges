package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
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
	
	// Jaeger API returns an object with a "data" field containing the traces array
	var response struct {
		Data []*JaegerTrace `json:"data"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		// If that fails, try direct array unmarshaling
		var traces []*JaegerTrace
		if err2 := json.Unmarshal(body, &traces); err2 != nil {
			return nil, fmt.Errorf("failed to unmarshal traces (tried both formats): %w, %w", err, err2)
		}
		response.Data = traces
	}
	
	// Update bloom filter and cache
	for _, trace := range response.Data {
		loader.bloomFilter.Add([]byte(trace.TraceID))
		loader.traceCache[trace.TraceID] = trace
	}
	
	return response.Data, nil
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

// Helper function to get keys from a map
func getKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
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

// SimulateDataLoss removes random non-root, non-leaf spans from a trace to simulate data loss
func (loader *JaegerTraceLoader) SimulateDataLoss(trace *JaegerTrace, lossPercentage float64) *JaegerTrace {
	if lossPercentage <= 0 || lossPercentage >= 1 {
		return trace // No loss or 100% loss, return as-is
	}
	
	// First, identify root spans (no parent references) and leaf spans (no children)
	spanIDs := make(map[string]bool)
	parentSpanIDs := make(map[string]bool)
	
	// Build maps of all span IDs and parent span IDs
	for _, span := range trace.Spans {
		spanIDs[span.SpanID] = true
		
		// Check for parent references in CHILD_OF relationships
		for _, ref := range span.References {
			if ref.RefType == "CHILD_OF" {
				parentSpanIDs[ref.SpanID] = true
			}
		}
		
		// Also check ParentSpanID field for backward compatibility
		if span.ParentSpanID != "" {
			parentSpanIDs[span.ParentSpanID] = true
		}
	}
	
	// Identify eligible spans (non-root, non-leaf)
	var eligibleSpans []int
	for i, span := range trace.Spans {
		isRoot := true
		isLeaf := true
		
		// Check if it's a root span (no parent references)
		for _, ref := range span.References {
			if ref.RefType == "CHILD_OF" {
				isRoot = false
				break
			}
		}
		if span.ParentSpanID != "" {
			isRoot = false
		}
		
		// Check if it's a leaf span (no children reference it)
		if parentSpanIDs[span.SpanID] {
			isLeaf = false
		}
		
		// Only include non-root, non-leaf spans
		if !isRoot && !isLeaf {
			eligibleSpans = append(eligibleSpans, i)
		}
	}
	
	if len(eligibleSpans) == 0 {
		fmt.Printf("  SIMULATION - No eligible spans to remove (all are root or leaf spans)\n")
		return trace
	}
	
	// Calculate how many eligible spans to remove
	spansToRemove := int(float64(len(eligibleSpans)) * lossPercentage)
	if spansToRemove == 0 {
		spansToRemove = 1 // Remove at least one if we have eligible spans
	}
	
	if spansToRemove > len(eligibleSpans) {
		spansToRemove = len(eligibleSpans)
	}
	
	// Randomly select which eligible spans to remove
	rand.Seed(time.Now().UnixNano())
	spansToRemoveSet := make(map[int]bool)
	
	// Randomly select spans to remove from eligible spans
	for len(spansToRemoveSet) < spansToRemove {
		idx := rand.Intn(len(eligibleSpans))
		spansToRemoveSet[eligibleSpans[idx]] = true
	}
	
	// Create a copy of the trace with selected spans removed
	simulatedTrace := &JaegerTrace{
		TraceID:   trace.TraceID,
		Processes: trace.Processes,
		Warnings:  trace.Warnings,
		Spans:     make([]JaegerSpan, 0, len(trace.Spans)-spansToRemove),
	}
	
	// Add spans that are not marked for removal
	for i, span := range trace.Spans {
		if !spansToRemoveSet[i] {
			simulatedTrace.Spans = append(simulatedTrace.Spans, span)
		}
	}
	
	fmt.Printf("  SIMULATION - Removed %d non-root/non-leaf spans (%.1f%% of eligible) from trace %s\n", 
		spansToRemove, lossPercentage*100, trace.TraceID)
	
	return simulatedTrace
}

// SimulateDataLossForTrace simulates data loss for a specific trace by removing random spans
func (loader *JaegerTraceLoader) SimulateDataLossForTrace(traceID string, lossPercentage float64) (*JaegerTrace, error) {
	// Get the original trace
	originalTrace, exists := loader.traceCache[traceID]
	if !exists {
		return nil, fmt.Errorf("trace %s not found in cache", traceID)
	}
	
	// Simulate data loss
	simulatedTrace := loader.SimulateDataLoss(originalTrace, lossPercentage)
	
	// Cache the simulated trace with a modified ID
	simulatedTraceID := fmt.Sprintf("%s_simulated_%.0f", traceID, lossPercentage*100)
	simulatedTrace.TraceID = simulatedTraceID
	loader.traceCache[simulatedTraceID] = simulatedTrace
	
	return simulatedTrace, nil
}

// TestDataLossDetection tests the data loss detection with simulated data loss
func (loader *JaegerTraceLoader) TestDataLossDetection(traceID string, lossPercentage float64) (*ProcessedTrace, error) {
	// Simulate data loss
	simulatedTrace, err := loader.SimulateDataLossForTrace(traceID, lossPercentage)
	if err != nil {
		return nil, err
	}
	
	// Process the simulated trace for data loss detection
	processedTrace := loader.ProcessTrace(simulatedTrace)
	
	return processedTrace, nil
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
	traceMap := make(map[string]*JaegerTrace) // Use map to deduplicate by trace ID
	
	for _, service := range services {
		fmt.Printf("\nLoading traces for service: %s\n", service)
		traces, err := loader.LoadTraces(ctx, service, 10) // Limit to 10 traces per service
		if err != nil {
			fmt.Printf("Error loading traces for %s: %v\n", service, err)
			continue
		}
		
		serviceTraces := len(traces)
		totalTraces += serviceTraces
		
		// Add traces to map (deduplicates by trace ID)
		duplicates := 0
		for _, trace := range traces {
			if _, exists := traceMap[trace.TraceID]; exists {
				duplicates++
			} else {
				traceMap[trace.TraceID] = trace
			}
		}
		
		fmt.Printf("Loaded %d traces for service %s (duplicates: %d)\n", serviceTraces, service, duplicates)
		
		// Print trace details
		for i, trace := range traces {
			if i >= 3 { // Limit output to first 3 traces
				break
			}
			fmt.Printf("  Trace %d: ID=%s, Spans=%d\n", i+1, trace.TraceID, len(trace.Spans))
		}
	}
	
	// Convert map to slice for processing
	var allTraces []*JaegerTrace
	for _, trace := range traceMap {
		allTraces = append(allTraces, trace)
	}
	
	fmt.Printf("\nDeduplication: %d total loaded, %d unique traces\n", totalTraces, len(allTraces))
	
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
	
	// Test data loss simulation if we have traces
	if len(allTraces) > 0 {
		fmt.Printf("\n=== TESTING DATA LOSS SIMULATION ===\n")
		
		// Test with the first trace that has multiple spans
		var testTrace *JaegerTrace
		for _, trace := range allTraces {
			if len(trace.Spans) > 5 { // Pick a trace with multiple spans
				testTrace = trace
				break
			}
		}
		
		if testTrace != nil {
			fmt.Printf("Testing data loss simulation on trace: %s (%d spans)\n", testTrace.TraceID, len(testTrace.Spans))
			
			// Test different loss percentages
			lossPercentages := []float64{0.1, 0.2, 0.3, 0.5} // 10%, 20%, 30%, 50%
			
			for _, lossPct := range lossPercentages {
				fmt.Printf("\n--- Simulating %.0f%% data loss ---\n", lossPct*100)
				processed, err := loader.TestDataLossDetection(testTrace.TraceID, lossPct)
				if err != nil {
					fmt.Printf("Error simulating data loss: %v\n", err)
					continue
				}
				
				fmt.Printf("Simulated trace ID: %s\n", processed.Trace.TraceID)
				fmt.Printf("Original spans: %d, Simulated spans: %d\n", len(testTrace.Spans), len(processed.Trace.Spans))
				fmt.Printf("Data loss detected: %v\n", processed.DataLoss.HasDataLoss)
				if processed.DataLoss.HasDataLoss {
					fmt.Printf("Missing span IDs: %v\n", processed.DataLoss.MissingSpanIDs)
					fmt.Printf("Orphan spans: %v\n", processed.DataLoss.OrphanSpans)
					fmt.Printf("Loss severity: %s\n", processed.DataLoss.LossSeverity)
				}
			}
		} else {
			fmt.Printf("No suitable trace found for data loss simulation (need traces with >5 spans)\n")
		}
	}
}
