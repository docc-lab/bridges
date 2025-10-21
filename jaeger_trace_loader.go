package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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
	
	var traces []*JaegerTrace
	if err := json.Unmarshal(body, &traces); err != nil {
		return nil, fmt.Errorf("failed to unmarshal traces: %w", err)
	}
	
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
	
	// Check each span for missing parent references
	for _, span := range trace.Spans {
		// Check if this span has a parent that doesn't exist
		if span.ParentSpanID != "" && !spanIDs[span.ParentSpanID] {
			missingSpanIDs = append(missingSpanIDs, span.ParentSpanID)
			orphanSpans = append(orphanSpans, span.SpanID)
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
}
