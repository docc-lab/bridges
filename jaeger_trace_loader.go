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

// JaegerTraceLoader handles loading traces from Jaeger backend
type JaegerTraceLoader struct {
	jaegerURL    string
	httpClient   *http.Client
	bloomFilter  *bloom.BloomFilter
	traceCache   map[string]*JaegerTrace
}

// NewJaegerTraceLoader creates a new trace loader
func NewJaegerTraceLoader(jaegerURL string) *JaegerTraceLoader {
	// Create bloom filter for efficient trace ID lookups
	// Estimate 10000 traces with 0.01 false positive rate
	filter := bloom.NewWithEstimates(10000, 0.01)
	
	return &JaegerTraceLoader{
		jaegerURL:   jaegerURL,
		httpClient:  &http.Client{Timeout: 30 * time.Second},
		bloomFilter: filter,
		traceCache:  make(map[string]*JaegerTrace),
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

// ClearCache clears the trace cache and bloom filter
func (loader *JaegerTraceLoader) ClearCache() {
	loader.traceCache = make(map[string]*JaegerTrace)
	loader.bloomFilter = bloom.NewWithEstimates(10000, 0.01)
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
	
	// Load traces for each service
	totalTraces := 0
	for _, service := range services {
		fmt.Printf("\nLoading traces for service: %s\n", service)
		traces, err := loader.LoadTraces(ctx, service, 10) // Limit to 10 traces per service
		if err != nil {
			fmt.Printf("Error loading traces for %s: %v\n", service, err)
			continue
		}
		
		serviceTraces := len(traces)
		totalTraces += serviceTraces
		fmt.Printf("Loaded %d traces for service %s\n", serviceTraces, service)
		
		// Print trace details
		for i, trace := range traces {
			if i >= 3 { // Limit output to first 3 traces
				break
			}
			fmt.Printf("  Trace %d: ID=%s, Spans=%d\n", i+1, trace.TraceID, len(trace.Spans))
		}
	}
	
	// Print total traces loaded
	fmt.Printf("\n=== SUMMARY ===\n")
	fmt.Printf("Total traces loaded: %d\n", totalTraces)
	
	// Print statistics
	stats := loader.GetTraceStats()
	fmt.Printf("\nTrace Statistics:\n")
	for key, value := range stats {
		fmt.Printf("  %s: %v\n", key, value)
	}
}
