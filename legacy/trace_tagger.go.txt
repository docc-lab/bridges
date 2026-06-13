package main

import (
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/bits-and-blooms/bloom"
)

// OTelTrace represents an OTel trace (can handle both single trace and batch format)
// Also supports Jaeger format for compatibility
type OTelTrace struct {
	TraceID           string                 `json:"traceId,omitempty"`
	TraceIDAlt        string                 `json:"traceID,omitempty"` // Alternative format / Jaeger format
	Spans             []OTelSpan             `json:"spans,omitempty"` // Both OTel and Jaeger use lowercase "spans"
	ResourceSpans     []ResourceSpan         `json:"resourceSpans,omitempty"` // OTel batch format
	Processes         map[string]interface{} `json:"processes,omitempty"` // Jaeger format
}

// ResourceSpan represents resource spans in OTel batch format
type ResourceSpan struct {
	Resource struct {
		Attributes []Attribute `json:"attributes"`
	} `json:"resource"`
	ScopeSpans []ScopeSpan `json:"scopeSpans"`
}

// ScopeSpan represents scope spans in OTel format
type ScopeSpan struct {
	Scope struct {
		Name string `json:"name"`
	} `json:"scope"`
	Spans []OTelSpan `json:"spans"`
}

// OTelSpan represents a span in OTel format
// Also supports Jaeger format fields for compatibility
type OTelSpan struct {
	TraceID       string      `json:"traceId,omitempty"` // OTel format
	TraceIDAlt    string      `json:"traceID,omitempty"` // Jaeger format
	SpanID        string      `json:"spanId,omitempty"` // OTel format
	SpanIDAlt     string      `json:"spanID,omitempty"` // Jaeger format (note: capital ID)
	ParentSpanID  string      `json:"parentSpanId,omitempty"` // OTel format
	ParentSpanIDAlt string    `json:"parentSpanID,omitempty"` // Jaeger format
	Name          string      `json:"name,omitempty"` // OTel format
	OperationName string      `json:"operationName,omitempty"` // Jaeger format
	StartTimeUnixNano uint64  `json:"startTimeUnixNano,omitempty"` // OTel format
	EndTimeUnixNano   uint64  `json:"endTimeUnixNano,omitempty"` // OTel format
	StartTime     int64       `json:"startTime,omitempty"` // Jaeger format
	Duration      int64       `json:"duration,omitempty"` // Jaeger format
	Attributes    []Attribute `json:"attributes,omitempty"` // OTel format
	Tags          []Tag       `json:"tags,omitempty"` // Jaeger format
	Status        struct {
		Code string `json:"code"`
	} `json:"status,omitempty"`
	Kind          string      `json:"kind,omitempty"`
	Links         []Link      `json:"links,omitempty"`
	Events        []Event     `json:"events,omitempty"`
	References    []Reference `json:"references,omitempty"` // Jaeger format for parent-child relationships
	ProcessID     string      `json:"processID,omitempty"` // Jaeger format
	Flags         int         `json:"flags,omitempty"` // Jaeger format
}

// Attribute represents an OTel attribute
type Attribute struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
}

// Tag represents a key-value tag (alternative format)
type Tag struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
	Type  string      `json:"type,omitempty"`
}

// Link represents a span link
type Link struct {
	TraceID string `json:"traceId"`
	SpanID  string `json:"spanId"`
}

// Reference represents a span reference (Jaeger format)
type Reference struct {
	RefType string `json:"refType"`
	TraceID string `json:"traceID"`
	SpanID  string `json:"spanID"`
}

// Event represents a span event
type Event struct {
	Name       string      `json:"name"`
	Timestamp  uint64      `json:"timeUnixNano"`
	Attributes []Attribute `json:"attributes"`
}

// TraceProcessor processes traces to add priority and ancestry tags
type TraceProcessor struct {
	PriorityDepth   int    // Depth interval for high priority spans
	AncestryMode    string // "hash", "bloom", or "hybrid"
}

// getTraceID gets trace ID from span (handles different field names)
func (tp *TraceProcessor) getTraceID(span *OTelSpan) string {
	if span.TraceID != "" {
		return span.TraceID
	}
	return span.TraceIDAlt
}

// ProcessTrace processes a single trace and adds priority/ancestry tags
func (tp *TraceProcessor) ProcessTrace(trace *OTelTrace) error {
	// Extract spans from trace (handle different formats)
	spans := tp.extractSpans(trace)
	if len(spans) == 0 {
		// Debug: check trace structure
		traceID := trace.TraceID
		if traceID == "" {
			traceID = trace.TraceIDAlt
		}
		return fmt.Errorf("no spans found in trace (traceID: %s, has Spans field: %v, len: %d, has ResourceSpans: %v)", 
			traceID, trace.Spans != nil, len(trace.Spans), len(trace.ResourceSpans) > 0)
	}

	// Group spans by trace ID (in case file contains multiple traces)
	traceGroups := make(map[string][]*OTelSpan)
	for i := range spans {
		traceID := tp.getTraceID(&spans[i])
		if traceID == "" {
			// Fallback to trace-level trace ID
			if trace.TraceID != "" {
				traceID = trace.TraceID
			} else {
				traceID = trace.TraceIDAlt
			}
		}
		if traceID == "" {
			traceID = "unknown" // Default if no trace ID found
		}
		traceGroups[traceID] = append(traceGroups[traceID], &spans[i])
	}

	// Process each trace group separately
	for _, traceSpans := range traceGroups {
		if err := tp.processSpanGroup(traceSpans); err != nil {
			return err
		}
	}

	return nil
}

// processSpanGroup processes a group of spans belonging to the same trace
func (tp *TraceProcessor) processSpanGroup(spans []*OTelSpan) error {
	// Build span tree to determine roots, leaves, and depths
	spanMap := make(map[string]*OTelSpan)
	children := make(map[string][]string)
	parentMap := make(map[string]string) // child -> parent mapping
	var rootSpans []*OTelSpan

	// First pass: build maps
	for _, span := range spans {
		spanID := tp.getSpanID(span)
		if spanID == "" {
			continue // Skip spans without ID
		}
		spanMap[spanID] = span
		parentID := tp.getParentSpanID(span)
		
		if parentID == "" {
			rootSpans = append(rootSpans, span)
		} else {
			children[parentID] = append(children[parentID], spanID)
			parentMap[spanID] = parentID
		}
	}
	
	// Check for missing parent references after all spans are added
	missingParentCount := 0
	for spanID, parentID := range parentMap {
		if _, exists := spanMap[parentID]; !exists {
			missingParentCount++
			fmt.Printf("  ERROR: Span %s references parent %s that does not exist in this trace\n", spanID, parentID)
		}
	}
	
	if missingParentCount > 0 {
		fmt.Printf("  Total missing parent references: %d\n", missingParentCount)
	}

	// Calculate depth for each span from root
	depthMap := make(map[string]int)
	var calculateDepth func(spanID string, depth int)
	calculateDepth = func(spanID string, depth int) {
		depthMap[spanID] = depth
		for _, childID := range children[spanID] {
			calculateDepth(childID, depth+1)
		}
	}

	// Start from all roots
	for _, root := range rootSpans {
		rootID := tp.getSpanID(root)
		calculateDepth(rootID, 0)
	}

	// Build path from root to each span (for ancestry data)
	pathMap := make(map[string][]string) // spanID -> path of spanIDs from root to this span
	var buildPath func(spanID string) []string
	buildPath = func(spanID string) []string {
		if path, exists := pathMap[spanID]; exists {
			return path
		}
		parentID, hasParent := parentMap[spanID]
		if !hasParent {
			// This is a root
			pathMap[spanID] = []string{spanID}
			return []string{spanID}
		}
		parentPath := buildPath(parentID)
		path := append([]string{}, parentPath...)
		path = append(path, spanID)
		pathMap[spanID] = path
		return path
	}

	// Build paths for all spans
	for spanID := range spanMap {
		buildPath(spanID)
	}

	// Identify leaves (spans with no children)
	leafSet := make(map[string]bool)
	for spanID := range spanMap {
		if len(children[spanID]) == 0 {
			leafSet[spanID] = true
		}
	}

	// First pass: mark all spans with their priority
	priorityMap := make(map[string]bool) // spanID -> isHighPriority
	for spanID, span := range spanMap {
		depth := depthMap[spanID]
		parentID := tp.getParentSpanID(span)
		isRoot := parentID == ""
		isLeaf := leafSet[spanID]
		
		// Determine priority
		highPriority := false
		if isRoot || isLeaf {
			highPriority = true
		} else if tp.PriorityDepth > 0 && depth%tp.PriorityDepth == 0 {
			highPriority = true
		}
		
		priorityMap[spanID] = highPriority
		
		// Set priority tag
		if highPriority {
			tp.setTag(span, "prio", "high")
		} else {
			tp.setTag(span, "prio", "low")
		}
	}

	// Second pass: build ancestry paths that reset at each high priority span
	// Ancestry includes only spans from the last high priority ancestor (or root) to current span
	// Reconstruction code expects: ancestry includes the span itself as last element, and parent is at idx-1
	var findLastHighPriorityAncestor func(spanID string, skipSelf bool) string
	findLastHighPriorityAncestor = func(spanID string, skipSelf bool) string {
		// Check if span exists in spanMap
		span, exists := spanMap[spanID]
		if !exists || span == nil {
			// Span not found, return empty (shouldn't happen, but handle gracefully)
			fmt.Printf("  ERROR: findLastHighPriorityAncestor: spanID %s not found in spanMap\n", spanID)
			return ""
		}
		// If skipSelf is false and this span is high priority, return it
		// If skipSelf is true, we want to skip this span and find the previous high priority
		if !skipSelf && priorityMap[spanID] {
			return spanID
		}
		// Go up to parent
		parentID := tp.getParentSpanID(span)
		if parentID == "" {
			// Reached root, return it (root is always considered high priority)
			return spanID
		}
		// Check if parent exists before recursing
		if _, exists := spanMap[parentID]; !exists {
			fmt.Printf("  ERROR: findLastHighPriorityAncestor: spanID %s references missing parent %s\n", spanID, parentID)
			// Return current spanID as fallback (treat as root)
			return spanID
		}
		// Recursively find in parent (always check parent, don't skip it)
		return findLastHighPriorityAncestor(parentID, false)
	}
	
	var buildPathFromAncestor func(spanID string, ancestorID string) []string
	buildPathFromAncestor = func(spanID string, ancestorID string) []string {
		if spanID == ancestorID {
			return []string{spanID}
		}
		// Check if span exists in spanMap
		span, exists := spanMap[spanID]
		if !exists || span == nil {
			// Span not found, return empty (shouldn't happen, but handle gracefully)
			fmt.Printf("  ERROR: buildPathFromAncestor: spanID %s not found in spanMap\n", spanID)
			return []string{}
		}
		parentID := tp.getParentSpanID(span)
		if parentID == "" {
			// Shouldn't happen, but handle gracefully
			return []string{spanID}
		}
		// Check if parent exists before recursing
		if _, exists := spanMap[parentID]; !exists {
			fmt.Printf("  ERROR: buildPathFromAncestor: spanID %s references missing parent %s\n", spanID, parentID)
			return []string{spanID}
		}
		parentPath := buildPathFromAncestor(parentID, ancestorID)
		return append(parentPath, spanID)
	}
	
	buildResetPath := func(spanID string) []string {
		// Check if span exists in spanMap
		span, exists := spanMap[spanID]
		if !exists || span == nil {
			// Span not found, return empty
			return []string{}
		}
		// Find the last high priority ancestor (or root), skipping current span
		ancestorID := findLastHighPriorityAncestor(spanID, true)
		if ancestorID == "" {
			// Couldn't find ancestor, return empty
			return []string{}
		}
		// Get parent of current span
		parentID := tp.getParentSpanID(span)
		if parentID == "" {
			// This is a root, no ancestry needed (or return empty)
			return []string{}
		}
		// Build path from ancestor to parent (NOT including current span)
		return buildPathFromAncestor(parentID, ancestorID)
	}

	// For hybrid mode: detect fan-outs and track hash assignments
	// Track hash assignments by checkpoint interval for proper reset
	// Format: checkpointID -> highPrioritySpanID -> []"spanID:depth" pairs
	hashAssignmentsByCheckpoint := make(map[string]map[string][]string)
	if tp.AncestryMode == "hybrid" {
		// Helper function to get start time for a span
		getStartTime := func(spanID string) int64 {
			span, exists := spanMap[spanID]
			if !exists {
				return 0
			}
			// Try OTel format first
			if span.StartTimeUnixNano > 0 {
				return int64(span.StartTimeUnixNano)
			}
			// Fall back to Jaeger format
			return span.StartTime
		}
		
		// Helper to find first high-priority descendant (or self if already high-priority)
		var findFirstHighPriorityDescendant func(spanID string) string
		findFirstHighPriorityDescendant = func(spanID string) string {
			if priorityMap[spanID] {
				return spanID
			}
			// Check all children, return first high-priority descendant found
			childIDs := children[spanID]
			for _, childID := range childIDs {
				if result := findFirstHighPriorityDescendant(childID); result != "" {
					return result
				}
			}
			return ""
		}
		
		// Helper to find the checkpoint for a span (its last high-priority ancestor, or itself if high-priority)
		var findCheckpoint func(spanID string) string
		findCheckpoint = func(spanID string) string {
			if priorityMap[spanID] {
				// Find last high-priority ancestor (or root)
				ancestor := findLastHighPriorityAncestor(spanID, true)
				if ancestor == "" {
					return spanID // This span is the checkpoint
				}
				return ancestor
			}
			parentID := parentMap[spanID]
			if parentID == "" {
				return spanID // Root
			}
			return findCheckpoint(parentID)
		}
		
		// Detect fan-outs and assign hash to 2nd child
		for parentID, childIDs := range children {
			if len(childIDs) > 1 {
				// Fan-out detected: sort children by start time
				type childWithTime struct {
					spanID string
					time   int64
				}
				childrenWithTime := make([]childWithTime, 0, len(childIDs))
				for _, childID := range childIDs {
					childrenWithTime = append(childrenWithTime, childWithTime{
						spanID: childID,
						time:   getStartTime(childID),
					})
				}
				
				// Sort by start time
				for i := 0; i < len(childrenWithTime)-1; i++ {
					for j := i + 1; j < len(childrenWithTime); j++ {
						if childrenWithTime[i].time > childrenWithTime[j].time {
							childrenWithTime[i], childrenWithTime[j] = childrenWithTime[j], childrenWithTime[i]
						}
					}
				}
				
				// Select 2nd child (index 1)
				if len(childrenWithTime) >= 2 {
					selectedChildID := childrenWithTime[1].spanID
					// Find first high-priority descendant
					highPrioritySpanID := findFirstHighPriorityDescendant(selectedChildID)
					if highPrioritySpanID != "" {
						// Find which checkpoint this assignment belongs to
						checkpointID := findCheckpoint(highPrioritySpanID)
						if checkpointID == "" {
							checkpointID = highPrioritySpanID // Fallback
						}
						
						// Initialize map for this checkpoint if needed
						if hashAssignmentsByCheckpoint[checkpointID] == nil {
							hashAssignmentsByCheckpoint[checkpointID] = make(map[string][]string)
						}
						
						// Get parent depth from depthMap
						parentDepth := depthMap[parentID]
						
						// Add parent spanID with depth to hash array for this high-priority span in this checkpoint interval
						// Format: "spanID:depth"
						hashEntry := fmt.Sprintf("%s:%d", parentID, parentDepth)
						hashAssignmentsByCheckpoint[checkpointID][highPrioritySpanID] = append(
							hashAssignmentsByCheckpoint[checkpointID][highPrioritySpanID], hashEntry)
					}
				}
			}
		}
	}

	// Process each span to set ancestry tags (only for high priority)
	// Also set depth tag for high priority spans only:
	// - Root span keeps its absolute depth (0)
	// - All other checkpoints keep their absolute depth
	// - Depth resets to 0 at the child of a checkpoint (even if that child is a checkpoint itself)
	for spanID, span := range spanMap {
		if priorityMap[spanID] {
			// Calculate depth tag (only for high priority spans)
			spanDepth := depthMap[spanID]
			var depthTag int
			
			// Check if this is a root span
			parentID := tp.getParentSpanID(span)
			isRoot := parentID == ""
			
			if isRoot {
				// Root always keeps absolute depth (0) - only edge case that doesn't reset
				depthTag = spanDepth
			} else {
				// For all non-root checkpoints, check if parent is a checkpoint
				parentIsCheckpoint := priorityMap[parentID]
				if parentIsCheckpoint {
					// Parent is a checkpoint, so depth resets at this child
					// Child of checkpoint gets depth = spanDepth - checkpointDepth
					checkpointDepth := depthMap[parentID]
					depthTag = spanDepth - checkpointDepth
					if depthTag < 1 {
						depthTag = 1 // Should be at least 1 for child of checkpoint
					}
				} else {
					// Parent is not a checkpoint, so this checkpoint keeps its absolute depth
					depthTag = spanDepth
				}
			}
			tp.setTag(span, "depth", fmt.Sprintf("%d", depthTag))
			tp.setTag(span, "ancestry_mode", tp.AncestryMode)
			
			// Build ancestry data with reset path (from last high priority ancestor to this span)
			path := buildResetPath(spanID)
			var ancestryData string
			
			if tp.AncestryMode == "bloom" || tp.AncestryMode == "hybrid" {
				// Hybrid mode uses bloom filter for ancestry
				ancestryData = tp.buildBloomFilterAncestry(path)
				tp.setTag(span, "ancestry", ancestryData)
			} else if tp.AncestryMode == "hash" {
				ancestryData = tp.buildHashArrayAncestry(path)
				tp.setTag(span, "ancestry", ancestryData)
			}
			
			// For hybrid mode: handle hash arrays
			if tp.AncestryMode == "hybrid" {
				// Find last checkpoint (high-priority ancestor)
				lastCheckpoint := findLastHighPriorityAncestor(spanID, true)
				if lastCheckpoint == "" {
					lastCheckpoint = spanID // Current span is checkpoint (root)
				}
				
				// Get hash assignments for this span from the current checkpoint interval
				// Hash resets at each checkpoint, so we only get assignments from this checkpoint
				var hashParents []string
				if checkpointAssignments, exists := hashAssignmentsByCheckpoint[lastCheckpoint]; exists {
					hashParents = checkpointAssignments[spanID]
				}
				
				if len(hashParents) > 0 {
					// Create hash array: comma-separated "spanID:depth" pairs
					// Example: "B:1,D:2"
					hashArray := strings.Join(hashParents, ",")
					tp.setTag(span, "hash", hashArray)
				}
				// Note: Hash automatically resets at each checkpoint because we only include
				// assignments from the current checkpoint interval (keyed by lastCheckpoint)
			}
		}
	}

	return nil
}

// extractSpans extracts spans from trace in various formats
func (tp *TraceProcessor) extractSpans(trace *OTelTrace) []OTelSpan {
	// If spans are directly in trace (Jaeger-like format)
	if len(trace.Spans) > 0 {
		return trace.Spans
	}

	// If resourceSpans format (OTel batch format)
	var spans []OTelSpan
	for _, rs := range trace.ResourceSpans {
		for _, ss := range rs.ScopeSpans {
			spans = append(spans, ss.Spans...)
		}
	}
	if len(spans) > 0 {
		return spans
	}
	
	// Debug: if no spans found, return empty slice (will trigger error message)
	return []OTelSpan{}
}

// getSpanID gets span ID from span (handles different field names)
func (tp *TraceProcessor) getSpanID(span *OTelSpan) string {
	if span.SpanID != "" {
		return span.SpanID
	}
	return span.SpanIDAlt
}

// getParentSpanID gets parent span ID from span
// Checks References array for CHILD_OF or FOLLOWS_FROM relationships (Jaeger format) or direct parentSpanID field
// FOLLOWS_FROM is used as a fallback if CHILD_OF is not present, as long as the referenced span exists
func (tp *TraceProcessor) getParentSpanID(span *OTelSpan) string {
	// First check References array for CHILD_OF (Jaeger format) - highest priority
	for _, ref := range span.References {
		if ref.RefType == "CHILD_OF" {
			return ref.SpanID
		}
	}
	// Fallback to FOLLOWS_FROM if CHILD_OF not found
	// Note: We use FOLLOWS_FROM as a valid parent relationship
	for _, ref := range span.References {
		if ref.RefType == "FOLLOWS_FROM" {
			return ref.SpanID
		}
	}
	// Fallback to direct parentSpanID fields
	if span.ParentSpanID != "" {
		return span.ParentSpanID
	}
	return span.ParentSpanIDAlt
}

// setTag sets a tag/attribute on a span (writes to both Attributes and Tags for compatibility)
func (tp *TraceProcessor) setTag(span *OTelSpan, key string, value interface{}) {
	// Check if tag already exists in Attributes and update it
	foundInAttributes := false
	for i := range span.Attributes {
		if span.Attributes[i].Key == key {
			span.Attributes[i].Value = value
			foundInAttributes = true
			break
		}
	}
	
	// Check if tag already exists in Tags and update it
	foundInTags := false
	for i := range span.Tags {
		if span.Tags[i].Key == key {
			span.Tags[i].Value = value
			foundInTags = true
			break
		}
	}

	// If tag doesn't exist in either, add it to both
	if !foundInAttributes {
		span.Attributes = append(span.Attributes, Attribute{
			Key:   key,
			Value: value,
		})
	}
	if !foundInTags {
		span.Tags = append(span.Tags, Tag{
			Key:   key,
			Value: value,
		})
	}
}

// buildBloomFilterAncestry creates a bloom filter containing all span IDs in the path and serializes it
func (tp *TraceProcessor) buildBloomFilterAncestry(path []string) string {
	// Create bloom filter sized for depth elements with 1% false positive rate
	// Use PriorityDepth as the expected number of elements (worst case ancestry path length)
	depth := uint(tp.PriorityDepth)
	if depth == 0 {
		// Fallback to reasonable default if depth not set
		depth = 10
	}
	bf := bloom.NewWithEstimates(depth, 0.01)
	
	// Add all span IDs from the ancestry path
	for _, spanID := range path {
		bf.Add([]byte(spanID))
	}
	
	// Serialize using same method as priority_processor.go
	serialized, err := serializeBloomFilter(bf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to serialize bloom filter: %v\n", err)
		return ""
	}
	
	return serialized
}

// buildHashArrayAncestry creates a comma-separated string of span IDs from root to current
func (tp *TraceProcessor) buildHashArrayAncestry(path []string) string {
	return strings.Join(path, ",")
}

// serializeBloomFilter converts a bloom filter to a base64-encoded string (matches priority_processor.go)
func serializeBloomFilter(bf *bloom.BloomFilter) (string, error) {
	data, err := bf.GobEncode()
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(data), nil
}

// loadTraceFile loads a trace file (can contain multiple traces)
func loadTraceFile(filePath string) ([]*OTelTrace, error) {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	// Try Jaeger API format with "data" wrapper first (most common)
	// Logic: json["data"] -> traces array, for each trace t: t["spans"] -> spans array
	var rawWrapper struct {
		Data []struct {
			TraceID   string          `json:"traceID"`
			Spans     json.RawMessage `json:"spans"`
			Processes json.RawMessage `json:"processes,omitempty"`
		} `json:"data"`
	}
	if err := json.Unmarshal(data, &rawWrapper); err == nil && len(rawWrapper.Data) > 0 {
		traces := make([]*OTelTrace, len(rawWrapper.Data))
		for i, rawTrace := range rawWrapper.Data {
			trace := &OTelTrace{
				TraceIDAlt: rawTrace.TraceID,
			}
			// Unmarshal spans: t["spans"]
			if len(rawTrace.Spans) > 0 {
				var spans []OTelSpan
				if err2 := json.Unmarshal(rawTrace.Spans, &spans); err2 != nil {
					return nil, fmt.Errorf("failed to unmarshal spans for trace %d: %w", i, err2)
				}
				trace.Spans = spans
			}
			// Unmarshal processes if present
			if len(rawTrace.Processes) > 0 {
				var processes map[string]interface{}
				if err2 := json.Unmarshal(rawTrace.Processes, &processes); err2 == nil {
					trace.Processes = processes
				}
			}
			traces[i] = trace
		}
		return traces, nil
	}

	// Try to parse as array of traces
	var traces []*OTelTrace
	if err := json.Unmarshal(data, &traces); err == nil && len(traces) > 0 {
		return traces, nil
	}

	// Try to parse as single trace
	var singleTrace OTelTrace
	if err := json.Unmarshal(data, &singleTrace); err == nil {
		return []*OTelTrace{&singleTrace}, nil
	}

	return nil, fmt.Errorf("failed to parse trace file: unknown format")
}

// saveTraceFile saves traces to a file
func saveTraceFile(filePath string, traces []*OTelTrace) error {
	// Wrap in "data" for Jaeger-compatible format
	wrapper := map[string]interface{}{
		"data": traces,
	}

	data, err := json.MarshalIndent(wrapper, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal traces: %w", err)
	}

	return ioutil.WriteFile(filePath, data, 0644)
}

func main() {
	var (
		inputDir     = flag.String("input", ".", "Input directory containing trace JSON files")
		outputDir    = flag.String("output", "", "Output directory for processed traces (default: tagged-{mode}-{depth})")
		priorityDepth = flag.Int("depth", 3, "Depth interval for high priority spans (default: 3)")
		ancestryMode = flag.String("mode", "hash", "Ancestry mode: 'hash', 'bloom', or 'hybrid' (default: hash)")
	)
	flag.Parse()

	// Validate ancestry mode
	if *ancestryMode != "hash" && *ancestryMode != "bloom" && *ancestryMode != "hybrid" {
		fmt.Fprintf(os.Stderr, "Error: ancestry mode must be 'hash', 'bloom', or 'hybrid'\n")
		os.Exit(1)
	}

	// Validate priority depth
	if *priorityDepth < 1 {
		fmt.Fprintf(os.Stderr, "Error: priority depth must be >= 1\n")
		os.Exit(1)
	}

	// Set default output directory if not specified (always under data/)
	if *outputDir == "" {
		*outputDir = fmt.Sprintf("data/tagged-%s-%d", *ancestryMode, *priorityDepth)
	} else if !strings.HasPrefix(*outputDir, "data/") {
		// Ensure output is always under data/
		*outputDir = filepath.Join("data", *outputDir)
	}

	// Create output directory
	if err := os.MkdirAll(*outputDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating output directory: %v\n", err)
		os.Exit(1)
	}

	// Create processor
	processor := &TraceProcessor{
		PriorityDepth: *priorityDepth,
		AncestryMode:  *ancestryMode,
	}

	fmt.Printf("Processing traces from: %s\n", *inputDir)
	fmt.Printf("Output directory: %s\n", *outputDir)
	fmt.Printf("Priority depth interval: %d\n", *priorityDepth)
	fmt.Printf("Ancestry mode: %s\n\n", *ancestryMode)

	// Find all JSON files in input directory
	files, err := filepath.Glob(filepath.Join(*inputDir, "*.json"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error finding JSON files: %v\n", err)
		os.Exit(1)
	}

	if len(files) == 0 {
		fmt.Printf("No JSON files found in %s\n", *inputDir)
		return
	}

	totalTraces := 0
	totalSpans := 0

	// Process each file
	for _, filePath := range files {
		fileName := filepath.Base(filePath)
		fmt.Printf("Processing file: %s\n", fileName)

		// Load traces from file
		traces, err := loadTraceFile(filePath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  Warning: Failed to load %s: %v\n", fileName, err)
			continue
		}

		fmt.Printf("  Found %d trace(s)\n", len(traces))

		// Process each trace
		for i, trace := range traces {
			if err := processor.ProcessTrace(trace); err != nil {
				fmt.Fprintf(os.Stderr, "  Warning: Failed to process trace %d in %s: %v\n", i, fileName, err)
				continue
			}

			// Count spans
			spans := processor.extractSpans(trace)
			totalSpans += len(spans)
			totalTraces++
		}

		// Save processed traces
		outputPath := filepath.Join(*outputDir, fileName)
		if err := saveTraceFile(outputPath, traces); err != nil {
			fmt.Fprintf(os.Stderr, "  Warning: Failed to save %s: %v\n", outputPath, err)
			continue
		}

		fmt.Printf("  Saved to: %s\n\n", outputPath)
	}

	fmt.Printf("Processing complete!\n")
	fmt.Printf("Total traces processed: %d\n", totalTraces)
	fmt.Printf("Total spans processed: %d\n", totalSpans)
}
