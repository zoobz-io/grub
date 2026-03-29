package shared //nolint:revive // internal shared package is intentional

import (
	"encoding/json"
	"fmt"

	"github.com/zoobz-io/lucene"
)

// AggResult is a single typed aggregation result.
type AggResult struct {
	// Name is the aggregation name from the query.
	Name string

	// Type is the aggregation type from lucene.
	Type lucene.AggType

	// Buckets holds bucket results for bucket aggregations.
	// Nil for metric aggregations.
	Buckets []AggBucket

	// Value holds the scalar result for metric aggregations.
	// Nil for bucket aggregations.
	Value *float64

	// Stats holds results for stats/extended_stats aggregations.
	// Nil for non-stats aggregations.
	Stats *AggStats

	// Percentiles holds percentile results keyed by percentile string.
	// Nil for non-percentile aggregations.
	Percentiles map[string]float64

	// Raw holds the original map for fallback access.
	// Always populated.
	Raw map[string]any
}

// AggBucket is a single bucket in a bucket aggregation result.
type AggBucket struct {
	// Key is the bucket key.
	Key string

	// KeyAsString is the formatted key (e.g. date strings for date_histogram).
	KeyAsString string

	// DocCount is the number of documents in the bucket.
	DocCount int64

	// From is the lower bound for range buckets.
	From any

	// To is the upper bound for range buckets.
	To any

	// SubAggs holds nested aggregation results within this bucket.
	SubAggs []AggResult
}

// AggStats holds the result of a stats or extended_stats aggregation.
type AggStats struct {
	Count int64
	Min   float64
	Max   float64
	Avg   float64
	Sum   float64
}

// ParseAggregations converts raw aggregation results into typed results
// using the aggregation definitions from the search request.
func ParseAggregations(raw map[string]any, aggs []lucene.Aggregation) []AggResult {
	if len(raw) == 0 || len(aggs) == 0 {
		return nil
	}
	results := make([]AggResult, 0, len(aggs))
	for _, agg := range aggs {
		rawAgg, ok := raw[agg.Name()]
		if !ok {
			continue
		}
		m, ok := rawAgg.(map[string]any)
		if !ok {
			continue
		}
		results = append(results, parseAggResult(agg, m))
	}
	if len(results) == 0 {
		return nil
	}
	return results
}

func parseAggResult(agg lucene.Aggregation, raw map[string]any) AggResult {
	result := AggResult{
		Name: agg.Name(),
		Type: agg.Type(),
		Raw:  raw,
	}
	switch agg.Type() {
	case lucene.AggTerms, lucene.AggHistogram, lucene.AggDateHistogram,
		lucene.AggRange, lucene.AggDateRange, lucene.AggMissing,
		lucene.AggFilter, lucene.AggFilters, lucene.AggNested:
		result.Buckets = parseBuckets(raw, agg.SubAggs())
	case lucene.AggAvg, lucene.AggSum, lucene.AggMin, lucene.AggMax,
		lucene.AggCount, lucene.AggCardinality:
		result.Value = parseMetricValue(raw)
	case lucene.AggStats, lucene.AggExtendedStats:
		result.Stats = parseStats(raw)
	case lucene.AggPercentiles:
		result.Percentiles = parsePercentiles(raw)
	}
	return result
}

func parseBuckets(raw map[string]any, subAggDefs []lucene.Aggregation) []AggBucket {
	bucketsRaw, ok := raw["buckets"]
	if !ok {
		// Filter agg has no buckets array — it is a single bucket.
		// Treat the whole raw map as one bucket.
		docCount := toInt64(raw["doc_count"])
		bucket := AggBucket{
			DocCount: docCount,
			SubAggs:  parseSubAggs(raw, subAggDefs),
		}
		return []AggBucket{bucket}
	}
	slice, ok := bucketsRaw.([]any)
	if !ok {
		return nil
	}
	buckets := make([]AggBucket, 0, len(slice))
	for _, item := range slice {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		bucket := AggBucket{
			Key:      fmt.Sprintf("%v", m["key"]),
			DocCount: toInt64(m["doc_count"]),
			SubAggs:  parseSubAggs(m, subAggDefs),
		}
		if kas, ok := m["key_as_string"]; ok {
			bucket.KeyAsString = fmt.Sprintf("%v", kas)
		}
		if from, ok := m["from"]; ok {
			bucket.From = from
		}
		if to, ok := m["to"]; ok {
			bucket.To = to
		}
		buckets = append(buckets, bucket)
	}
	return buckets
}

func parseSubAggs(raw map[string]any, subAggDefs []lucene.Aggregation) []AggResult {
	if len(subAggDefs) == 0 {
		return nil
	}
	return ParseAggregations(raw, subAggDefs)
}

func parseMetricValue(raw map[string]any) *float64 {
	v, ok := raw["value"]
	if !ok {
		return nil
	}
	f := toFloat64(v)
	return &f
}

func parseStats(raw map[string]any) *AggStats {
	return &AggStats{
		Count: toInt64(raw["count"]),
		Min:   toFloat64(raw["min"]),
		Max:   toFloat64(raw["max"]),
		Avg:   toFloat64(raw["avg"]),
		Sum:   toFloat64(raw["sum"]),
	}
}

func parsePercentiles(raw map[string]any) map[string]float64 {
	values, ok := raw["values"]
	if !ok {
		return nil
	}
	m, ok := values.(map[string]any)
	if !ok {
		return nil
	}
	result := make(map[string]float64, len(m))
	for k, v := range m {
		result[k] = toFloat64(v)
	}
	return result
}

func toFloat64(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case json.Number:
		f, err := n.Float64()
		if err != nil {
			return 0
		}
		return f
	default:
		return 0
	}
}

func toInt64(v any) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case float32:
		return int64(n)
	case int:
		return int64(n)
	case int64:
		return n
	case json.Number:
		i, err := n.Int64()
		if err != nil {
			return 0
		}
		return i
	default:
		return 0
	}
}
