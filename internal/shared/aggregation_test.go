package shared //nolint:revive // internal shared package is intentional

import (
	"encoding/json"
	"testing"

	"github.com/zoobz-io/lucene"
)

// testDoc is a minimal struct for creating lucene builders in tests.
type testDoc struct {
	Category  string  `json:"category"`
	Price     float64 `json:"price"`
	Score     float64 `json:"score"`
	LoadTime  float64 `json:"load_time"`
	CreatedAt string  `json:"created_at"`
}

func newBuilder() *lucene.Builder[testDoc] {
	return lucene.New[testDoc]()
}

func TestParseAggregations_Terms(t *testing.T) {
	raw := map[string]any{
		"categories": map[string]any{
			"buckets": []any{
				map[string]any{"key": "footwear", "doc_count": float64(10)},
				map[string]any{"key": "apparel", "doc_count": float64(5)},
			},
		},
	}
	qb := newBuilder()
	aggs := []lucene.Aggregation{qb.TermsAgg("categories", "category")}

	results := ParseAggregations(raw, aggs)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	r := results[0]
	if r.Name != "categories" {
		t.Errorf("expected name 'categories', got %q", r.Name)
	}
	if r.Type != lucene.AggTerms {
		t.Errorf("expected type AggTerms, got %d", r.Type)
	}
	if len(r.Buckets) != 2 {
		t.Fatalf("expected 2 buckets, got %d", len(r.Buckets))
	}
	if r.Buckets[0].Key != "footwear" {
		t.Errorf("expected key 'footwear', got %q", r.Buckets[0].Key)
	}
	if r.Buckets[0].DocCount != 10 {
		t.Errorf("expected doc_count 10, got %d", r.Buckets[0].DocCount)
	}
	if r.Buckets[1].Key != "apparel" {
		t.Errorf("expected key 'apparel', got %q", r.Buckets[1].Key)
	}
	if r.Buckets[1].DocCount != 5 {
		t.Errorf("expected doc_count 5, got %d", r.Buckets[1].DocCount)
	}
	if r.Raw == nil {
		t.Error("expected Raw to be populated")
	}
}

func TestParseAggregations_Metric(t *testing.T) {
	tests := []struct {
		name string
		agg  func(*lucene.Builder[testDoc]) lucene.Aggregation
		want lucene.AggType
	}{
		{"avg", func(qb *lucene.Builder[testDoc]) lucene.Aggregation { return qb.Avg("metric", "price") }, lucene.AggAvg},
		{"sum", func(qb *lucene.Builder[testDoc]) lucene.Aggregation { return qb.Sum("metric", "price") }, lucene.AggSum},
		{"min", func(qb *lucene.Builder[testDoc]) lucene.Aggregation { return qb.Min("metric", "price") }, lucene.AggMin},
		{"max", func(qb *lucene.Builder[testDoc]) lucene.Aggregation { return qb.Max("metric", "price") }, lucene.AggMax},
		{"count", func(qb *lucene.Builder[testDoc]) lucene.Aggregation { return qb.Count("metric", "price") }, lucene.AggCount},
		{"cardinality", func(qb *lucene.Builder[testDoc]) lucene.Aggregation { return qb.Cardinality("metric", "price") }, lucene.AggCardinality},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := map[string]any{
				"metric": map[string]any{"value": float64(42.5)},
			}
			qb := newBuilder()
			aggs := []lucene.Aggregation{tt.agg(qb)}
			results := ParseAggregations(raw, aggs)
			if len(results) != 1 {
				t.Fatalf("expected 1 result, got %d", len(results))
			}
			r := results[0]
			if r.Value == nil {
				t.Fatal("expected Value to be populated")
			}
			if *r.Value != 42.5 {
				t.Errorf("expected value 42.5, got %f", *r.Value)
			}
			if r.Buckets != nil {
				t.Error("expected Buckets to be nil for metric agg")
			}
		})
	}
}

func TestParseAggregations_Stats(t *testing.T) {
	raw := map[string]any{
		"price_stats": map[string]any{
			"count": float64(10),
			"min":   float64(1.5),
			"max":   float64(99.9),
			"avg":   float64(50.2),
			"sum":   float64(502.0),
		},
	}
	qb := newBuilder()
	aggs := []lucene.Aggregation{qb.Stats("price_stats", "price")}
	results := ParseAggregations(raw, aggs)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	r := results[0]
	if r.Stats == nil {
		t.Fatal("expected Stats to be populated")
	}
	if r.Stats.Count != 10 {
		t.Errorf("expected count 10, got %d", r.Stats.Count)
	}
	if r.Stats.Min != 1.5 {
		t.Errorf("expected min 1.5, got %f", r.Stats.Min)
	}
	if r.Stats.Max != 99.9 {
		t.Errorf("expected max 99.9, got %f", r.Stats.Max)
	}
	if r.Stats.Avg != 50.2 {
		t.Errorf("expected avg 50.2, got %f", r.Stats.Avg)
	}
	if r.Stats.Sum != 502.0 {
		t.Errorf("expected sum 502.0, got %f", r.Stats.Sum)
	}
}

func TestParseAggregations_Percentiles(t *testing.T) {
	raw := map[string]any{
		"load_time_pct": map[string]any{
			"values": map[string]any{
				"1.0":  float64(5.0),
				"50.0": float64(75.0),
				"99.0": float64(200.0),
			},
		},
	}
	qb := newBuilder()
	aggs := []lucene.Aggregation{qb.Percentiles("load_time_pct", "load_time")}
	results := ParseAggregations(raw, aggs)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	r := results[0]
	if r.Percentiles == nil {
		t.Fatal("expected Percentiles to be populated")
	}
	if len(r.Percentiles) != 3 {
		t.Errorf("expected 3 percentiles, got %d", len(r.Percentiles))
	}
	if r.Percentiles["50.0"] != 75.0 {
		t.Errorf("expected p50 75.0, got %f", r.Percentiles["50.0"])
	}
}

func TestParseAggregations_DateHistogram(t *testing.T) {
	raw := map[string]any{
		"monthly": map[string]any{
			"buckets": []any{
				map[string]any{
					"key":           float64(1704067200000),
					"key_as_string": "2024-01",
					"doc_count":     float64(15),
				},
				map[string]any{
					"key":           float64(1706745600000),
					"key_as_string": "2024-02",
					"doc_count":     float64(20),
				},
			},
		},
	}
	qb := newBuilder()
	aggs := []lucene.Aggregation{qb.DateHistogram("monthly", "created_at")}
	results := ParseAggregations(raw, aggs)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	r := results[0]
	if len(r.Buckets) != 2 {
		t.Fatalf("expected 2 buckets, got %d", len(r.Buckets))
	}
	if r.Buckets[0].KeyAsString != "2024-01" {
		t.Errorf("expected key_as_string '2024-01', got %q", r.Buckets[0].KeyAsString)
	}
	if r.Buckets[1].DocCount != 20 {
		t.Errorf("expected doc_count 20, got %d", r.Buckets[1].DocCount)
	}
}

func TestParseAggregations_SubAggs(t *testing.T) {
	raw := map[string]any{
		"categories": map[string]any{
			"buckets": []any{
				map[string]any{
					"key":       "footwear",
					"doc_count": float64(10),
					"avg_price": map[string]any{
						"value": float64(59.99),
					},
				},
				map[string]any{
					"key":       "apparel",
					"doc_count": float64(5),
					"avg_price": map[string]any{
						"value": float64(29.99),
					},
				},
			},
		},
	}
	qb := newBuilder()
	aggs := []lucene.Aggregation{
		qb.TermsAgg("categories", "category").SubAgg(qb.Avg("avg_price", "price")),
	}
	results := ParseAggregations(raw, aggs)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	r := results[0]
	if len(r.Buckets) != 2 {
		t.Fatalf("expected 2 buckets, got %d", len(r.Buckets))
	}
	bucket := r.Buckets[0]
	if len(bucket.SubAggs) != 1 {
		t.Fatalf("expected 1 sub-agg, got %d", len(bucket.SubAggs))
	}
	subAgg := bucket.SubAggs[0]
	if subAgg.Name != "avg_price" {
		t.Errorf("expected sub-agg name 'avg_price', got %q", subAgg.Name)
	}
	if subAgg.Value == nil {
		t.Fatal("expected sub-agg Value to be populated")
	}
	if *subAgg.Value != 59.99 {
		t.Errorf("expected sub-agg value 59.99, got %f", *subAgg.Value)
	}
}

func TestParseAggregations_MissingName(t *testing.T) {
	raw := map[string]any{
		"other": map[string]any{"value": float64(1)},
	}
	qb := newBuilder()
	aggs := []lucene.Aggregation{qb.Avg("missing", "price")}
	results := ParseAggregations(raw, aggs)
	if results != nil {
		t.Errorf("expected nil results, got %d", len(results))
	}
}

func TestParseAggregations_Nil(t *testing.T) {
	if results := ParseAggregations(nil, nil); results != nil {
		t.Errorf("expected nil, got %v", results)
	}
	if results := ParseAggregations(map[string]any{}, nil); results != nil {
		t.Errorf("expected nil, got %v", results)
	}
	qb := newBuilder()
	if results := ParseAggregations(nil, []lucene.Aggregation{qb.Avg("x", "price")}); results != nil {
		t.Errorf("expected nil, got %v", results)
	}
}

func TestParseAggregations_RangeBuckets(t *testing.T) {
	raw := map[string]any{
		"price_ranges": map[string]any{
			"buckets": []any{
				map[string]any{
					"key":       "*-50.0",
					"to":        float64(50),
					"doc_count": float64(3),
				},
				map[string]any{
					"key":       "50.0-100.0",
					"from":      float64(50),
					"to":        float64(100),
					"doc_count": float64(7),
				},
				map[string]any{
					"key":       "100.0-*",
					"from":      float64(100),
					"doc_count": float64(2),
				},
			},
		},
	}
	qb := newBuilder()
	aggs := []lucene.Aggregation{qb.RangeAgg("price_ranges", "price")}
	results := ParseAggregations(raw, aggs)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	r := results[0]
	if len(r.Buckets) != 3 {
		t.Fatalf("expected 3 buckets, got %d", len(r.Buckets))
	}
	if r.Buckets[1].From != float64(50) {
		t.Errorf("expected From 50, got %v", r.Buckets[1].From)
	}
	if r.Buckets[1].To != float64(100) {
		t.Errorf("expected To 100, got %v", r.Buckets[1].To)
	}
}

func TestParseAggregations_FilterAgg(t *testing.T) {
	raw := map[string]any{
		"active_docs": map[string]any{
			"doc_count": float64(42),
			"avg_score": map[string]any{
				"value": float64(8.5),
			},
		},
	}
	qb := newBuilder()
	aggs := []lucene.Aggregation{
		qb.FilterAgg("active_docs", qb.MatchAll()).SubAgg(qb.Avg("avg_score", "score")),
	}
	results := ParseAggregations(raw, aggs)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	r := results[0]
	if len(r.Buckets) != 1 {
		t.Fatalf("expected 1 bucket, got %d", len(r.Buckets))
	}
	if r.Buckets[0].DocCount != 42 {
		t.Errorf("expected doc_count 42, got %d", r.Buckets[0].DocCount)
	}
	if len(r.Buckets[0].SubAggs) != 1 {
		t.Fatalf("expected 1 sub-agg, got %d", len(r.Buckets[0].SubAggs))
	}
	if r.Buckets[0].SubAggs[0].Value == nil || *r.Buckets[0].SubAggs[0].Value != 8.5 {
		t.Error("expected sub-agg value 8.5")
	}
}

func TestToFloat64(t *testing.T) {
	tests := []struct {
		name string
		in   any
		want float64
	}{
		{"float64", float64(1.5), 1.5},
		{"float32", float32(2.5), 2.5},
		{"int", int(3), 3.0},
		{"int64", int64(4), 4.0},
		{"json.Number", json.Number("5.5"), 5.5},
		{"json.Number_invalid", json.Number("bad"), 0},
		{"nil", nil, 0},
		{"string", "nope", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := toFloat64(tt.in)
			if got != tt.want {
				t.Errorf("toFloat64(%v) = %f, want %f", tt.in, got, tt.want)
			}
		})
	}
}

func TestToInt64(t *testing.T) {
	tests := []struct {
		name string
		in   any
		want int64
	}{
		{"float64", float64(10), 10},
		{"float32", float32(20), 20},
		{"int", int(30), 30},
		{"int64", int64(40), 40},
		{"json.Number", json.Number("50"), 50},
		{"json.Number_invalid", json.Number("bad"), 0},
		{"nil", nil, 0},
		{"string", "nope", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := toInt64(tt.in)
			if got != tt.want {
				t.Errorf("toInt64(%v) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}
}

func TestParseAggregations_MetricValueMissing(t *testing.T) {
	// Metric agg with no "value" key
	raw := map[string]any{
		"metric": map[string]any{"other": float64(1)},
	}
	qb := newBuilder()
	aggs := []lucene.Aggregation{qb.Avg("metric", "price")}
	results := ParseAggregations(raw, aggs)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Value != nil {
		t.Error("expected nil Value when key is missing")
	}
}

func TestParseAggregations_PercentilesMissingValues(t *testing.T) {
	// Percentiles with no "values" key
	raw := map[string]any{
		"pct": map[string]any{"other": float64(1)},
	}
	qb := newBuilder()
	aggs := []lucene.Aggregation{qb.Percentiles("pct", "price")}
	results := ParseAggregations(raw, aggs)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Percentiles != nil {
		t.Error("expected nil Percentiles when values key is missing")
	}
}

func TestParseAggregations_PercentilesWrongType(t *testing.T) {
	// Percentiles where "values" is not a map
	raw := map[string]any{
		"pct": map[string]any{"values": "not a map"},
	}
	qb := newBuilder()
	aggs := []lucene.Aggregation{qb.Percentiles("pct", "price")}
	results := ParseAggregations(raw, aggs)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Percentiles != nil {
		t.Error("expected nil Percentiles when values is wrong type")
	}
}

func TestParseAggregations_BucketsWrongType(t *testing.T) {
	// Terms agg where "buckets" is not a slice
	raw := map[string]any{
		"cats": map[string]any{"buckets": "not a slice"},
	}
	qb := newBuilder()
	aggs := []lucene.Aggregation{qb.TermsAgg("cats", "category")}
	results := ParseAggregations(raw, aggs)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Buckets != nil {
		t.Error("expected nil Buckets when buckets value is wrong type")
	}
}

func TestParseAggregations_BucketItemWrongType(t *testing.T) {
	// Terms agg where bucket items are not maps
	raw := map[string]any{
		"cats": map[string]any{
			"buckets": []any{"not a map", 42},
		},
	}
	qb := newBuilder()
	aggs := []lucene.Aggregation{qb.TermsAgg("cats", "category")}
	results := ParseAggregations(raw, aggs)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if len(results[0].Buckets) != 0 {
		t.Errorf("expected 0 buckets, got %d", len(results[0].Buckets))
	}
}

func TestParseAggregations_RawNotMap(t *testing.T) {
	// Agg value is not a map — should be skipped
	raw := map[string]any{
		"metric": "not a map",
	}
	qb := newBuilder()
	aggs := []lucene.Aggregation{qb.Avg("metric", "price")}
	results := ParseAggregations(raw, aggs)
	if results != nil {
		t.Errorf("expected nil results when agg value is not a map, got %d", len(results))
	}
}

func TestParseAggregations_IntDocCount(t *testing.T) {
	// Test with int-typed doc_count (not float64)
	raw := map[string]any{
		"cats": map[string]any{
			"buckets": []any{
				map[string]any{"key": "a", "doc_count": int(7)},
				map[string]any{"key": "b", "doc_count": int64(3)},
			},
		},
	}
	qb := newBuilder()
	aggs := []lucene.Aggregation{qb.TermsAgg("cats", "category")}
	results := ParseAggregations(raw, aggs)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Buckets[0].DocCount != 7 {
		t.Errorf("expected doc_count 7, got %d", results[0].Buckets[0].DocCount)
	}
	if results[0].Buckets[1].DocCount != 3 {
		t.Errorf("expected doc_count 3, got %d", results[0].Buckets[1].DocCount)
	}
}
