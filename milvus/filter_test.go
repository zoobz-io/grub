package milvus

import (
	"errors"
	"strings"
	"testing"

	"github.com/zoobz-io/grub"
	"github.com/zoobz-io/vecna"
)

type testMeta struct {
	Category string
	Score    int
	Name     string
	Status   string
	Tags     []string
	Deleted  bool
	Count    int
	Field    any
}

func mustBuilder(t *testing.T) *vecna.Builder[testMeta] {
	t.Helper()
	b, err := vecna.New[testMeta]()
	if err != nil {
		t.Fatalf("failed to create builder: %v", err)
	}
	return b
}

func TestTranslateFilter_Nil(t *testing.T) {
	result, err := translateFilter(nil, "metadata")
	if err != nil {
		t.Errorf("expected no error for nil filter, got %v", err)
	}
	if result != "" {
		t.Error("expected empty string for nil filter")
	}
}

func TestTranslateFilter_InvalidFilter(t *testing.T) {
	b := mustBuilder(t)
	f := b.Where("").Eq("test") // Empty field name causes error

	_, err := translateFilter(f, "metadata")
	if err == nil {
		t.Error("expected error for invalid filter")
	}
	if !errors.Is(err, grub.ErrInvalidQuery) {
		t.Errorf("expected ErrInvalidQuery, got %v", err)
	}
}

func TestTranslateFilter_Eq(t *testing.T) {
	b := mustBuilder(t)

	tests := []struct {
		name     string
		field    string
		value    any
		expected string
	}{
		{"string", "Category", "test", `"test"`},
		{"int", "Score", 42, "42"},
		{"int64", "Score", int64(42), "42"},
		{"float64", "Score", 3.14, "3.14"},
		{"bool", "Deleted", true, "true"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := b.Where(tt.field).Eq(tt.value)
			result, err := translateFilter(f, "metadata")
			if err != nil {
				t.Fatalf("translateFilter failed: %v", err)
			}
			if !strings.Contains(result, "==") {
				t.Errorf("expected == in result, got: %s", result)
			}
			if !strings.Contains(result, tt.expected) {
				t.Errorf("expected %s in result, got: %s", tt.expected, result)
			}
		})
	}
}

func TestTranslateFilter_Ne(t *testing.T) {
	b := mustBuilder(t)
	f := b.Where("Status").Ne("deleted")

	result, err := translateFilter(f, "metadata")
	if err != nil {
		t.Fatalf("translateFilter failed: %v", err)
	}
	if !strings.Contains(result, "!=") {
		t.Errorf("expected != in result, got: %s", result)
	}
}

func TestTranslateFilter_RangeOperators(t *testing.T) {
	b := mustBuilder(t)

	tests := []struct {
		name     string
		f        *vecna.Filter
		expected string
	}{
		{"Gt", b.Where("Score").Gt(10), ">"},
		{"Gte", b.Where("Score").Gte(10), ">="},
		{"Lt", b.Where("Score").Lt(100), "<"},
		{"Lte", b.Where("Score").Lte(100), "<="},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := translateFilter(tt.f, "metadata")
			if err != nil {
				t.Fatalf("translateFilter failed: %v", err)
			}
			if !strings.Contains(result, tt.expected) {
				t.Errorf("expected %s in result, got: %s", tt.expected, result)
			}
		})
	}
}

func TestTranslateFilter_In(t *testing.T) {
	b := mustBuilder(t)
	f := b.Where("Category").In("a", "b", "c")

	result, err := translateFilter(f, "metadata")
	if err != nil {
		t.Fatalf("translateFilter failed: %v", err)
	}
	if !strings.Contains(result, " in [") {
		t.Errorf("expected 'in [' in result, got: %s", result)
	}
}

func TestTranslateFilter_InEmpty(t *testing.T) {
	b := mustBuilder(t)
	f := b.Where("Tags").In()

	result, err := translateFilter(f, "metadata")
	if err != nil {
		t.Fatalf("translateFilter failed: %v", err)
	}
	if result != "false" {
		t.Errorf("expected 'false' for empty In, got: %s", result)
	}
}

func TestTranslateFilter_Nin(t *testing.T) {
	b := mustBuilder(t)
	f := b.Where("Status").Nin("deleted", "archived")

	result, err := translateFilter(f, "metadata")
	if err != nil {
		t.Fatalf("translateFilter failed: %v", err)
	}
	if !strings.Contains(result, "not in [") {
		t.Errorf("expected 'not in [' in result, got: %s", result)
	}
}

func TestTranslateFilter_NinEmpty(t *testing.T) {
	b := mustBuilder(t)
	f := b.Where("Status").Nin()

	result, err := translateFilter(f, "metadata")
	if err != nil {
		t.Fatalf("translateFilter failed: %v", err)
	}
	if result != "true" {
		t.Errorf("expected 'true' for empty Nin, got: %s", result)
	}
}

func TestTranslateFilter_Like(t *testing.T) {
	b := mustBuilder(t)
	f := b.Where("Name").Like("test%")

	result, err := translateFilter(f, "metadata")
	if err != nil {
		t.Fatalf("translateFilter failed: %v", err)
	}
	if !strings.Contains(result, " like ") {
		t.Errorf("expected 'like' in result, got: %s", result)
	}
}

func TestTranslateFilter_Contains(t *testing.T) {
	b := mustBuilder(t)
	f := b.Where("Tags").Contains("important")

	result, err := translateFilter(f, "metadata")
	if err != nil {
		t.Fatalf("translateFilter failed: %v", err)
	}
	if !strings.Contains(result, "array_contains") {
		t.Errorf("expected 'array_contains' in result, got: %s", result)
	}
}

func TestTranslateFilter_And(t *testing.T) {
	b := mustBuilder(t)

	f := b.And(
		b.Where("Category").Eq("test"),
		b.Where("Score").Gt(50),
	)

	result, err := translateFilter(f, "metadata")
	if err != nil {
		t.Fatalf("translateFilter failed: %v", err)
	}
	if !strings.Contains(result, " and ") {
		t.Errorf("expected ' and ' in result, got: %s", result)
	}
}

func TestTranslateFilter_Or(t *testing.T) {
	b := mustBuilder(t)

	f := b.Or(
		b.Where("Status").Eq("active"),
		b.Where("Status").Eq("pending"),
	)

	result, err := translateFilter(f, "metadata")
	if err != nil {
		t.Fatalf("translateFilter failed: %v", err)
	}
	if !strings.Contains(result, " or ") {
		t.Errorf("expected ' or ' in result, got: %s", result)
	}
}

func TestTranslateFilter_Not(t *testing.T) {
	b := mustBuilder(t)
	f := b.Not(b.Where("Deleted").Eq(true))

	result, err := translateFilter(f, "metadata")
	if err != nil {
		t.Fatalf("translateFilter failed: %v", err)
	}
	if !strings.HasPrefix(result, "not (") {
		t.Errorf("expected 'not (' prefix in result, got: %s", result)
	}
}

func TestTranslateFilter_EmptyLogical(t *testing.T) {
	b := mustBuilder(t)

	t.Run("empty And", func(t *testing.T) {
		f := b.And()
		result, err := translateFilter(f, "metadata")
		if err != nil {
			t.Fatalf("translateFilter failed: %v", err)
		}
		if result != "" {
			t.Errorf("expected empty string for empty And, got: %s", result)
		}
	})

	t.Run("empty Or", func(t *testing.T) {
		f := b.Or()
		result, err := translateFilter(f, "metadata")
		if err != nil {
			t.Fatalf("translateFilter failed: %v", err)
		}
		if result != "" {
			t.Errorf("expected empty string for empty Or, got: %s", result)
		}
	})
}

func TestTranslateFilter_SingleChildLogical(t *testing.T) {
	b := mustBuilder(t)

	t.Run("single And", func(t *testing.T) {
		f := b.And(b.Where("Status").Eq("active"))
		result, err := translateFilter(f, "metadata")
		if err != nil {
			t.Fatalf("translateFilter failed: %v", err)
		}
		// Single child should not have 'and'
		if strings.Contains(result, " and ") {
			t.Errorf("single child And should not have 'and', got: %s", result)
		}
	})

	t.Run("single Or", func(t *testing.T) {
		f := b.Or(b.Where("Status").Eq("active"))
		result, err := translateFilter(f, "metadata")
		if err != nil {
			t.Fatalf("translateFilter failed: %v", err)
		}
		// Single child should not have 'or'
		if strings.Contains(result, " or ") {
			t.Errorf("single child Or should not have 'or', got: %s", result)
		}
	})
}

func TestTranslateFilter_NestedLogical(t *testing.T) {
	b := mustBuilder(t)

	// (category = "test" AND score > 50) OR status = "special"
	f := b.Or(
		b.And(
			b.Where("Category").Eq("test"),
			b.Where("Score").Gt(50),
		),
		b.Where("Status").Eq("special"),
	)

	result, err := translateFilter(f, "metadata")
	if err != nil {
		t.Fatalf("translateFilter failed: %v", err)
	}
	if !strings.Contains(result, " or ") {
		t.Errorf("expected ' or ' in result, got: %s", result)
	}
	if !strings.Contains(result, " and ") {
		t.Errorf("expected ' and ' in nested result, got: %s", result)
	}
}

func TestTranslateFilter_FieldPath(t *testing.T) {
	b := mustBuilder(t)
	f := b.Where("Category").Eq("test")

	result, err := translateFilter(f, "meta")
	if err != nil {
		t.Fatalf("translateFilter failed: %v", err)
	}
	if !strings.Contains(result, `meta["Category"]`) {
		t.Errorf("expected field path with metadata column, got: %s", result)
	}
}

func TestFormatValue(t *testing.T) {
	tests := []struct {
		input    any
		expected string
	}{
		{"hello", `"hello"`},
		{42, "42"},
		{int64(42), "42"},
		{3.14, "3.14"},
		{true, "true"},
		{false, "false"},
	}

	for _, tt := range tests {
		result := formatValue(tt.input)
		if result != tt.expected {
			t.Errorf("formatValue(%v) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}
