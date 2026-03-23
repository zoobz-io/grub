package qdrant

import (
	"errors"
	"testing"

	"github.com/zoobz-io/grub"
	"github.com/zoobz-io/vecna"
)

// Test metadata types
type testMeta struct {
	Category string
	Score    int
	Name     string
	Status   string
	Tags     []string
	Deleted  bool
	Data     any
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
	result, err := translateFilter(nil)
	if err != nil {
		t.Errorf("expected no error for nil filter, got %v", err)
	}
	if result != nil {
		t.Error("expected nil result for nil filter")
	}
}

func TestTranslateFilter_InvalidFilter(t *testing.T) {
	b := mustBuilder(t)
	f := b.Where("").Eq("test") // Empty field name causes error

	_, err := translateFilter(f)
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
		name  string
		field string
		value any
	}{
		{"string", "Category", "test"},
		{"int", "Score", 42},
		{"int64", "Score", int64(42)},
		{"float64", "Score", 3.14},
		{"bool", "Deleted", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := b.Where(tt.field).Eq(tt.value)
			result, err := translateFilter(f)
			if err != nil {
				t.Fatalf("translateFilter failed: %v", err)
			}
			if result == nil {
				t.Fatal("expected non-nil result")
			}
		})
	}
}

func TestTranslateFilter_Ne(t *testing.T) {
	b := mustBuilder(t)
	f := b.Where("Name").Ne("excluded")

	result, err := translateFilter(f)
	if err != nil {
		t.Fatalf("translateFilter failed: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	// Ne is wrapped as Must[Condition{Filter{MustNot[...]}}]
	if len(result.Must) == 0 {
		t.Error("expected Must condition wrapping Ne")
	}
}

func TestTranslateFilter_RangeOperators(t *testing.T) {
	b := mustBuilder(t)

	tests := []struct {
		name string
		f    *vecna.Filter
	}{
		{"Gt", b.Where("Score").Gt(10)},
		{"Gte", b.Where("Score").Gte(10)},
		{"Lt", b.Where("Score").Lt(100)},
		{"Lte", b.Where("Score").Lte(100)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := translateFilter(tt.f)
			if err != nil {
				t.Fatalf("translateFilter failed: %v", err)
			}
			if result == nil {
				t.Fatal("expected non-nil result")
			}
		})
	}
}

func TestTranslateFilter_RangeRequiresNumeric(t *testing.T) {
	b := mustBuilder(t)
	f := b.Where("Name").Gt("not a number")

	_, err := translateFilter(f)
	if err == nil {
		t.Error("expected error for non-numeric range value")
	}
	if !errors.Is(err, grub.ErrInvalidQuery) {
		t.Errorf("expected ErrInvalidQuery, got %v", err)
	}
}

func TestTranslateFilter_In(t *testing.T) {
	b := mustBuilder(t)

	t.Run("strings", func(t *testing.T) {
		f := b.Where("Category").In("a", "b", "c")
		result, err := translateFilter(f)
		if err != nil {
			t.Fatalf("translateFilter failed: %v", err)
		}
		if result == nil {
			t.Fatal("expected non-nil result")
		}
	})

	t.Run("ints", func(t *testing.T) {
		f := b.Where("Score").In(1, 2, 3)
		result, err := translateFilter(f)
		if err != nil {
			t.Fatalf("translateFilter failed: %v", err)
		}
		if result == nil {
			t.Fatal("expected non-nil result")
		}
	})
}

func TestTranslateFilter_InEmpty(t *testing.T) {
	b := mustBuilder(t)
	f := b.Where("Tags").In()

	_, err := translateFilter(f)
	if err == nil {
		t.Error("expected error for empty In")
	}
	if !errors.Is(err, grub.ErrInvalidQuery) {
		t.Errorf("expected ErrInvalidQuery, got %v", err)
	}
}

func TestTranslateFilter_InMixedTypes(t *testing.T) {
	b := mustBuilder(t)
	f := b.Where("Field").In("string", 42)

	_, err := translateFilter(f)
	if err == nil {
		t.Error("expected error for mixed types in In")
	}
	if !errors.Is(err, grub.ErrInvalidQuery) {
		t.Errorf("expected ErrInvalidQuery, got %v", err)
	}
}

func TestTranslateFilter_Nin(t *testing.T) {
	b := mustBuilder(t)
	f := b.Where("Status").Nin("deleted", "archived")

	result, err := translateFilter(f)
	if err != nil {
		t.Fatalf("translateFilter failed: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	// Nin is wrapped as Must[Condition{Filter{MustNot[...]}}]
	if len(result.Must) == 0 {
		t.Error("expected Must condition wrapping Nin")
	}
}

func TestTranslateFilter_Like(t *testing.T) {
	b := mustBuilder(t)
	f := b.Where("Name").Like("test%")

	result, err := translateFilter(f)
	if err != nil {
		t.Fatalf("translateFilter failed: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

func TestTranslateFilter_Contains(t *testing.T) {
	b := mustBuilder(t)
	f := b.Where("Tags").Contains("important")

	result, err := translateFilter(f)
	if err != nil {
		t.Fatalf("translateFilter failed: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

func TestTranslateFilter_And(t *testing.T) {
	b := mustBuilder(t)

	f := b.And(
		b.Where("Category").Eq("test"),
		b.Where("Score").Gt(50),
	)

	result, err := translateFilter(f)
	if err != nil {
		t.Fatalf("translateFilter failed: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if len(result.Must) != 2 {
		t.Errorf("expected 2 Must conditions, got %d", len(result.Must))
	}
}

func TestTranslateFilter_Or(t *testing.T) {
	b := mustBuilder(t)

	f := b.Or(
		b.Where("Status").Eq("active"),
		b.Where("Status").Eq("pending"),
	)

	result, err := translateFilter(f)
	if err != nil {
		t.Fatalf("translateFilter failed: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if len(result.Should) != 2 {
		t.Errorf("expected 2 Should conditions, got %d", len(result.Should))
	}
}

func TestTranslateFilter_Not(t *testing.T) {
	b := mustBuilder(t)
	f := b.Not(b.Where("Deleted").Eq(true))

	result, err := translateFilter(f)
	if err != nil {
		t.Fatalf("translateFilter failed: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if len(result.MustNot) != 1 {
		t.Errorf("expected 1 MustNot condition, got %d", len(result.MustNot))
	}
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

	result, err := translateFilter(f)
	if err != nil {
		t.Fatalf("translateFilter failed: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if len(result.Should) != 2 {
		t.Errorf("expected 2 Should conditions, got %d", len(result.Should))
	}
}

func TestTranslateFilter_UnsupportedValueType(t *testing.T) {
	b := mustBuilder(t)
	f := b.Where("Data").Eq(struct{}{})

	_, err := translateFilter(f)
	if err == nil {
		t.Error("expected error for unsupported value type")
	}
	if !errors.Is(err, grub.ErrInvalidQuery) {
		t.Errorf("expected ErrInvalidQuery, got %v", err)
	}
}
