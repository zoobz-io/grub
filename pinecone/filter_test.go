package pinecone

import (
	"errors"
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
	f := b.Where("Category").Eq("test")

	result, err := translateFilter(f)
	if err != nil {
		t.Fatalf("translateFilter failed: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

func TestTranslateFilter_Ne(t *testing.T) {
	b := mustBuilder(t)
	f := b.Where("Status").Ne("deleted")

	result, err := translateFilter(f)
	if err != nil {
		t.Fatalf("translateFilter failed: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

func TestTranslateFilter_In(t *testing.T) {
	b := mustBuilder(t)
	f := b.Where("Category").In("a", "b", "c")

	result, err := translateFilter(f)
	if err != nil {
		t.Fatalf("translateFilter failed: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
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
}

func TestTranslateFilter_And(t *testing.T) {
	b := mustBuilder(t)

	f := b.And(
		b.Where("Category").Eq("test"),
		b.Where("Status").Eq("active"),
	)

	result, err := translateFilter(f)
	if err != nil {
		t.Fatalf("translateFilter failed: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
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
}

func TestTranslateFilter_NotWithEq(t *testing.T) {
	b := mustBuilder(t)
	f := b.Not(b.Where("Deleted").Eq(true))

	result, err := translateFilter(f)
	if err != nil {
		t.Fatalf("translateFilter failed: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

func TestTranslateFilter_NotWithIn(t *testing.T) {
	b := mustBuilder(t)
	f := b.Not(b.Where("Status").In("deleted", "archived"))

	result, err := translateFilter(f)
	if err != nil {
		t.Fatalf("translateFilter failed: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

// Tests for unsupported operators - Pinecone should return ErrOperatorNotSupported

func TestTranslateFilter_GtNotSupported(t *testing.T) {
	b := mustBuilder(t)
	f := b.Where("Score").Gt(10)

	_, err := translateFilter(f)
	if err == nil {
		t.Error("expected error for unsupported Gt operator")
	}
	if !errors.Is(err, grub.ErrOperatorNotSupported) {
		t.Errorf("expected ErrOperatorNotSupported, got %v", err)
	}
}

func TestTranslateFilter_GteNotSupported(t *testing.T) {
	b := mustBuilder(t)
	f := b.Where("Score").Gte(10)

	_, err := translateFilter(f)
	if err == nil {
		t.Error("expected error for unsupported Gte operator")
	}
	if !errors.Is(err, grub.ErrOperatorNotSupported) {
		t.Errorf("expected ErrOperatorNotSupported, got %v", err)
	}
}

func TestTranslateFilter_LtNotSupported(t *testing.T) {
	b := mustBuilder(t)
	f := b.Where("Score").Lt(100)

	_, err := translateFilter(f)
	if err == nil {
		t.Error("expected error for unsupported Lt operator")
	}
	if !errors.Is(err, grub.ErrOperatorNotSupported) {
		t.Errorf("expected ErrOperatorNotSupported, got %v", err)
	}
}

func TestTranslateFilter_LteNotSupported(t *testing.T) {
	b := mustBuilder(t)
	f := b.Where("Score").Lte(100)

	_, err := translateFilter(f)
	if err == nil {
		t.Error("expected error for unsupported Lte operator")
	}
	if !errors.Is(err, grub.ErrOperatorNotSupported) {
		t.Errorf("expected ErrOperatorNotSupported, got %v", err)
	}
}

func TestTranslateFilter_LikeNotSupported(t *testing.T) {
	b := mustBuilder(t)
	f := b.Where("Name").Like("test%")

	_, err := translateFilter(f)
	if err == nil {
		t.Error("expected error for unsupported Like operator")
	}
	if !errors.Is(err, grub.ErrOperatorNotSupported) {
		t.Errorf("expected ErrOperatorNotSupported, got %v", err)
	}
}

func TestTranslateFilter_ContainsNotSupported(t *testing.T) {
	b := mustBuilder(t)
	f := b.Where("Tags").Contains("important")

	_, err := translateFilter(f)
	if err == nil {
		t.Error("expected error for unsupported Contains operator")
	}
	if !errors.Is(err, grub.ErrOperatorNotSupported) {
		t.Errorf("expected ErrOperatorNotSupported, got %v", err)
	}
}

func TestTranslateFilter_NotWithUnsupportedOperator(t *testing.T) {
	b := mustBuilder(t)
	// NOT with Gt (unsupported combination)
	f := b.Not(b.Where("Score").Gt(10))

	_, err := translateFilter(f)
	if err == nil {
		t.Error("expected error for NOT with unsupported operator")
	}
	if !errors.Is(err, grub.ErrOperatorNotSupported) {
		t.Errorf("expected ErrOperatorNotSupported, got %v", err)
	}
}

func TestTranslateFilter_NestedLogical(t *testing.T) {
	b := mustBuilder(t)

	// (category = "test" AND status = "active") OR category = "special"
	f := b.Or(
		b.And(
			b.Where("Category").Eq("test"),
			b.Where("Status").Eq("active"),
		),
		b.Where("Category").Eq("special"),
	)

	result, err := translateFilter(f)
	if err != nil {
		t.Fatalf("translateFilter failed: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}
