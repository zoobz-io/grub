package grub

import (
	"errors"
	"testing"

	"github.com/zoobz-io/grub/internal/shared"
)

func TestErrorsReexported(t *testing.T) {
	// Verify that public errors are correctly re-exported from internal/shared.
	tests := []struct {
		name   string
		public error
		shared error
	}{
		{"ErrNotFound", ErrNotFound, shared.ErrNotFound},
		{"ErrDuplicate", ErrDuplicate, shared.ErrDuplicate},
		{"ErrConflict", ErrConflict, shared.ErrConflict},
		{"ErrConstraint", ErrConstraint, shared.ErrConstraint},
		{"ErrInvalidKey", ErrInvalidKey, shared.ErrInvalidKey},
		{"ErrReadOnly", ErrReadOnly, shared.ErrReadOnly},
		{"ErrTableExists", ErrTableExists, shared.ErrTableExists},
		{"ErrTableNotFound", ErrTableNotFound, shared.ErrTableNotFound},
		{"ErrTTLNotSupported", ErrTTLNotSupported, shared.ErrTTLNotSupported},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Verify public and shared errors are the same instance.
			if !errors.Is(tt.public, tt.shared) {
				t.Errorf("%s: errors.Is(public, shared) failed", tt.name)
			}
			if !errors.Is(tt.shared, tt.public) {
				t.Errorf("%s: errors.Is(shared, public) failed", tt.name)
			}
		})
	}
}

func TestErrorsNotNil(t *testing.T) {
	// Ensure all errors are actually defined.
	errs := []error{
		ErrNotFound,
		ErrDuplicate,
		ErrConflict,
		ErrConstraint,
		ErrInvalidKey,
		ErrReadOnly,
		ErrTableExists,
		ErrTableNotFound,
		ErrTTLNotSupported,
	}

	for _, err := range errs {
		if err == nil {
			t.Error("expected non-nil error")
		}
	}
}
