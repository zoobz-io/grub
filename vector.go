package grub

import "github.com/google/uuid"

// Vector wraps payload T (metadata) with vector data.
type Vector[T any] struct {
	ID       uuid.UUID `json:"id"`
	Vector   []float32 `json:"vector"`
	Score    float32   `json:"score,omitempty"`
	Metadata T         `json:"metadata"`
}
