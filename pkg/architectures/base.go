package architectures

import (
	"context"

	"github.com/cloudwego/eino/schema"
)

// ArchitectureResult is the standardized return type for all architectures.
type ArchitectureResult struct {
	Output string `json:"output"`
}

// StreamEvent is emitted by architecture.Stream.
type StreamEvent struct {
	Type    string              `json:"type"`
	Content string              `json:"content,omitempty"`
	Result  *ArchitectureResult `json:"result,omitempty"`
	Error   string              `json:"error,omitempty"`
}

// Architecture is the small extension point for each architecture demo.
type Architecture interface {
	Name() string
	Description() string
	Stream(context.Context, string) (*schema.StreamReader[StreamEvent], error)
}
