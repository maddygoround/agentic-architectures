package architectures

import (
	"context"

	"github.com/cloudwego/eino/schema"
	"agentic-architectures/pkg/utils"
)

// StreamEvent is emitted by architecture.Stream.
type StreamEvent = utils.StreamEvent

// ArchitectureResult is the standardized return type for all architectures.
type ArchitectureResult = utils.ArchitectureResult

// Architecture is the small extension point for each architecture demo.
type Architecture interface {
	Name() string
	Description() string
	Stream(context.Context, string) (*schema.StreamReader[StreamEvent], error)
}
