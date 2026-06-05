# Agentic Architectures in Go

Streaming implementations of two agentic reasoning patterns: **ReAct** and **Reflection**.

## Quick Start

### Prerequisites
- Go 1.23+
- `ANTHROPIC_API_KEY` environment variable set

### Installation
```bash
go build ./cmd/examples
```

### Run a Demo

**ReAct** (Reason + Act with tool use):
```bash
./examples --architecture react --task "What is 25 plus 17?"
```

**Reflection** (Draft → Critique → Revise):
```bash
./examples --architecture reflection --task "Write a concise explanation of why Go is useful for CLI tools."
```

## Architecture Overview

### ReAct
Alternates between explicit thinking and action with a graph-based approach.
- Produces a thought step
- Takes one action (tool call or final answer)
- Receives tool results and loops until done
- Supports tool calling via the model

**Stream Events**: `thinking`, `tool_call`, `tool_result`, `stream_chunk`, `result`, `error`

### Reflection
Iteratively improves answers through critique and revision.
- Generates an initial draft
- Critiques the draft with a numeric score
- Revises if below target quality (score < 9)
- Stops after quality threshold or max iterations

**Stream Events**: `draft`, `critique`, `revision`, `result`, `error`

## Project Structure

```
.
├── cmd/examples/          # CLI demo application
├── pkg/architectures/     # Architecture implementations
│   ├── base.go           # Architecture interface & types
│   ├── react.go          # ReAct implementation
│   └── reflection.go     # Reflection implementation
├── pkg/utils/            # Shared streaming utilities
│   ├── streaming.go      # Stream decoration functions
│   └── types.go          # StreamEvent & ArchitectureResult
└── go.mod               # Go module definition
```

## Core Types

### Architecture Interface
```go
type Architecture interface {
    Name() string
    Description() string
    Stream(context.Context, string) (*schema.StreamReader[StreamEvent], error)
}
```

### StreamEvent
```go
type StreamEvent struct {
    Type    string              // Event type (thinking, draft, critique, etc.)
    Content string              // Event content
    Result  *ArchitectureResult // Final result (if Type == "result")
    Error   string              // Error message (if Type == "error")
}
```

## Building an Architecture

1. Implement the `Architecture` interface
2. Return a `*schema.StreamReader[StreamEvent]` from `Stream()`
3. Send `StreamEvent` objects to emit progress
4. Use `pkg/utils` helpers for streaming message decoration

## Dependencies

- `github.com/cloudwego/eino` — Composition framework for AI workflows
- `github.com/cloudwego/eino-ext/components/model/claude` — Claude model integration
- `github.com/spf13/cobra` — CLI framework

## Development

Run tests:
```bash
go test ./...
```

Build:
```bash
go build ./...
```

## License

See LICENSE file.
