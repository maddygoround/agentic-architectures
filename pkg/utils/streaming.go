package utils

import (
	"context"
	"io"
	"strings"

	"github.com/cloudwego/eino/schema"
)

func StreamHasToolCalls(_ context.Context, sr *schema.StreamReader[*schema.Message]) (bool, error) {
	defer sr.Close()

	for {
		msg, err := sr.Recv()
		if err == io.EOF {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if len(msg.ToolCalls) > 0 {
			return true, nil
		}
	}
}

func DecorateMessageStream(
	stream *schema.StreamReader[*schema.Message],
	eventType string,
	emit func(StreamEvent),
	onFinish func(full string, last *schema.Message, hasToolCalls bool),
) *schema.StreamReader[*schema.Message] {
	var (
		full         strings.Builder
		sawToolCalls bool
	)

	return schema.StreamReaderWithConvert(stream, func(msg *schema.Message) (*schema.Message, error) {
		if msg == nil {
			return nil, nil
		}

		if emit != nil && msg.ReasoningContent != "" {
			emit(StreamEvent{Type: "thinking", Content: msg.ReasoningContent})
		}

		if emit != nil && len(msg.ToolCalls) > 0 {
			for _, toolCall := range msg.ToolCalls {
				if formatted := formatToolCall(toolCall); formatted != "" {
					emit(StreamEvent{Type: "tool_call", Content: formatted})
					sawToolCalls = true
				}
			}
		}

		if msg.Content != "" {
			if emit != nil && eventType != "" {
				emit(StreamEvent{Type: eventType, Content: msg.Content})
			}
			full.WriteString(msg.Content)
		}

		if msg.ResponseMeta != nil && msg.ResponseMeta.FinishReason != "" && onFinish != nil {
			onFinish(strings.TrimSpace(full.String()), msg, sawToolCalls)
		}

		return msg, nil
	})
}

func DecorateToolResultsStream(
	stream *schema.StreamReader[[]*schema.Message],
	emit func(StreamEvent),
) *schema.StreamReader[[]*schema.Message] {
	return schema.StreamReaderWithConvert(stream, func(msgs []*schema.Message) ([]*schema.Message, error) {
		if emit != nil {
			for _, msg := range msgs {
				if formatted := formatToolResult(msg); formatted != "" {
					emit(StreamEvent{Type: "tool_result", Content: formatted})
				}
			}
		}
		return msgs, nil
	})
}

func formatToolCall(toolCall schema.ToolCall) string {
	name := strings.TrimSpace(toolCall.Function.Name)
	args := strings.TrimSpace(toolCall.Function.Arguments)
	switch {
	case name == "" && args == "":
		return ""
	case name != "" && args != "":
		return name + "(" + args + ")"
	default:
		return strings.TrimSpace(name + " " + args)
	}
}

func formatToolResult(msg *schema.Message) string {
	if msg == nil {
		return ""
	}

	name := strings.TrimSpace(msg.ToolName)
	if name == "" {
		name = strings.TrimSpace(msg.Name)
	}

	content := strings.TrimSpace(msg.Content)
	switch {
	case name == "" && content == "":
		return ""
	case name == "":
		return content
	case content == "":
		return name
	default:
		return name + ": " + content
	}
}

func LastAssistantContent(messages []*schema.Message) string {
	for i := len(messages) - 1; i >= 0; i-- {
		msg := messages[i]
		if msg == nil {
			continue
		}
		if msg.Role == schema.Assistant && strings.TrimSpace(msg.Content) != "" {
			return strings.TrimSpace(msg.Content)
		}
	}
	return ""
}
