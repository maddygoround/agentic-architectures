package architectures

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
)

// ReAct alternates explicit thinking and acting with a graph.
type ReAct struct {
	model model.ToolCallingChatModel
	tools []tool.BaseTool
}

var _ Architecture = (*ReAct)(nil)

const (
	DefaultReActSystemPrompt = `You are a simple ReAct assistant. Think first, then act. For arithmetic, use the add tool. Keep the final answer concise.`

	reactThinkInstruction = `Produce exactly one short paragraph beginning with the literal word "Thought:" that reflects on what you have learned and states the next step. Do not call tools here.`
	reactActInstruction   = `Based on the latest Thought, take exactly one action: call one tool or write the final answer.`

	reactName        = "react"
	reactDescription = "Reason and act with an explicit think -> act -> tools graph."
)

type reactState struct {
	Messages []*schema.Message
}

// NewReAct creates a new ReAct architecture instance.
func NewReAct(chatModel model.ToolCallingChatModel, tools []tool.BaseTool) *ReAct {
	return &ReAct{
		model: chatModel,
		tools: tools,
	}
}

// Name returns the architecture identifier.
func (r *ReAct) Name() string { return reactName }

// Description returns the architecture description.
func (r *ReAct) Description() string { return reactDescription }

func (r *ReAct) Stream(ctx context.Context, task string) (*schema.StreamReader[StreamEvent], error) {
	sr, sw := schema.Pipe[StreamEvent](8)

	go func() {
		defer sw.Close()

		runnable, state, err := r.buildRunnable(ctx, func(event StreamEvent) {
			sw.Send(event, nil)
		})
		if err != nil {
			sw.Send(StreamEvent{Type: "error", Error: err.Error()}, nil)
			return
		}

		stream, err := runnable.Stream(ctx, []*schema.Message{schema.UserMessage(task)})
		if err != nil {
			sw.Send(StreamEvent{Type: "error", Error: err.Error()}, nil)
			return
		}
		defer stream.Close()

		for {
			_, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				sw.Send(StreamEvent{Type: "error", Error: err.Error()}, nil)
				return
			}
		}

		sw.Send(StreamEvent{
			Type: "result",
			Result: &ArchitectureResult{
				Output: strings.TrimSpace(lastAssistantContent(state.Messages)),
			},
		}, nil)
	}()

	return sr, nil
}

func (r *ReAct) buildRunnable(
	ctx context.Context,
	emit func(StreamEvent),
) (compose.Runnable[[]*schema.Message, *schema.Message], *reactState, error) {
	state := &reactState{Messages: make([]*schema.Message, 0, 34)}

	graph := compose.NewGraph[[]*schema.Message, *schema.Message](compose.WithGenLocalState(func(context.Context) *reactState {
		return state
	}))

	if err := graph.AddChatModelNode(
		"think",
		r.model,
		compose.WithNodeName("think"),
		compose.WithStatePreHandler(func(ctx context.Context, input []*schema.Message, state *reactState) ([]*schema.Message, error) {
			state.Messages = append(state.Messages, input...)
			messages := make([]*schema.Message, 0, len(state.Messages)+2)
			messages = append(messages, schema.SystemMessage(DefaultReActSystemPrompt))
			messages = append(messages, state.Messages...)
			messages = append(messages, schema.UserMessage(reactThinkInstruction))
			return messages, nil
		}),
		compose.WithStreamStatePostHandler(func(ctx context.Context, out *schema.StreamReader[*schema.Message], state *reactState) (*schema.StreamReader[*schema.Message], error) {
			return decorateMessageStream(out, "thinking", emit, nil), nil
		}),
	); err != nil {
		return nil, nil, err
	}

	if err := graph.AddLambdaNode("think_to_list", compose.ToList[*schema.Message](compose.WithLambdaType("think_to_list"))); err != nil {
		return nil, nil, err
	}

	actModel := r.model
	hasTools := len(r.tools) > 0
	if hasTools {
		toolInfos, err := toolInfosFromBaseTools(ctx, r.tools)
		if err != nil {
			return nil, nil, err
		}
		withTools, err := r.model.WithTools(toolInfos)
		if err != nil {
			return nil, nil, err
		}
		actModel = withTools
	}

	if err := graph.AddChatModelNode(
		"act",
		actModel,
		compose.WithNodeName("act"),
		compose.WithStatePreHandler(func(ctx context.Context, input []*schema.Message, state *reactState) ([]*schema.Message, error) {
			state.Messages = append(state.Messages, input...)
			messages := make([]*schema.Message, 0, len(state.Messages)+2)
			messages = append(messages, schema.SystemMessage(DefaultReActSystemPrompt))
			messages = append(messages, state.Messages...)
			messages = append(messages, schema.UserMessage(reactActInstruction))
			return messages, nil
		}),
		compose.WithStreamStatePostHandler(func(ctx context.Context, out *schema.StreamReader[*schema.Message], state *reactState) (*schema.StreamReader[*schema.Message], error) {
			return decorateMessageStream(out, "stream_chunk", emit, func(full string, last *schema.Message, sawToolCalls bool) {
				if sawToolCalls {
					return
				}
				if strings.TrimSpace(full) == "" {
					return
				}
				state.Messages = append(state.Messages, &schema.Message{
					Role:             schema.Assistant,
					Content:          full,
					ResponseMeta:     last.ResponseMeta,
					ReasoningContent: last.ReasoningContent,
				})
			}), nil
		}),
	); err != nil {
		return nil, nil, err
	}

	if hasTools {
		toolsNode, err := compose.NewToolNode(ctx, &compose.ToolsNodeConfig{Tools: r.tools})
		if err != nil {
			return nil, nil, err
		}

		if err := graph.AddToolsNode(
			"tools",
			toolsNode,
			compose.WithNodeName("tools"),
			compose.WithStatePreHandler(func(ctx context.Context, input *schema.Message, state *reactState) (*schema.Message, error) {
				state.Messages = append(state.Messages, input)
				return input, nil
			}),
			compose.WithStreamStatePostHandler(func(ctx context.Context, out *schema.StreamReader[[]*schema.Message], state *reactState) (*schema.StreamReader[[]*schema.Message], error) {
				return decorateToolResultsStream(out, emit), nil
			}),
		); err != nil {
			return nil, nil, err
		}

		if err := graph.AddBranch("act", compose.NewStreamGraphBranch(func(ctx context.Context, sr *schema.StreamReader[*schema.Message]) (string, error) {
			hasToolCalls, err := streamHasToolCalls(ctx, sr)
			if err != nil {
				return "", err
			}
			if hasToolCalls {
				return "tools", nil
			}
			return compose.END, nil
		}, map[string]bool{"tools": true, compose.END: true})); err != nil {
			return nil, nil, err
		}
		if err := graph.AddEdge("tools", "think"); err != nil {
			return nil, nil, err
		}
	} else {
		if err := graph.AddEdge("act", compose.END); err != nil {
			return nil, nil, err
		}
	}

	if err := graph.AddEdge(compose.START, "think"); err != nil {
		return nil, nil, err
	}
	if err := graph.AddEdge("think", "think_to_list"); err != nil {
		return nil, nil, err
	}
	if err := graph.AddEdge("think_to_list", "act"); err != nil {
		return nil, nil, err
	}

	compileOpts := []compose.GraphCompileOption{
		compose.WithMaxRunSteps(40),
		compose.WithNodeTriggerMode(compose.AnyPredecessor),
		compose.WithGraphName(reactName),
	}

	runnable, err := graph.Compile(ctx, compileOpts...)
	if err != nil {
		return nil, nil, fmt.Errorf("compile react graph: %w", err)
	}

	return runnable, state, nil
}

func toolInfosFromBaseTools(ctx context.Context, tools []tool.BaseTool) ([]*schema.ToolInfo, error) {
	infos := make([]*schema.ToolInfo, 0, len(tools))
	for _, t := range tools {
		info, err := t.Info(ctx)
		if err != nil {
			return nil, err
		}
		infos = append(infos, info)
	}
	return infos, nil
}
