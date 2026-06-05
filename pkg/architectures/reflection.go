package architectures

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
	"agentic-architectures/pkg/utils"
)

// Reflection improves a draft by critiquing and revising it.
type Reflection struct {
	model model.BaseChatModel
}

var _ Architecture = (*Reflection)(nil)

const (
	DefaultReflectionSystemPrompt = `You are a careful assistant that improves answers through critique and revision.`

	DefaultReflectionDraftInstruction = `Write a clear first draft that answers the user's task. Return only the draft.`

	DefaultReflectionCritiqueInstruction = `Critique the latest draft or revision. Return a single JSON object with keys "score" (integer 1-10) and "critique" (a concise, actionable critique).`

	DefaultReflectionReviseInstruction = `Revise the latest answer using the critique. Return only the improved final answer, with no critique labels or process commentary.`

	reflectionName        = "reflection"
	reflectionDescription = "Draft, critique, and revise the answer."
	reflectionTargetScore = 9
	reflectionMaxRounds   = 1
)

type reflectionState struct {
	Messages  []*schema.Message
	Score     int
	Critique  string
	Iteration int
	Final     string
}

type reflectionCritique struct {
	Score    int    `json:"score"`
	Critique string `json:"critique"`
}

// NewReflection creates a reflection architecture.
func NewReflection(chatModel model.BaseChatModel) *Reflection {
	return &Reflection{
		model: chatModel,
	}
}

func (r *Reflection) Name() string { return reflectionName }

func (r *Reflection) Description() string { return reflectionDescription }

func (r *Reflection) Stream(ctx context.Context, task string) (*schema.StreamReader[StreamEvent], error) {
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

		final := strings.TrimSpace(state.Final)
		if final == "" {
			final = strings.TrimSpace(utils.LastAssistantContent(state.Messages))
		}

		sw.Send(StreamEvent{
			Type: "result",
			Result: &ArchitectureResult{
				Output: final,
			},
		}, nil)
	}()

	return sr, nil
}

func (r *Reflection) buildRunnable(
	ctx context.Context,
	emit func(StreamEvent),
) (compose.Runnable[[]*schema.Message, string], *reflectionState, error) {
	state := &reflectionState{Messages: make([]*schema.Message, 0, 8)}

	graph := compose.NewGraph[[]*schema.Message, string](compose.WithGenLocalState(func(context.Context) *reflectionState {
		return state
	}))

	if err := graph.AddChatModelNode(
		"generate",
		r.model,
		compose.WithNodeName("generate"),
		compose.WithStatePreHandler(func(ctx context.Context, input []*schema.Message, state *reflectionState) ([]*schema.Message, error) {
			state.Messages = append(state.Messages, input...)
			state.Iteration = 0
			state.Score = 0
			state.Critique = ""

			messages := make([]*schema.Message, 0, len(state.Messages)+2)
			messages = append(messages, schema.SystemMessage(DefaultReflectionSystemPrompt))
			messages = append(messages, state.Messages...)
			messages = append(messages, schema.UserMessage(DefaultReflectionDraftInstruction))
			return messages, nil
		}),
		compose.WithStreamStatePostHandler(func(ctx context.Context, out *schema.StreamReader[*schema.Message], state *reflectionState) (*schema.StreamReader[*schema.Message], error) {
			return utils.DecorateMessageStream(out, "draft", emit, nil), nil
		}),
	); err != nil {
		return nil, nil, err
	}

	if err := graph.AddLambdaNode("generate_to_list", compose.ToList[*schema.Message](compose.WithLambdaType("generate_to_list"))); err != nil {
		return nil, nil, err
	}

	if err := graph.AddChatModelNode(
		"critique",
		r.model,
		compose.WithNodeName("critique"),
		compose.WithStatePreHandler(func(ctx context.Context, input []*schema.Message, state *reflectionState) ([]*schema.Message, error) {
			state.Messages = append(state.Messages, input...)

			messages := make([]*schema.Message, 0, len(state.Messages)+2)
			messages = append(messages, schema.SystemMessage(DefaultReflectionSystemPrompt))
			messages = append(messages, state.Messages...)
			messages = append(messages, schema.UserMessage(DefaultReflectionCritiqueInstruction))
			return messages, nil
		}),
		compose.WithStreamStatePostHandler(func(ctx context.Context, out *schema.StreamReader[*schema.Message], state *reflectionState) (*schema.StreamReader[*schema.Message], error) {
			return utils.DecorateMessageStream(out, "critique", emit, func(full string, last *schema.Message, _ bool) {
				parsedScore, parsedCritique := parseReflectionCritique(full)
				state.Score = parsedScore
				state.Critique = parsedCritique
				_ = last
			}), nil
		}),
	); err != nil {
		return nil, nil, err
	}

	if err := graph.AddLambdaNode("refine_input", compose.ToList[*schema.Message](compose.WithLambdaType("refine_input"))); err != nil {
		return nil, nil, err
	}

	if err := graph.AddChatModelNode(
		"refine",
		r.model,
		compose.WithNodeName("refine"),
		compose.WithStatePreHandler(func(ctx context.Context, input []*schema.Message, state *reflectionState) ([]*schema.Message, error) {
			state.Messages = append(state.Messages, input...)

			messages := make([]*schema.Message, 0, len(state.Messages)+2)
			messages = append(messages, schema.SystemMessage(DefaultReflectionSystemPrompt))
			messages = append(messages, state.Messages...)
			messages = append(messages, schema.UserMessage(DefaultReflectionReviseInstruction))
			return messages, nil
		}),
		compose.WithStreamStatePostHandler(func(ctx context.Context, out *schema.StreamReader[*schema.Message], state *reflectionState) (*schema.StreamReader[*schema.Message], error) {
			return utils.DecorateMessageStream(out, "revision", emit, func(full string, last *schema.Message, _ bool) {
				state.Iteration++
				_ = full
				_ = last
			}), nil
		}),
	); err != nil {
		return nil, nil, err
	}

	if err := graph.AddLambdaNode("refine_to_list", compose.ToList[*schema.Message](compose.WithLambdaType("refine_to_list"))); err != nil {
		return nil, nil, err
	}

	if err := graph.AddLambdaNode("finalize", compose.InvokableLambda(func(ctx context.Context, _ *schema.Message) (string, error) {
		state.Final = strings.TrimSpace(utils.LastAssistantContent(state.Messages))
		return state.Final, nil
	}, compose.WithLambdaType("finalize"))); err != nil {
		return nil, nil, err
	}

	if err := graph.AddBranch("critique", compose.NewGraphBranch(func(ctx context.Context, _ *schema.Message) (string, error) {
		if state.Score >= reflectionTargetScore || state.Iteration >= reflectionMaxRounds {
			return "finalize", nil
		}
		return "refine_input", nil
	}, map[string]bool{"refine_input": true, "finalize": true})); err != nil {
		return nil, nil, err
	}

	if err := graph.AddEdge(compose.START, "generate"); err != nil {
		return nil, nil, err
	}
	if err := graph.AddEdge("generate", "generate_to_list"); err != nil {
		return nil, nil, err
	}
	if err := graph.AddEdge("generate_to_list", "critique"); err != nil {
		return nil, nil, err
	}
	if err := graph.AddEdge("refine_input", "refine"); err != nil {
		return nil, nil, err
	}
	if err := graph.AddEdge("refine", "refine_to_list"); err != nil {
		return nil, nil, err
	}
	if err := graph.AddEdge("refine_to_list", "critique"); err != nil {
		return nil, nil, err
	}
	if err := graph.AddEdge("finalize", compose.END); err != nil {
		return nil, nil, err
	}

	compileOpts := []compose.GraphCompileOption{
		compose.WithMaxRunSteps(14),
		compose.WithNodeTriggerMode(compose.AnyPredecessor),
		compose.WithGraphName(reflectionName),
	}

	runnable, err := graph.Compile(ctx, compileOpts...)
	if err != nil {
		return nil, nil, fmt.Errorf("compile reflection graph: %w", err)
	}

	return runnable, state, nil
}

func parseReflectionCritique(text string) (int, string) {
	var payload reflectionCritique
	if err := json.Unmarshal([]byte(strings.TrimSpace(text)), &payload); err == nil && payload.Critique != "" {
		return payload.Score, strings.TrimSpace(payload.Critique)
	}

	return 0, strings.TrimSpace(text)
}
