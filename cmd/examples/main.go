package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"agentic-architectures/pkg/architectures"

	"github.com/cloudwego/eino-ext/components/model/claude"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/components/tool/utils"
	"github.com/cloudwego/eino/schema"
	"github.com/spf13/cobra"
)

const (
	ansiReset   = "\x1b[0m"
	ansiBold    = "\x1b[1m"
	ansiWhite   = "\x1b[37m"
	ansiCyan    = "\x1b[36m"
	ansiYellow  = "\x1b[33m"
	ansiBlue    = "\x1b[34m"
	ansiMagenta = "\x1b[35m"
	ansiGreen   = "\x1b[32m"
	ansiRed     = "\x1b[31m"
)

var (
	architectureName string
	task             string
)

func main() {
	if err := newRootCommand().ExecuteContext(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func newRootCommand() *cobra.Command {
	root := &cobra.Command{
		Use:   "agentic-architectures",
		Short: "Run a streaming agent demo",
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(cmd.Context())
		},
	}

	root.Flags().StringVarP(&architectureName, "architecture", "a", "react", "react or reflection")
	root.Flags().StringVarP(&task, "task", "t", "", "Task to run")

	return root
}

func run(ctx context.Context) error {
	model, err := buildModel(ctx)
	if err != nil {
		return err
	}

	arch, defaultTask, err := buildArchitecture(strings.TrimSpace(architectureName), model)
	if err != nil {
		return err
	}

	if task == "" {
		task = defaultTask
	}

	fmt.Printf("%s\n%s\n\n", strings.ToUpper(arch.Name()), arch.Description())
	fmt.Printf("User: %s\n\n", task)

	stream, err := arch.Stream(ctx, task)
	if err != nil {
		return err
	}
	result, err := renderStream(stream)
	if err != nil {
		return err
	}

	if result == nil {
		return fmt.Errorf("stream ended without a final result")
	}

	fmt.Printf("\n\n%sFinal Answer:%s\n%s\n", ansiBold+ansiGreen, ansiReset, colorize(result.Output, ansiGreen))
	return nil
}

func buildModel(ctx context.Context) (*claude.ChatModel, error) {
	apiKey := os.Getenv("ANTHROPIC_API_KEY")
	if apiKey == "" {
		return nil, fmt.Errorf("ANTHROPIC_API_KEY not set")
	}

	return claude.NewChatModel(ctx, &claude.Config{
		APIKey:    apiKey,
		Model:     "claude-sonnet-4-6",
		MaxTokens: 4096,
		Thinking:  &claude.Thinking{Enable: true, BudgetTokens: 2048},
	})
}

func buildArchitecture(name string, model *claude.ChatModel) (architectures.Architecture, string, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "react":
		calc, err := newAddTool()
		if err != nil {
			return nil, "", err
		}
		return architectures.NewReAct(model, []tool.BaseTool{calc}), "What is 25 plus 17?", nil
	case "reflection":
		return architectures.NewReflection(model), "Write a concise explanation of why Go is useful for CLI tools.", nil
	default:
		return nil, "", fmt.Errorf("unknown architecture %q", name)
	}
}

type addArgs struct {
	A int `json:"a"`
	B int `json:"b"`
}

func newAddTool() (tool.BaseTool, error) {
	return utils.InferTool("add", "Add two numbers together.", func(_ context.Context, input addArgs) (string, error) {
		return fmt.Sprintf("%d", input.A+input.B), nil
	})
}

func renderStream(stream *schema.StreamReader[architectures.StreamEvent]) (*architectures.ArchitectureResult, error) {
	defer stream.Close()

	var (
		result        *architectures.ArchitectureResult
		activeSection string
	)

	printSection := func(section, label, color string) {
		if activeSection == section {
			return
		}
		activeSection = section
		fmt.Printf("\n%s%s:%s ----------\n", ansiBold+color, label, ansiReset)
	}

	for {
		event, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		switch event.Type {
		case "thinking":
			if event.Content == "" {
				continue
			}
			printSection("thinking", "Thinking", ansiCyan)
			fmt.Print(colorize(event.Content, ansiCyan))
		case "tool_call":
			if event.Content == "" {
				continue
			}
			printSection("tool_call", "Tool Call", ansiYellow)
			fmt.Print(colorize(event.Content, ansiYellow))
		case "tool_result":
			if event.Content == "" {
				continue
			}
			printSection("tool_result", "Tool Result", ansiBlue)
			fmt.Print(colorize(event.Content, ansiBlue))
		case "draft":
			if event.Content == "" {
				continue
			}
			printSection("draft", "Draft", ansiWhite)
			fmt.Print(colorize(event.Content, ansiWhite))
		case "critique":
			if event.Content == "" {
				continue
			}
			printSection("critique", "Critique", ansiMagenta)
			fmt.Print(colorize(event.Content, ansiMagenta))
		case "revision":
			if event.Content == "" {
				continue
			}
			printSection("revision", "Revision", ansiGreen)
			fmt.Print(colorize(event.Content, ansiGreen))
		case "stream_chunk":
			activeSection = "stream_chunk"
			fmt.Print(colorize(event.Content, ansiWhite))
		case "result":
			result = event.Result
		case "error":
			if event.Error != "" {
				return nil, fmt.Errorf("%s", colorize(event.Error, ansiRed))
			}
			return nil, fmt.Errorf("%sstream failed%s", ansiRed, ansiReset)
		}
	}

	return result, nil
}

func colorize(text, color string) string {
	if text == "" {
		return ""
	}
	return color + text + ansiReset
}
