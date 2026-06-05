package utils

type ArchitectureResult struct {
	Output string `json:"output"`
}

type StreamEvent struct {
	Type    string              `json:"type"`
	Content string              `json:"content,omitempty"`
	Result  *ArchitectureResult `json:"result,omitempty"`
	Error   string              `json:"error,omitempty"`
}
