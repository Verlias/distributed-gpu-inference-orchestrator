package models

// VLLMChatMessage is an OpenAI-compatible chat message.
type VLLMChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// VLLMChatCompletionRequest is sent to vLLM's chat completions endpoint.
type VLLMChatCompletionRequest struct {
	Model       string            `json:"model"`
	Messages    []VLLMChatMessage `json:"messages"`
	MaxTokens   int               `json:"max_tokens"`
	Temperature *float64          `json:"temperature,omitempty"`
}

// VLLMChatCompletionResponse contains the fields consumed from vLLM.
type VLLMChatCompletionResponse struct {
	Choices []struct {
		Message VLLMChatMessage `json:"message"`
	} `json:"choices"`
}
