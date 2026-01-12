package services

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type OpenAIMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
	ToolCalls []ToolCall `json:"tool_calls,omitempty"`
	ToolCallID string `json:"tool_call_id,omitempty"`
}

type OpenAIClient struct {
	APIKey string
	Model  string
	HTTP   *http.Client
}

type chatRequest struct {
	Model      string          `json:"model"`
	Messages   []OpenAIMessage  `json:"messages"`
	Stream     bool            `json:"stream,omitempty"`
	Tools      []OpenAITool     `json:"tools,omitempty"`
	ToolChoice any             `json:"tool_choice,omitempty"`
}

type OpenAITool struct {
	Type     string             `json:"type"`
	Function OpenAIToolFunction `json:"function"`
}

type OpenAIToolFunction struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	Parameters  map[string]any `json:"parameters"`
}

type chatResponse struct {
	Choices []struct {
		Message OpenAIMessage `json:"message"`
	} `json:"choices"`
}

type ToolCall struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

type streamChunk struct {
	Choices []struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
	} `json:"choices"`
}

func (c *OpenAIClient) httpClient() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return http.DefaultClient
}

func (c *OpenAIClient) Chat(messages []OpenAIMessage) (string, error) {
	payload := chatRequest{Model: c.Model, Messages: messages}
	buf, _ := json.Marshal(payload)

	req, err := http.NewRequest(http.MethodPost, "https://api.openai.com/v1/chat/completions", bytes.NewReader(buf))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+c.APIKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient().Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("openai request failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var out chatResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return "", fmt.Errorf("openai invalid json: %w", err)
	}
	if len(out.Choices) == 0 {
		return "", errors.New("openai: empty choices")
	}
	return out.Choices[0].Message.Content, nil
}

func (c *OpenAIClient) ChatWithTools(messages []OpenAIMessage, tools []OpenAITool) (OpenAIMessage, error) {
	return c.ChatWithToolsChoice(messages, tools, "auto")
	}

func (c *OpenAIClient) ChatWithToolsChoice(messages []OpenAIMessage, tools []OpenAITool, toolChoice any) (OpenAIMessage, error) {
	payload := chatRequest{Model: c.Model, Messages: messages, Tools: tools, ToolChoice: toolChoice}
	buf, _ := json.Marshal(payload)

	req, err := http.NewRequest(http.MethodPost, "https://api.openai.com/v1/chat/completions", bytes.NewReader(buf))
	if err != nil {
		return OpenAIMessage{}, err
	}
	req.Header.Set("Authorization", "Bearer "+c.APIKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient().Do(req)
	if err != nil {
		return OpenAIMessage{}, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return OpenAIMessage{}, fmt.Errorf("openai request failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var out chatResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return OpenAIMessage{}, fmt.Errorf("openai invalid json: %w", err)
	}
	if len(out.Choices) == 0 {
		return OpenAIMessage{}, errors.New("openai: empty choices")
	}
	return out.Choices[0].Message, nil
}

func (c *OpenAIClient) ChatStream(messages []OpenAIMessage, onToken func(string)) (string, error) {
	payload := chatRequest{Model: c.Model, Messages: messages, Stream: true}
	buf, _ := json.Marshal(payload)

	req, err := http.NewRequest(http.MethodPost, "https://api.openai.com/v1/chat/completions", bytes.NewReader(buf))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+c.APIKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")

	resp, err := c.httpClient().Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("openai request failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	reader := bufio.NewReader(resp.Body)
	var full strings.Builder
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return full.String(), err
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "[DONE]" {
			break
		}
		var chunk streamChunk
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			continue
		}
		if len(chunk.Choices) == 0 {
			continue
		}
		tok := chunk.Choices[0].Delta.Content
		if tok == "" {
			continue
		}
		full.WriteString(tok)
		if onToken != nil {
			onToken(tok)
		}
	}

	return full.String(), nil
}
