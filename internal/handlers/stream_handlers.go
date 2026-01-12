package handlers

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"openai-agent-service/internal/models"
	"openai-agent-service/internal/services"
)

type StreamHandlers struct {
	Chat *services.ChatService
}

func sseWriteEvent(w io.Writer, event string, payload any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if event != "" {
		if _, err := fmt.Fprintf(w, "event: %s\n", event); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(w, "data: %s\n\n", string(b)); err != nil {
		return err
	}
	return nil
}

func (h *StreamHandlers) HandleChatStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "streaming_not_supported"})
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	var req models.ChatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		_ = sseWriteEvent(w, "error", map[string]any{"error": "invalid_json"})
		flusher.Flush()
		return
	}
	if req.Message == "" {
		_ = sseWriteEvent(w, "error", map[string]any{"error": "message_required"})
		flusher.Flush()
		return
	}

	resp, err := h.Chat.ChatStream(r.Context(), CallerKey(r), req, func(tok string) {
		_ = sseWriteEvent(w, "token", map[string]any{"text": tok})
		flusher.Flush()
	})
	if err != nil {
		_ = sseWriteEvent(w, "error", map[string]any{"error": "chat_failed", "message": err.Error()})
		flusher.Flush()
		return
	}
	_ = sseWriteEvent(w, "final", resp)
	flusher.Flush()
}
