package handlers

import (
	"encoding/json"
	"net/http"

	"openai-agent-service/internal/models"
	"openai-agent-service/internal/services"
)

type ChatHandlers struct {
	Chat *services.ChatService
}

func (h *ChatHandlers) HandleChat(w http.ResponseWriter, r *http.Request) {
	var req models.ChatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid_json"})
		return
	}
	if req.Message == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "message_required"})
		return
	}

	resp, err := h.Chat.Chat(r.Context(), CallerKey(r), req)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "chat_failed", "message": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}
