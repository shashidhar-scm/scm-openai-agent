package handlers

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"

	"openai-agent-service/internal/store"
)

type ConversationHandlers struct {
	Store *store.PostgresStore
}

func (h *ConversationHandlers) CreateConversation(w http.ResponseWriter, r *http.Request) {
	c, err := h.Store.CreateConversation(r.Context(), CallerKey(r))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "create_conversation_failed"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": c})
}

func (h *ConversationHandlers) GetConversation(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if strings.TrimSpace(id) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "conversation_id_required"})
		return
	}
	c, err := h.Store.GetConversation(r.Context(), CallerKey(r), id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "not_found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "get_conversation_failed"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": c})
}

func (h *ConversationHandlers) ListMessages(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if strings.TrimSpace(id) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "conversation_id_required"})
		return
	}
	limit := 20
	if v := strings.TrimSpace(r.URL.Query().Get("limit")); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			limit = n
		}
	}
	msgs, err := h.Store.ListMessages(r.Context(), CallerKey(r), id, limit)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "list_messages_failed"})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"data": msgs})
}
