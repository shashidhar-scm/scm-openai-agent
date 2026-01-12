package routes

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"openai-agent-service/internal/config"
	"openai-agent-service/internal/handlers"
)

func NewRouter(cfg config.Config, chat *handlers.ChatHandlers, stream *handlers.StreamHandlers, conv *handlers.ConversationHandlers) http.Handler {
	r := chi.NewRouter()

	r.Use(handlers.WithRequestLogging())
	r.Use(handlers.WithCORS(cfg))

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	auth := handlers.WithAPIKey(cfg)

	r.With(auth).Post("/conversations", conv.CreateConversation)
	r.With(auth).Get("/conversations/{id}", conv.GetConversation)
	r.With(auth).Get("/conversations/{id}/messages", conv.ListMessages)

	r.With(auth).Post("/chat", chat.HandleChat)
	r.With(auth).Post("/chat/stream", stream.HandleChatStream)

	return r
}
