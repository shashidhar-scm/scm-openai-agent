package handlers

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"openai-agent-service/internal/config"
)

type ctxKey string

const ctxCallerKey ctxKey = "caller_api_key"

var reqIDSeq uint64

func reqLogEnabled() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("GO_LOG")))
	return v == "debug" || v == "1" || v == "true"
}

type statusWriter struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(p)
	w.bytes += n
	return n, err
}

func WithRequestLogging() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !reqLogEnabled() {
				next.ServeHTTP(w, r)
				return
			}

			reqID := atomic.AddUint64(&reqIDSeq, 1)
			start := time.Now()
			path := r.URL.Path
			if r.URL.RawQuery != "" {
				path = path + "?" + r.URL.RawQuery
			}
			log.Printf("http in  id=%d method=%s path=%s", reqID, r.Method, path)

			sw := &statusWriter{ResponseWriter: w}
			next.ServeHTTP(sw, r)

			dur := time.Since(start)
			status := sw.status
			if status == 0 {
				status = http.StatusOK
			}
			log.Printf("http out id=%d status=%d bytes=%d dur_ms=%s", reqID, status, sw.bytes, strconv.FormatInt(dur.Milliseconds(), 10))
		})
	}
}

func WithAPIKey(cfg config.Config) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := strings.TrimSpace(r.Header.Get("X-API-Key"))
			if key == "" {
				writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "missing_x_api_key"})
				return
			}
			if _, ok := cfg.AgentAPIKeys[key]; !ok {
				writeJSON(w, http.StatusForbidden, map[string]any{"error": "invalid_x_api_key"})
				return
			}
			ctx := context.WithValue(r.Context(), ctxCallerKey, key)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func CallerKey(r *http.Request) string {
	v, _ := r.Context().Value(ctxCallerKey).(string)
	return strings.TrimSpace(v)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
