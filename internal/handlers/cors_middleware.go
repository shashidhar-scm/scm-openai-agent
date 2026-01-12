package handlers

import (
	"net/http"
	"strings"

	"openai-agent-service/internal/config"
)

func WithCORS(cfg config.Config) func(http.Handler) http.Handler {
	allowed := make([]string, 0)
	for _, part := range strings.Split(cfg.CORSAllowedOrigins, ",") {
		s := strings.TrimSpace(part)
		if s == "" {
			continue
		}
		allowed = append(allowed, s)
	}

	allowsAll := false
	for _, a := range allowed {
		if a == "*" {
			allowsAll = true
			break
		}
	}

	isAllowed := func(origin string) bool {
		if allowsAll {
			return true
		}
		for _, a := range allowed {
			if origin == a {
				return true
			}
		}
		return false
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := strings.TrimSpace(r.Header.Get("Origin"))
			if origin != "" && isAllowed(origin) {
				w.Header().Set("Vary", "Origin")
				if allowsAll {
					w.Header().Set("Access-Control-Allow-Origin", "*")
				} else {
					w.Header().Set("Access-Control-Allow-Origin", origin)
				}
				w.Header().Set("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
				w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-API-Key, Accept")
				w.Header().Set("Access-Control-Max-Age", "600")
			}

			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
