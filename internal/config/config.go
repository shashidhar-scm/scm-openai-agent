package config

import (
	"errors"
	"os"
	"strings"
)

type Config struct {
	Port              string
	AgentAPIKeys      map[string]struct{}
	OpenAIAPIKey      string
	OpenAIModel       string
	ToolGatewayURL    string
	ToolGatewayAPIKey string
	MockMode          bool
	DatabaseURL       string
	AutoCreateDB      bool
	MaintenanceDB     string
	CORSAllowedOrigins string
}

func getenv(key, def string) string {
	v := os.Getenv(key)
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
}

func parseCSVSet(v string) map[string]struct{} {
	out := make(map[string]struct{})
	for _, part := range strings.Split(v, ",") {
		s := strings.TrimSpace(part)
		if s == "" {
			continue
		}
		out[s] = struct{}{}
	}
	return out
}

func Load() (Config, error) {
	cfg := Config{
		Port:              strings.TrimSpace(getenv("PORT", "8091")),
		OpenAIModel:       strings.TrimSpace(getenv("OPENAI_MODEL", "gpt-4o-mini")),
		ToolGatewayURL:    strings.TrimSpace(getenv("TOOL_GATEWAY_BASE_URL", "https://tool-gateway.citypost.us")),
		OpenAIAPIKey:      strings.TrimSpace(os.Getenv("OPENAI_API_KEY")),
		ToolGatewayAPIKey: strings.TrimSpace(os.Getenv("TOOL_GATEWAY_API_KEY")),
		MockMode:          strings.EqualFold(strings.TrimSpace(os.Getenv("MOCK_MODE")), "true") || strings.TrimSpace(os.Getenv("MOCK_MODE")) == "1",
		DatabaseURL:       strings.TrimSpace(os.Getenv("DATABASE_URL")),
		AutoCreateDB:      strings.EqualFold(strings.TrimSpace(os.Getenv("AUTO_CREATE_DB")), "true") || strings.TrimSpace(os.Getenv("AUTO_CREATE_DB")) == "1",
		MaintenanceDB:     strings.TrimSpace(getenv("MAINTENANCE_DB", "postgres")),
		CORSAllowedOrigins: strings.TrimSpace(os.Getenv("CORS_ALLOWED_ORIGINS")),
	}

	keysRaw := strings.TrimSpace(getenv("AGENT_API_KEYS", getenv("AGENT_API_KEY", "")))
	cfg.AgentAPIKeys = parseCSVSet(keysRaw)

	if !cfg.MockMode {
		if cfg.OpenAIAPIKey == "" {
			return Config{}, errors.New("missing OPENAI_API_KEY")
		}
	}
	if cfg.ToolGatewayAPIKey == "" {
		return Config{}, errors.New("missing TOOL_GATEWAY_API_KEY")
	}
	if len(cfg.AgentAPIKeys) == 0 {
		return Config{}, errors.New("missing AGENT_API_KEY (or AGENT_API_KEYS)")
	}
	if cfg.Port == "" {
		return Config{}, errors.New("missing PORT")
	}
	if cfg.ToolGatewayURL == "" {
		return Config{}, errors.New("missing TOOL_GATEWAY_BASE_URL")
	}
	if cfg.DatabaseURL == "" {
		return Config{}, errors.New("missing DATABASE_URL")
	}

	return cfg, nil
}
