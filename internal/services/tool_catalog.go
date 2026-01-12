package services

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"
)

type OpenAPISpec struct {
	Paths map[string]map[string]any `json:"paths"`
}

type ToolCatalog struct {
	BaseURL string
	APIKey  string
	HTTP    *http.Client

	mu        sync.RWMutex
	cached    OpenAPISpec
	cachedAt  time.Time
	cacheTTL  time.Duration
	lastError error
}

func NewToolCatalog(baseURL string, httpClient *http.Client, ttl time.Duration) *ToolCatalog {
	if ttl <= 0 {
		ttl = 2 * time.Minute
	}
	return &ToolCatalog{BaseURL: strings.TrimRight(baseURL, "/"), HTTP: httpClient, cacheTTL: ttl}
}

func NewToolCatalogWithKey(baseURL, apiKey string, httpClient *http.Client, ttl time.Duration) *ToolCatalog {
	c := NewToolCatalog(baseURL, httpClient, ttl)
	c.APIKey = strings.TrimSpace(apiKey)
	return c
}

func (c *ToolCatalog) Fetch(ctx context.Context) (OpenAPISpec, error) {
	c.mu.RLock()
	if !c.cachedAt.IsZero() && time.Since(c.cachedAt) < c.cacheTTL {
		s := c.cached
		err := c.lastError
		c.mu.RUnlock()
		if err != nil {
			return OpenAPISpec{}, err
		}
		return s, nil
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.cachedAt.IsZero() && time.Since(c.cachedAt) < c.cacheTTL {
		if c.lastError != nil {
			return OpenAPISpec{}, c.lastError
		}
		return c.cached, nil
	}

	hc := c.HTTP
	if hc == nil {
		hc = http.DefaultClient
	}

	url := c.BaseURL + "/openapi.json"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		c.cachedAt = time.Now()
		c.lastError = err
		return OpenAPISpec{}, err
	}
	if strings.TrimSpace(c.APIKey) != "" {
		req.Header.Set("X-API-Key", c.APIKey)
	}

	resp, err := hc.Do(req)
	if err != nil {
		c.cachedAt = time.Now()
		c.lastError = err
		return OpenAPISpec{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		err = errors.New("openapi fetch failed")
		c.cachedAt = time.Now()
		c.lastError = err
		return OpenAPISpec{}, err
	}

	var spec OpenAPISpec
	if err := json.NewDecoder(resp.Body).Decode(&spec); err != nil {
		c.cachedAt = time.Now()
		c.lastError = err
		return OpenAPISpec{}, err
	}
	if spec.Paths == nil {
		spec.Paths = map[string]map[string]any{}
	}

	c.cached = spec
	c.cachedAt = time.Now()
	c.lastError = nil
	return spec, nil
}

func (c *ToolCatalog) IsAllowed(ctx context.Context, method, path string) bool {
	spec, err := c.Fetch(ctx)
	if err != nil {
		return false
	}
	m := strings.ToLower(strings.TrimSpace(method))
	p := strings.TrimSpace(path)
	// Exact match first.
	if ops, ok := spec.Paths[p]; ok {
		_, ok = ops[m]
		return ok
	}
	// Fallback: match templated OpenAPI paths like /ads/campaigns/{id}/impressions
	for specPath, ops := range spec.Paths {
		if !matchOpenAPIPath(specPath, p) {
			continue
		}
		_, ok := ops[m]
		if ok {
			return true
		}
	}
	return false
}

func matchOpenAPIPath(specPath, actualPath string) bool {
	sp := strings.TrimSpace(specPath)
	ap := strings.TrimSpace(actualPath)
	if sp == ap {
		return true
	}
	sp = strings.Trim(sp, "/")
	ap = strings.Trim(ap, "/")
	if sp == "" || ap == "" {
		return sp == ap
	}
	ss := strings.Split(sp, "/")
	as := strings.Split(ap, "/")
	if len(ss) != len(as) {
		return false
	}
	for i := 0; i < len(ss); i++ {
		seg := ss[i]
		if strings.HasPrefix(seg, "{") && strings.HasSuffix(seg, "}") {
			if strings.TrimSpace(as[i]) == "" {
				return false
			}
			continue
		}
		if seg != as[i] {
			return false
		}
	}
	return true
}
