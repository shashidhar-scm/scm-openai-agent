package services

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"openai-agent-service/internal/models"
)

func debugEnabled() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("GO_LOG")))
	return v == "debug" || v == "1" || v == "true"
}

func debugLogf(format string, args ...any) {
	if !debugEnabled() {
		return
	}
	log.Printf(format, args...)
}

func applyQueryDefaults(method, path string, query map[string]string) map[string]string {
	m := strings.ToUpper(strings.TrimSpace(method))
	p := strings.TrimSpace(path)
	if m != "GET" {
		return query
	}
	if query == nil {
		query = map[string]string{}
	}

	setIfMissing := func(k, v string) {
		if strings.TrimSpace(query[k]) == "" {
			query[k] = v
		}
	}

	capInt := func(k string, max int) {
		v := strings.TrimSpace(query[k])
		if v == "" {
			return
		}
		n, err := strconv.Atoi(v)
		if err != nil {
			return
		}
		if n > max {
			query[k] = strconv.Itoa(max)
		}
	}

	// POP stats defaults
	if p == "/pop/stats" {
		setIfMissing("limit", "10")
		setIfMissing("last_days", "30")
		capInt("limit", 200)
	}

	// POP list defaults
	if p == "/pop" {
		setIfMissing("page", "1")
		setIfMissing("page_size", "20")
		capInt("page_size", 200)
	}

	// Ads list-ish defaults (safe pagination)
	if p == "/ads/devices" || p == "/ads/venues" || strings.HasPrefix(p, "/ads/venues/") || strings.HasPrefix(p, "/ads/devices/") || p == "/ads/campaigns" || p == "/ads/creatives" || p == "/ads/projects" || p == "/ads/advertisers" {
		setIfMissing("page", "1")
		// campaigns tend to be heavier; still allow larger default
		if p == "/ads/campaigns" {
			setIfMissing("page_size", "50")
			capInt("page_size", 200)
		} else {
			setIfMissing("page_size", "20")
			capInt("page_size", 100)
		}
	}

	// Metrics defaults: avoid totals unless explicitly requested.
	if strings.HasPrefix(p, "/metrics/") {
		setIfMissing("include_totals", "false")
		setIfMissing("page", "1")
		setIfMissing("page_size", "50")
		capInt("page_size", 200)
	}

	return query
}

func boolToOnOff(val bool) string {
	if val {
		return "on"
	}
	return "off"
}

func bytesToGiB(b int64) float64 {
	if b <= 0 {
		return 0
	}
	return math.Round((float64(b)/(1024*1024*1024))*10) / 10
}

func bytesToMiB(b int64) float64 {
	if b <= 0 {
		return 0
	}
	return math.Round((float64(b)/(1024*1024))*10) / 10
}

type Store interface {
	AppendMessage(ctx context.Context, ownerKey, conversationID, role, content string) error
	ListMessages(ctx context.Context, ownerKey, conversationID string, limit int) ([]models.Message, error)
	CreateConversation(ctx context.Context, ownerKey string) (models.Conversation, error)
	GetConversation(ctx context.Context, ownerKey, conversationID string) (models.Conversation, error)
}

type scmRequestArgs struct {
	Method string            `json:"method"`
	Path   string            `json:"path"`
	Query  map[string]string `json:"query"`
	Body   map[string]any    `json:"body"`
	Multipart *MultipartPayload `json:"multipart"`
}

func isLikelyDataRequest(msg string) bool {
	s := strings.ToLower(strings.TrimSpace(msg))
	if s == "" {
		return false
	}
	
	// Check for explicit patterns first (these should DEFINITELY use tools)
	explicitPatterns := []string{
		"pop data", "pop of", "pop for", "pop in", "pop from", 
		"pop list", "pop stats", "pop trend", "pop search",
		"kiosk count", "device count", "devices in", "kiosks in",
		"campaign impression", "impressions for",
		"advertiser list", "list advertiser", "show advertiser", 
		"campaign list", "list campaign", "show campaign",
		"top poster", "top device", "top kiosk", "top performer",
		"by click", "by play", "by count", "by value",
	}
	
	for _, pattern := range explicitPatterns {
		if strings.Contains(s, pattern) {
			return true
		}
	}

	// General heuristic: common verbs + domain nouns
	keywords := []string{
		"list", "show", "get", "fetch", "find", "give me", "display",
		"advertiser", "campaign", "creative", "impression", "device", "region",
		"metrics", "kiosk", "pop",
	}
	for _, k := range keywords {
		if strings.Contains(s, k) {
			return true
		}
	}
	return false
}

func isTopDevicesFromCityIntent(msgLower string) bool {
	s := strings.ToLower(strings.TrimSpace(msgLower))
	if s == "" {
		return false
	}
	if strings.Contains(s, "top") && (strings.Contains(s, "device") || strings.Contains(s, "devices")) {
		if strings.Contains(s, "from") || strings.Contains(s, "in") || strings.Contains(s, "city") {
			return true
		}
		if strings.Contains(s, "perform") {
			return true
		}
	}
	return false
}

func isTopPostersFromCityIntent(msgLower string) bool {
	s := strings.ToLower(strings.TrimSpace(msgLower))
	if s == "" {
		return false
	}
	if strings.Contains(s, "top") && (strings.Contains(s, "poster") || strings.Contains(s, "posters")) {
		if strings.Contains(s, "from") || strings.Contains(s, "in") || strings.Contains(s, "city") {
			return true
		}
		if strings.Contains(s, "perform") {
			return true
		}
	}
	return false
}

func isListCampaignsIntent(msgLower string) bool {
	s := strings.ToLower(strings.TrimSpace(msgLower))
	if s == "" {
		return false
	}
	if strings.Contains(s, "list") && strings.Contains(s, "campaign") {
		return true
	}
	if strings.Contains(s, "show") && strings.Contains(s, "campaign") {
		return true
	}
	return false
}

func (c *ChatService) chatWithToolLoop(ctx context.Context, messages []OpenAIMessage, tools []OpenAITool, toolChoice any) (string, error) {
	// Tool loop (non-streaming)
	msgs := make([]OpenAIMessage, 0, len(messages)+8)
	msgs = append(msgs, messages...)

	required := false
	if tc, ok := toolChoice.(string); ok && strings.EqualFold(strings.TrimSpace(tc), "required") {
		required = true
	}

	totalToolCalls := 0
	for step := 0; step < c.MaxToolCalls; step++ {
		assistantMsg, err := c.OpenAI.ChatWithToolsChoice(msgs, tools, toolChoice)
		if err != nil {
			return "", err
		}
		if len(assistantMsg.ToolCalls) == 0 {
			if required {
				msgs = append(msgs, OpenAIMessage{Role: "user", Content: "You must call the scm_request tool to fetch the requested data. Make at least one scm_request call (method + path) before answering."})
				continue
			}
			return assistantMsg.Content, nil
		}

		// Add assistant message containing tool_calls
		msgs = append(msgs, OpenAIMessage{Role: "assistant", Content: assistantMsg.Content, ToolCalls: assistantMsg.ToolCalls})

		for _, call := range assistantMsg.ToolCalls {
			totalToolCalls++
			// OpenAI requires that every tool_call_id is followed by a tool message.
			// If we exceed limits, respond with a synthetic error for the remaining calls.
			if totalToolCalls > c.MaxToolCalls {
				msgs = append(msgs, OpenAIMessage{Role: "tool", ToolCallID: call.ID, Content: `{"error":"tool_limit_exceeded"}`})
				continue
			}
			if call.Type != "function" || call.Function.Name != "scm_request" {
				msgs = append(msgs, OpenAIMessage{Role: "tool", ToolCallID: call.ID, Content: `{"error":"unsupported_tool"}`})
				continue
			}
			var args scmRequestArgs
			_ = json.Unmarshal([]byte(call.Function.Arguments), &args)
			method := strings.ToUpper(strings.TrimSpace(args.Method))
			path := strings.TrimSpace(args.Path)
			if method == "" || path == "" {
				msgs = append(msgs, OpenAIMessage{Role: "tool", ToolCallID: call.ID, Content: `{"error":"invalid_args"}`})
				continue
			}

			// Normalize tool calls that incorrectly include a query string in the path.
			// Tool allowlists generally work on the base path (e.g. "/pop/stats"), while query
			// parameters should be passed via args.Query.
			if strings.Contains(path, "?") {
				if u, err := url.Parse(path); err == nil {
					if args.Query == nil {
						args.Query = map[string]string{}
					}
					for k, vs := range u.Query() {
						if len(vs) == 0 {
							continue
						}
						// If the model already provided a query value explicitly, keep it.
						if _, exists := args.Query[k]; !exists {
							args.Query[k] = vs[0]
						}
					}
					if strings.TrimSpace(u.Path) != "" {
						path = u.Path
						args.Path = u.Path
					}
				}
			}

			if c.Catalog == nil || !c.Catalog.IsAllowed(ctx, method, path) {
				msgs = append(msgs, OpenAIMessage{Role: "tool", ToolCallID: call.ID, Content: `{"error":"forbidden_tool"}`})
				continue
			}

			args.Query = applyQueryDefaults(method, path, args.Query)

			var status int
			var body []byte
			var err error
			if args.Multipart != nil {
				status, body, err = c.Gateway.DoMultipart(method, path, args.Query, *args.Multipart)
			} else {
				status, body, err = c.Gateway.DoJSON(method, path, args.Query, args.Body)
			}
			payload := map[string]any{"status": status}
			if err != nil {
				payload["error"] = err.Error()
			} else {
				if len(body) > c.MaxToolBytes {
					payload["truncated"] = true
					body = body[:c.MaxToolBytes]
				}
				// Clip tool payload further before sending to the model to avoid token blowups.
				payload["body"] = clipString(string(body), 8000)
			}
			b, _ := json.Marshal(payload)
			msgs = append(msgs, OpenAIMessage{Role: "tool", ToolCallID: call.ID, Content: string(b)})
		}
		if totalToolCalls > c.MaxToolCalls {
			break
		}
	}

	// If we hit tool limit, ask model to answer with what it has.
	msgs = append(msgs, OpenAIMessage{Role: "user", Content: "Please answer using the information gathered so far."})
	answer, err := c.OpenAI.Chat(msgs)
	return answer, err
}

type ChatService struct {
	MockMode bool
	Gateway  *GatewayClient
	OpenAI   *OpenAIClient
	Store    Store
	Catalog  *ToolCatalog
	MaxToolCalls int
	MaxToolBytes int

	cityMu       sync.Mutex
	cityCache    map[string]struct{}
	cityCacheAt  time.Time
	cityCacheTTL time.Duration

	regionCache   map[string]struct{}
	regionCacheAt time.Time
}

func (c *ChatService) ensureCityRegionCachesLocked(ctx context.Context) {
	if c.cityCacheTTL <= 0 {
		c.cityCacheTTL = 10 * time.Minute
	}
	if c.cityCache != nil && !c.cityCacheAt.IsZero() && time.Since(c.cityCacheAt) < c.cityCacheTTL &&
		c.regionCache != nil && !c.regionCacheAt.IsZero() && time.Since(c.regionCacheAt) < c.cityCacheTTL {
		return
	}
	if c.Gateway == nil {
		return
	}

	status, body, err := c.Gateway.Get("/ads/devices/counts/regions")
	_ = status
	if err != nil {
		return
	}
	var root map[string]any
	if json.Unmarshal(body, &root) != nil {
		return
	}
	rows, _ := root["data"].([]any)

	citySet := map[string]struct{}{}
	regionSet := map[string]struct{}{}
	for _, it := range rows {
		m, ok := it.(map[string]any)
		if !ok {
			continue
		}
		city, _ := m["city"].(string)
		city = strings.ToLower(strings.TrimSpace(city))
		if city != "" {
			citySet[city] = struct{}{}
		}
		region, _ := m["region"].(string)
		region = strings.ToLower(strings.TrimSpace(region))
		if region != "" {
			regionSet[region] = struct{}{}
		}
	}

	// Only overwrite caches when we have data; otherwise keep last good values.
	now := time.Now()
	if len(citySet) > 0 {
		c.cityCache = citySet
		c.cityCacheAt = now
	}
	if len(regionSet) > 0 {
		c.regionCache = regionSet
		c.regionCacheAt = now
	}
}

func (c *ChatService) cityCodes(ctx context.Context) []string {
	c.cityMu.Lock()
	defer c.cityMu.Unlock()

	c.ensureCityRegionCachesLocked(ctx)
	if c.cityCache != nil && !c.cityCacheAt.IsZero() && time.Since(c.cityCacheAt) < c.cityCacheTTL {
		out := make([]string, 0, len(c.cityCache))
		for k := range c.cityCache {
			out = append(out, k)
		}
		return out
	}
	if c.cityCache != nil {
		out := make([]string, 0, len(c.cityCache))
		for k := range c.cityCache {
			out = append(out, k)
		}
		return out
	}
	return nil
}

func (c *ChatService) detectCityCode(ctx context.Context, msgLower string) string {
	s := strings.ToLower(strings.TrimSpace(msgLower))
	if s == "" {
		return ""
	}
	codes := c.cityCodes(ctx)
	if len(codes) == 0 {
		return ""
	}
	sort.Slice(codes, func(i, j int) bool { return len(codes[i]) > len(codes[j]) })
	for _, city := range codes {
		city = strings.ToLower(strings.TrimSpace(city))
		if city == "" {
			continue
		}
		if strings.Contains(s, city) {
			return city
		}
	}
	return ""
}

func (c *ChatService) handleKioskCountFromCity(ctx context.Context, req models.ChatRequest, onToken func(string)) (models.ChatResponse, bool, error) {
	msgLower := strings.ToLower(req.Message)
	if !(strings.Contains(msgLower, "kiosk") || strings.Contains(msgLower, "kiosks") || strings.Contains(msgLower, "device") || strings.Contains(msgLower, "devices")) {
		return models.ChatResponse{}, false, nil
	}
	hasCountKeyword := strings.Contains(msgLower, "how many") || strings.Contains(msgLower, "count") || strings.Contains(msgLower, "number of")
	hasStatusKeyword := strings.Contains(msgLower, "offline") || strings.Contains(msgLower, "online") || strings.Contains(msgLower, "status") || strings.Contains(msgLower, "down")
	if !(hasCountKeyword || hasStatusKeyword) {
		return models.ChatResponse{}, false, nil
	}
	city := c.detectCityCode(ctx, msgLower)
	region := c.detectRegionCode(ctx, msgLower)

	if hasStatusKeyword {
		queryCity := city
		queryRegion := region
		// Special-case: users often say "kc" meaning the KC region within KCMO.
		if queryCity == "" && strings.EqualFold(region, "kc") {
			queryCity = "kcmo"
		}
		if queryCity == "" && queryRegion == "" {
			return models.ChatResponse{Answer: "Please specify a city code (for example: kcmo) or a region code (for example: kc)."}, true, nil
		}

		if c.Gateway == nil {
			return models.ChatResponse{Answer: "Tool gateway is not configured."}, true, nil
		}
		values := url.Values{}
		if queryCity != "" {
			values.Set("city", queryCity)
		}
		if queryRegion != "" && queryCity == "" {
			values.Set("region", queryRegion)
		}
		path := "/metrics/servers/status/city"
		if encoded := values.Encode(); encoded != "" {
			path += "?" + encoded
		}

		debugLogf("gateway GET %s", path)
		status, body, err := c.Gateway.Get(path)
		debugLogf("gateway GET %s -> status=%d err=%v", path, status, err)
		step := models.Step{Tool: "metricsServersStatusCity", Status: status}
		if err != nil {
			step.Error = err.Error()
		} else {
			step.Body = clipString(strings.TrimSpace(string(body)), 2000)
		}

		answer := ""
		if err != nil {
			answer = "Failed to fetch device status: " + err.Error()
		} else if status < 200 || status >= 300 {
			answer = fmt.Sprintf("Failed to fetch device status (status %d).", status)
		} else {
			var root map[string]any
			_ = json.Unmarshal(body, &root)
			rows, _ := root["data"].([]any)
			resolveFloat := func(m map[string]any, key string) float64 {
				switch v := m[key].(type) {
				case float64:
					return v
				case int:
					return float64(v)
				case json.Number:
					if f, e := v.Float64(); e == nil {
						return f
					}
					return 0
				default:
					return 0
				}
			}

			var matched bool
			var online, offline, total float64

			lowerCity := strings.ToLower(queryCity)
			if queryCity != "" {
				for _, it := range rows {
					m, ok := it.(map[string]any)
					if !ok {
						continue
					}
					rowCity, _ := m["city"].(string)
					if strings.ToLower(strings.TrimSpace(rowCity)) == lowerCity {
						matched = true
						online = resolveFloat(m, "online")
						offline = resolveFloat(m, "offline")
						total = resolveFloat(m, "total")
						break
					}
				}
			} else if len(rows) > 0 {
				for _, it := range rows {
					m, ok := it.(map[string]any)
					if !ok {
						continue
					}
					online += resolveFloat(m, "online")
					offline += resolveFloat(m, "offline")
					total += resolveFloat(m, "total")
				}
				matched = true
			}

			round := func(v float64) int64 {
				if v >= 0 {
					return int64(v + 0.5)
				}
				return int64(v - 0.5)
			}

			if matched {
				if queryCity != "" {
					answer = fmt.Sprintf("City '%s': %d offline / %d online (total %d devices in the last 5m).",
						queryCity,
						round(offline),
						round(online),
						round(total),
					)
				} else {
					answer = fmt.Sprintf("Region '%s': %d offline / %d online (total %d devices in the last 5m).",
						queryRegion,
						round(offline),
						round(online),
						round(total),
					)
				}
			} else if queryCity != "" {
				answer = fmt.Sprintf("No device status data was found for city '%s'.", queryCity)
			} else {
				answer = fmt.Sprintf("No device status data was found for region '%s'.", queryRegion)
			}
		}

		if onToken != nil {
			onToken(answer)
		}
		return models.ChatResponse{Answer: answer, Steps: []models.Step{step}}, true, nil
	}

	lookupCity := city
	if lookupCity == "" && region != "" {
		if strings.EqualFold(region, "kc") {
			lookupCity = "kcmo"
		}
	}
	if lookupCity == "" {
		return models.ChatResponse{Answer: "Please specify a city code (for example: kcmo)."}, true, nil
	}

	if c.Gateway == nil {
		return models.ChatResponse{Answer: "Tool gateway is not configured."}, true, nil
	}
	path := "/ads/devices/counts/regions?city=" + urlEscape(lookupCity)
	status, body, err := c.Gateway.Get(path)
	step := models.Step{Tool: "adsDevicesCountsRegions", Status: status}
	if err != nil {
		step.Error = err.Error()
	} else {
		step.Body = clipString(strings.TrimSpace(string(body)), 2000)
	}

	answer := ""
	if err != nil {
		answer = "Failed to fetch kiosk counts: " + err.Error()
	} else if status < 200 || status >= 300 {
		answer = fmt.Sprintf("Failed to fetch kiosk counts (status %d).", status)
	} else {
		var root map[string]any
		_ = json.Unmarshal(body, &root)
		rows, _ := root["data"].([]any)
		total := 0.0
		regionCount := 0.0
		for _, it := range rows {
			m, ok := it.(map[string]any)
			if !ok {
				continue
			}
			rowRegion, _ := m["region"].(string)
			rowRegion = strings.ToLower(strings.TrimSpace(rowRegion))
			for _, k := range []string{"count", "kiosk_count", "kiosks", "devices"} {
				switch v := m[k].(type) {
				case float64:
					total += v
					if region != "" && strings.EqualFold(rowRegion, region) {
						regionCount += v
					}
				case int:
					total += float64(v)
					if region != "" && strings.EqualFold(rowRegion, region) {
						regionCount += float64(v)
					}
				}
			}
		}
		if region != "" && city == "" {
			if regionCount > 0 {
				answer = fmt.Sprintf("There are %.0f kiosks/devices recorded for region '%s' (city '%s').", regionCount, region, lookupCity)
			} else {
				answer = fmt.Sprintf("No kiosk/device counts were found for region '%s' (city '%s').", region, lookupCity)
			}
		} else if total > 0 {
			answer = fmt.Sprintf("There are %.0f kiosks/devices recorded for city '%s'.", total, lookupCity)
		} else {
			answer = fmt.Sprintf("No kiosk/device counts were found for city '%s'.", lookupCity)
		}
	}

	if onToken != nil {
		onToken(answer)
	}
	return models.ChatResponse{Answer: answer, Steps: []models.Step{step}}, true, nil
}

func (c *ChatService) handleDeviceTelemetry(ctx context.Context, req models.ChatRequest, onToken func(string)) (models.ChatResponse, bool, error) {
	msgLower := strings.ToLower(req.Message)
	contains := func(tokens ...string) bool {
		for _, token := range tokens {
			if strings.Contains(msgLower, token) {
				return true
			}
		}
		return false
	}

	wantsTemp := contains("temp", "temperature", "heat")
	wantsVolume := contains("volume", "sound", "speaker", "audio")
	wantsMute := contains("mute", "muted", "unmute")
	wantsPower := contains("power", "online", "offline")
	wantsBattery := contains("battery")
	wantsDisplay := contains("display", "screen", "panel")
	wantsFan := contains("fan")
	wantsCPU := contains("cpu", "processor")
	wantsMemory := contains("memory", "ram")
	wantsDisk := contains("disk", "storage")
	wantsNetwork := contains("network", "bandwidth", "traffic", "throughput")
	wantsProcesses := contains("process", "service", "app", "apps", "kiosk")
	wantsInputDevices := contains("input", "usb", "peripheral")
	wantsUptime := contains("uptime")
	wantsTelemetry := contains("telemetry", "status", "health", "metrics", "device status")
	if wantsTelemetry {
		wantsTemp = true
		wantsVolume = true
		wantsPower = true
		wantsBattery = true
		wantsDisplay = true
		wantsFan = true
		wantsCPU = true
		wantsMemory = true
		wantsDisk = true
		wantsNetwork = true
		wantsProcesses = true
		wantsInputDevices = true
		wantsUptime = true
		wantsMute = true
	}
	if !(wantsTemp || wantsVolume || wantsMute || wantsPower || wantsBattery || wantsDisplay || wantsFan || wantsCPU || wantsMemory || wantsDisk || wantsNetwork || wantsProcesses || wantsInputDevices || wantsUptime) {
		return models.ChatResponse{}, false, nil
	}
	if c.Gateway == nil {
		return models.ChatResponse{Answer: "Tool gateway is not configured."}, true, nil
	}

	hostTokens := detectHostTokens(req.Message)
	if len(hostTokens) == 0 {
		return models.ChatResponse{Answer: "Please specify the device or server name (for example: dart2)."}, true, nil
	}

	host := strings.ToLower(strings.TrimSpace(hostTokens[0]))
	if host == "" {
		return models.ChatResponse{Answer: "Please specify the device or server name (for example: dart2)."}, true, nil
	}

	path := "/metrics/history?page=1&page_size=1&include_totals=false&server_id=" + urlEscape(host)
	status, body, err := c.Gateway.Get(path)
	step := models.Step{Tool: "metricsHistory", Status: status}
	if err != nil {
		step.Error = err.Error()
	} else {
		step.Body = clipString(strings.TrimSpace(string(body)), 2000)
	}

	answer := ""
	if err != nil {
		answer = "Failed to fetch temperature data: " + err.Error()
	} else if status < 200 || status >= 300 {
		answer = fmt.Sprintf("Failed to fetch temperature data (status %d).", status)
	} else {
		var payload struct {
			Data []struct {
				Time                 time.Time `json:"time"`
				CPU                  float64   `json:"cpu"`
				Memory               float64   `json:"memory"`
				Temperature          float64   `json:"temperature"`
				ChassisTemperature   float64   `json:"chassis_temperature"`
				HotspotTemperature   float64   `json:"hotspot_temperature"`
				SoundVolumePercent   float64   `json:"sound_volume_percent"`
				SoundMuted           bool      `json:"sound_muted"`
				PowerOnline          bool      `json:"power_online"`
				BatteryPresent       bool      `json:"battery_present"`
				BatteryChargePercent int64     `json:"battery_charge_percent"`
				DisplayConnected     bool      `json:"display_connected"`
				DisplayWidth         int64     `json:"display_width"`
				DisplayHeight        int64     `json:"display_height"`
				DisplayRefreshHz     int64     `json:"display_refresh_hz"`
				DisplayPrimary       bool      `json:"display_primary"`
				DisplayDpmsEnabled   bool      `json:"display_dpms_enabled"`
				FanRPM               int64     `json:"fan_rpm"`
				Disk                 float64   `json:"disk"`
				DiskTotalBytes       int64     `json:"disk_total_bytes"`
				DiskUsedBytes        int64     `json:"disk_used_bytes"`
				NetBytesSent         int64     `json:"net_bytes_sent"`
				NetBytesRecv         int64     `json:"net_bytes_recv"`
				NetDailyRxBytes      int64     `json:"net_daily_rx_bytes"`
				NetDailyTxBytes      int64     `json:"net_daily_tx_bytes"`
				InputDevicesHealthy  int64     `json:"input_devices_healthy"`
				InputDevicesMissing  int64     `json:"input_devices_missing"`
				ProcessStatuses      []struct {
					Name         string `json:"name"`
					Running      bool   `json:"running"`
					ProcessCount int    `json:"process_count"`
				} `json:"process_statuses"`
				LinkState struct {
					Interface  string `json:"interface"`
					Type       string `json:"type"`
					LinkUp     bool   `json:"link_up"`
					SpeedMbps  int    `json:"speed_mbps"`
					DuplexFull bool   `json:"duplex_full"`
					Autoneg    bool   `json:"autoneg"`
					RXDropped  int64  `json:"rx_dropped"`
				} `json:"link_state"`
				Uptime int64 `json:"uptime"`
			} `json:"data"`
		}
		if json.Unmarshal(body, &payload) != nil || len(payload.Data) == 0 {
			answer = fmt.Sprintf("No telemetry was found for device '%s'.", host)
		} else {
			entry := payload.Data[0]
			var sections []string
			if wantsTemp {
				tempChunks := make([]string, 0, 3)
				tempChunks = append(tempChunks, fmt.Sprintf("ambient %.1f°C", entry.Temperature))
				if entry.ChassisTemperature != 0 {
					tempChunks = append(tempChunks, fmt.Sprintf("chassis %.1f°C", entry.ChassisTemperature))
				}
				if entry.HotspotTemperature != 0 {
					tempChunks = append(tempChunks, fmt.Sprintf("hotspot %.1f°C", entry.HotspotTemperature))
				}
				sections = append(sections, "Temperature: "+strings.Join(tempChunks, ", "))
			}
			if wantsVolume || wantsMute {
				vol := fmt.Sprintf("%.0f%%", entry.SoundVolumePercent)
				if wantsMute {
					if entry.SoundMuted {
						sections = append(sections, fmt.Sprintf("Volume muted (level %s).", vol))
					} else {
						sections = append(sections, fmt.Sprintf("Volume active at %s (not muted).", vol))
					}
				} else {
					status := "Volume " + vol
					if entry.SoundMuted {
						status += " (muted)"
					}
					sections = append(sections, status)
				}
			}
			if wantsPower {
				state := "Power offline"
				if entry.PowerOnline {
					state = "Power online"
				}
				sections = append(sections, state)
			}
			if wantsBattery {
				if entry.BatteryPresent {
					sections = append(sections, fmt.Sprintf("Battery %d%% charge.", entry.BatteryChargePercent))
				} else {
					sections = append(sections, "Battery not present.")
				}
			}
			if wantsDisplay {
				if entry.DisplayConnected {
					sections = append(sections, fmt.Sprintf("Display %dx%d @ %dHz (DPMS %v).", entry.DisplayWidth, entry.DisplayHeight, entry.DisplayRefreshHz, boolToOnOff(!entry.DisplayDpmsEnabled)))
				} else {
					sections = append(sections, "Display disconnected.")
				}
			}
			if wantsFan {
				sections = append(sections, fmt.Sprintf("Fan %d RPM.", entry.FanRPM))
			}
			if wantsCPU || wantsMemory {
				var stats []string
				if wantsCPU {
					stats = append(stats, fmt.Sprintf("CPU %.1f%%", entry.CPU))
				}
				if wantsMemory {
					stats = append(stats, fmt.Sprintf("Memory %.1f%%", entry.Memory))
				}
				if len(stats) > 0 {
					sections = append(sections, strings.Join(stats, ", "))
				}
			}
			if wantsDisk {
				sections = append(sections, fmt.Sprintf("Disk %.1f%% used (%.1f/%0.1f GB).", entry.Disk, bytesToGiB(entry.DiskUsedBytes), bytesToGiB(entry.DiskTotalBytes)))
			}
			if wantsNetwork {
				sections = append(sections, fmt.Sprintf("Network daily RX %.1f MB, TX %.1f MB (current RX %d bytes).", bytesToMiB(entry.NetDailyRxBytes), bytesToMiB(entry.NetDailyTxBytes), entry.NetBytesRecv))
			}
			if wantsProcesses && len(entry.ProcessStatuses) > 0 {
				var offline []string
				for _, ps := range entry.ProcessStatuses {
					if !ps.Running {
						offline = append(offline, ps.Name)
					}
				}
				if len(offline) == 0 {
					sections = append(sections, "All monitored processes running.")
				} else {
					sections = append(sections, "Processes down: "+strings.Join(offline, ", "))
				}
			}
			if wantsInputDevices {
				sections = append(sections, fmt.Sprintf("Input devices healthy %d, missing %d.", entry.InputDevicesHealthy, entry.InputDevicesMissing))
			}
			if wantsNetwork && (entry.LinkState.Interface != "" || entry.LinkState.SpeedMbps > 0) {
				link := entry.LinkState
				status := "link down"
				if link.LinkUp {
					status = "link up"
				}
				sections = append(sections, fmt.Sprintf("Interface %s (%s) %s @ %dMbps, duplex=%v.", link.Interface, link.Type, status, link.SpeedMbps, link.DuplexFull))
			}
			if wantsUptime && entry.Uptime > 0 {
				uptime := time.Duration(entry.Uptime) * time.Second
				sections = append(sections, fmt.Sprintf("Uptime %s.", uptime.String()))
			}
			if len(sections) == 0 {
				sections = append(sections, "No matching telemetry fields requested.")
			}
			timestamp := entry.Time.UTC().Format(time.RFC3339)
			answer = fmt.Sprintf("Latest telemetry for '%s': %s (recorded %s UTC).", host, strings.Join(sections, " | "), timestamp)
		}
	}

	if onToken != nil {
		onToken(answer)
	}
	return models.ChatResponse{Answer: answer, Steps: []models.Step{step}}, true, nil
}

func (c *ChatService) regionCodes(ctx context.Context) []string {
	c.cityMu.Lock()
	defer c.cityMu.Unlock()

	c.ensureCityRegionCachesLocked(ctx)
	if c.regionCache != nil && !c.regionCacheAt.IsZero() && time.Since(c.regionCacheAt) < c.cityCacheTTL {
		out := make([]string, 0, len(c.regionCache))
		for k := range c.regionCache {
			out = append(out, k)
		}
		return out
	}
	if c.regionCache != nil {
		out := make([]string, 0, len(c.regionCache))
		for k := range c.regionCache {
			out = append(out, k)
		}
		return out
	}
	return nil
}

func (c *ChatService) detectRegionCode(ctx context.Context, msgLower string) string {
	s := strings.ToLower(strings.TrimSpace(msgLower))
	if s == "" {
		return ""
	}

	// Minimal aliases for common user phrasing.
	if strings.Contains(s, "bus rapid transit") {
		return "brt"
	}

	codes := c.regionCodes(ctx)
	if len(codes) == 0 {
		return ""
	}
	sort.Slice(codes, func(i, j int) bool { return len(codes[i]) > len(codes[j]) })
	for _, r := range codes {
		r = strings.ToLower(strings.TrimSpace(r))
		if r == "" {
			continue
		}
		if strings.Contains(s, r) {
			return r
		}
	}
	return ""
}

func looksLikeUUID(s string) bool {
	s = strings.TrimSpace(s)
	if len(s) != 36 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '-' {
			continue
		}
		if (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F') {
			continue
		}
		return false
	}
	return s[8] == '-' && s[13] == '-' && s[18] == '-' && s[23] == '-'
}

func parseSelectedDays(msgLower string) []string {
	s := strings.ToLower(msgLower)
	ordered := []struct {
		k   string
		out string
	}{
		{"monday", "mon"},
		{"mon", "mon"},
		{"tuesday", "tue"},
		{"tue", "tue"},
		{"wednesday", "wed"},
		{"wed", "wed"},
		{"thursday", "thu"},
		{"thu", "thu"},
		{"friday", "fri"},
		{"fri", "fri"},
		{"saturday", "sat"},
		{"sat", "sat"},
		{"sunday", "sun"},
		{"sun", "sun"},
	}
	seen := map[string]struct{}{}
	res := make([]string, 0)
	for _, it := range ordered {
		if strings.Contains(s, it.k) {
			if _, ok := seen[it.out]; !ok {
				seen[it.out] = struct{}{}
				res = append(res, it.out)
			}
		}
	}
	return res
}

func extractCampaignRows(parsed map[string]any) []any {
	if parsed == nil {
		return nil
	}
	if rows, ok := parsed["data"].([]any); ok {
		return rows
	}
	if rows, ok := parsed["items"].([]any); ok {
		return rows
	}
	if d, ok := parsed["data"].(map[string]any); ok {
		if rows, ok := d["campaigns"].([]any); ok {
			return rows
		}
		if rows, ok := d["items"].([]any); ok {
			return rows
		}
	}
	return nil
}

func formatCampaignSuggestions(rows []any, limit int) []string {
	if limit <= 0 {
		limit = 5
	}
	suggestions := make([]string, 0, limit)
	for _, it := range rows {
		m, ok := it.(map[string]any)
		if !ok {
			continue
		}
		id, _ := m["id"].(string)
		nm, _ := m["name"].(string)
		id = strings.TrimSpace(id)
		nm = strings.TrimSpace(nm)
		if id == "" || nm == "" {
			continue
		}
		suggestions = append(suggestions, nm+" ("+id+")")
		if len(suggestions) >= limit {
			break
		}
	}
	return suggestions
}

func parseTimeSlots(msg string) []string {
	re := regexp.MustCompile(`(?i)\b([01]?[0-9]|2[0-3]):[0-5][0-9]\s*-\s*([01]?[0-9]|2[0-3]):[0-5][0-9]\b`)
	matches := re.FindAllString(msg, -1)
	res := make([]string, 0, len(matches))
	seen := map[string]struct{}{}
	for _, m := range matches {
		m = strings.ReplaceAll(m, " ", "")
		if _, ok := seen[m]; ok {
			continue
		}
		seen[m] = struct{}{}
		res = append(res, m)
	}
	return res
}

func parseDevicesList(msgLower string) []string {
	key := "devices"
	idx := strings.Index(msgLower, key)
	if idx < 0 {
		return nil
	}
	rest := strings.TrimSpace(msgLower[idx+len(key):])
	rest = strings.TrimLeft(rest, " :")
	if rest == "" {
		return nil
	}
	stop := len(rest)
	for _, sep := range []string{"\n", ";", " with ", " where ", " from ", " campaign", " day", " slot", " time"} {
		if j := strings.Index(rest, sep); j >= 0 && j < stop {
			stop = j
		}
	}
	chunk := strings.TrimSpace(rest[:stop])
	chunk = strings.Trim(chunk, "[](){}")
	if chunk == "" {
		return nil
	}
	parts := strings.FieldsFunc(chunk, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t'
	})
	res := make([]string, 0, len(parts))
	seen := map[string]struct{}{}
	for _, p := range parts {
		p = strings.TrimSpace(strings.Trim(p, "\"'"))
		if p == "" {
			continue
		}
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		res = append(res, p)
	}
	return res
}

func extractPosterLookupToken(msgLower string) string {
	s := strings.ToLower(strings.TrimSpace(msgLower))
	if s == "" {
		return ""
	}
	needsPosterContext := !(strings.Contains(s, "poster") || strings.Contains(s, "creative") || strings.Contains(s, "detail"))
	tokens := strings.FieldsFunc(msgLower, func(r rune) bool {
		return r == ' ' || r == '\n' || r == '\t' || r == ',' || r == ';'
	})
	for _, raw := range tokens {
		token := strings.Trim(raw, "\"'.,;:()[]{}")
		if token == "" {
			continue
		}
		tokenLower := strings.ToLower(token)
		if looksLikeUUID(token) {
			continue
		}
		if !strings.Contains(tokenLower, "_") && !strings.Contains(tokenLower, "-") {
			continue
		}
		if needsPosterContext && !strings.HasPrefix(tokenLower, "vistar") {
			continue
		}
		if len(token) < 8 {
			continue
		}
		return token
	}
	return ""
}

func detectHostTokens(msg string) []string {
	if strings.TrimSpace(msg) == "" {
		return nil
	}
	parts := strings.FieldsFunc(msg, func(r rune) bool {
		switch r {
		case ' ', '\n', '\t', ',', ';', ':', '/', '\\', '|':
			return true
		}
		return strings.ContainsRune("()[]{}\"", r)
	})
	var out []string
	seen := map[string]struct{}{}
	for _, raw := range parts {
		token := strings.Trim(raw, "\"'.,;:()[]{}")
		if len(token) < 3 || len(token) > 50 {
			continue
		}
		tokenLower := strings.ToLower(token)
		if looksLikeUUID(tokenLower) {
			continue
		}
		hasDigit := false
		hasLetter := false
		valid := true
		for _, ch := range tokenLower {
			if ch >= '0' && ch <= '9' {
				hasDigit = true
				continue
			}
			if ch >= 'a' && ch <= 'z' {
				hasLetter = true
				continue
			}
			if ch == '-' || ch == '_' {
				continue
			}
			if ch == '.' {
				continue
			}
			valid = false
			break
		}
		if !valid || !hasDigit || !hasLetter {
			continue
		}
		if _, exists := seen[tokenLower]; exists {
			continue
		}
		seen[tokenLower] = struct{}{}
		out = append(out, strings.TrimSpace(token))
	}
	return out
}

func isCreativeUploadIntent(msgLower string) bool {
	s := strings.ToLower(strings.TrimSpace(msgLower))
	if s == "" {
		return false
	}
	if strings.Contains(s, "upload") && (strings.Contains(s, "creative") || strings.Contains(s, "file") || strings.Contains(s, "poster")) {
		return true
	}
	return false
}

func (c *ChatService) resolveCampaignID(ctx context.Context, msgLower string) string {
	if id := extractCampaignID(msgLower); looksLikeUUID(id) {
		return id
	}
	campaignName := extractAfterKeyword(msgLower, "campaign")
	if campaignName == "" {
		campaignName = extractAfterKeyword(msgLower, "campaign:")
	}
	if campaignName == "" {
		campaignName = extractAfterKeyword(msgLower, "to")
	}
	campaignName = strings.TrimSpace(campaignName)
	if campaignName == "" {
		return ""
	}
	status, body, err := c.Gateway.Get("/ads/campaigns?page=1&page_size=200")
	if err != nil || status < 200 || status >= 300 {
		return ""
	}
	var parsed map[string]any
	if json.Unmarshal(body, &parsed) != nil {
		return ""
	}
	rows := extractCampaignRows(parsed)
	nameLower := strings.ToLower(campaignName)
	nameTokens := strings.FieldsFunc(nameLower, func(r rune) bool {
		return r == ' ' || r == '\t' || r == '-' || r == '_' || r == ',' || r == ';'
	})
	bestID := ""
	bestScore := -1
	for _, it := range rows {
		m, ok := it.(map[string]any)
		if !ok {
			continue
		}
		id, _ := m["id"].(string)
		if !looksLikeUUID(id) {
			continue
		}
		nm, _ := m["name"].(string)
		nmLower := strings.ToLower(strings.TrimSpace(nm))
		if nmLower == "" {
			continue
		}
		score := -1
		if nmLower == nameLower {
			score = 1000
		} else if strings.Contains(nmLower, nameLower) {
			score = 500
		} else {
			// Token overlap fallback (handles small suffix/prefix differences).
			tokScore := 0
			for _, t := range nameTokens {
				if t == "" {
					continue
				}
				if strings.Contains(nmLower, t) {
					tokScore++
				}
			}
			if tokScore > 0 {
				score = tokScore
			}
		}
		if score > bestScore {
			bestScore = score
			bestID = id
		}
	}
	if bestScore <= 0 {
		return ""
	}
	return bestID
}

func (c *ChatService) handleCreativeUpload(ctx context.Context, ownerKey string, req models.ChatRequest) (models.ChatResponse, bool, error) {
	msgLower := strings.ToLower(req.Message)
	if isListCampaignsIntent(msgLower) {
		if c.Gateway == nil {
			return models.ChatResponse{Answer: "Tool gateway is not configured."}, true, nil
		}
		status, body, err := c.Gateway.Get("/ads/campaigns?page=1&page_size=50")
		step := models.Step{Tool: "adsCampaigns", Status: status}
		if err != nil {
			step.Error = err.Error()
			return models.ChatResponse{Answer: "Failed to list campaigns: " + err.Error(), Steps: []models.Step{step}}, true, nil
		}
		step.Body = clipString(strings.TrimSpace(string(body)), 2000)
		if status < 200 || status >= 300 {
			return models.ChatResponse{Answer: fmt.Sprintf("Failed to list campaigns (status %d).", status), Steps: []models.Step{step}}, true, nil
		}
		var parsed map[string]any
		if json.Unmarshal(body, &parsed) != nil {
			return models.ChatResponse{Answer: "Failed to parse campaign list.", Steps: []models.Step{step}}, true, nil
		}
		rows := extractCampaignRows(parsed)
		suggestions := formatCampaignSuggestions(rows, 10)
		if len(suggestions) == 0 {
			return models.ChatResponse{Answer: "No campaigns found.", Steps: []models.Step{step}}, true, nil
		}
		return models.ChatResponse{Answer: "Campaigns: " + strings.Join(suggestions, "; "), Steps: []models.Step{step}}, true, nil
	}

	if !isCreativeUploadIntent(msgLower) {
		return models.ChatResponse{}, false, nil
	}
	if len(req.Attachments) == 0 {
		return models.ChatResponse{Answer: "To upload creatives, attach the file(s) and include: campaign (id or name), selected days, time slots, and devices."}, true, nil
	}
	devices := parseDevicesList(msgLower)
	if len(devices) == 0 {
		return models.ChatResponse{Answer: "Devices are required for creative upload. Please specify devices (e.g. devices: dev1,dev2,dev3)."}, true, nil
	}
	days := parseSelectedDays(msgLower)
	if len(days) == 0 {
		return models.ChatResponse{Answer: "Please specify selected days for the creative (e.g. mon,tue,wed)."}, true, nil
	}
	slots := parseTimeSlots(req.Message)
	if len(slots) == 0 {
		return models.ChatResponse{Answer: "Please specify time slots in HH:MM-HH:MM format (e.g. 08:00-12:00, 12:00-16:00)."}, true, nil
	}
	if c.Gateway == nil {
		return models.ChatResponse{Answer: "Upload is not available because tool gateway is not configured."}, true, nil
	}
	campaignID := c.resolveCampaignID(ctx, msgLower)
	if campaignID == "" {
		status, body, err := c.Gateway.Get("/ads/campaigns?page=1&page_size=50")
		if err != nil {
			return models.ChatResponse{Answer: "Please specify a valid campaign (campaign_id UUID or campaign name). Also failed to fetch campaign list: " + err.Error()}, true, nil
		}
		if status < 200 || status >= 300 {
			return models.ChatResponse{Answer: fmt.Sprintf("Please specify a valid campaign (campaign_id UUID or campaign name). Also failed to fetch campaign list (status %d): %s", status, clipString(strings.TrimSpace(string(body)), 500))}, true, nil
		}
		var parsed map[string]any
		if json.Unmarshal(body, &parsed) != nil {
			return models.ChatResponse{Answer: "Please specify a valid campaign (campaign_id UUID or campaign name). Also could not parse campaign list: " + clipString(strings.TrimSpace(string(body)), 500)}, true, nil
		}
		rows := extractCampaignRows(parsed)
		suggestions := formatCampaignSuggestions(rows, 10)
		if len(suggestions) == 0 {
			return models.ChatResponse{Answer: "Please specify a valid campaign (campaign_id UUID or campaign name). Campaign list appears empty or in an unexpected format: " + clipString(strings.TrimSpace(string(body)), 500)}, true, nil
		}
		return models.ChatResponse{Answer: "Please specify a valid campaign. Here are campaigns I can see: " + strings.Join(suggestions, "; ")}, true, nil
	}

	fields := map[string][]string{
		"campaign_id":    {campaignID},
		"selected_days":  days,
		"time_slots":     slots,
		"devices":        devices,
	}
	files := make([]MultipartFile, 0, len(req.Attachments))
	for _, a := range req.Attachments {
		b64 := strings.TrimSpace(a.Base64)
		if b64 == "" {
			continue
		}
		files = append(files, MultipartFile{
			FieldName:   "files",
			FileName:    strings.TrimSpace(a.FileName),
			ContentType: strings.TrimSpace(a.ContentType),
			Base64:      b64,
		})
	}
	if len(files) == 0 {
		return models.ChatResponse{Answer: "Attachment(s) missing base64 content. Please attach the file again."}, true, nil
	}

	status, body, err := c.Gateway.DoMultipart("POST", "/ads/creatives/upload", nil, MultipartPayload{Fields: fields, Files: files})
	step := models.Step{Tool: "adsCreativesUpload", Status: status}
	if err != nil {
		step.Error = err.Error()
	} else {
		step.Body = clipString(strings.TrimSpace(string(body)), 2000)
	}
	answer := ""
	if err != nil {
		answer = "Creative upload failed: " + err.Error()
	} else if status < 200 || status >= 300 {
		answer = fmt.Sprintf("Creative upload failed with status %d.", status)
	} else {
		answer = "Creative upload successful."
	}
	return models.ChatResponse{Answer: answer, Steps: []models.Step{step}}, true, nil
}

func (c *ChatService) handlePosterDetails(ctx context.Context, req models.ChatRequest, onToken func(string)) (models.ChatResponse, bool, error) {
	msgLower := strings.ToLower(req.Message)
	token := extractPosterLookupToken(msgLower)
	if token == "" {
		return models.ChatResponse{}, false, nil
	}
	if c.Gateway == nil {
		return models.ChatResponse{Answer: "Tool gateway is not configured."}, true, nil
	}
	path := "/ads/creatives/search?query=" + urlEscape(token)
	status, body, err := c.Gateway.Get(path)
	step := models.Step{Tool: "adsCreativesSearch", Status: status}
	if err != nil {
		step.Error = err.Error()
	} else {
		step.Body = clipString(strings.TrimSpace(string(body)), 2000)
	}
	answer := ""
	if err != nil {
		answer = "Failed to search creatives: " + err.Error()
	} else if status < 200 || status >= 300 {
		answer = fmt.Sprintf("Creative search failed with status %d.", status)
	} else {
		var parsed map[string]any
		if json.Unmarshal(body, &parsed) != nil {
			answer = "Creative search returned an unreadable response."
		} else {
			items, _ := parsed["data"].([]any)
			if len(items) == 0 {
				answer = fmt.Sprintf("No creatives matched '%s'.", token)
			} else {
				lines := make([]string, 0, min(3, len(items)))
				for _, it := range items {
					row, ok := it.(map[string]any)
					if !ok {
						continue
					}
					name, _ := row["name"].(string)
					id, _ := row["id"].(string)
					campaignID, _ := row["campaign_id"].(string)
					urlStr, _ := row["url"].(string)
					line := name
					if line == "" {
						line = id
					}
					if strings.TrimSpace(line) == "" {
						continue
					}
					extras := make([]string, 0, 2)
					if campaignID != "" {
						extras = append(extras, "campaign "+campaignID)
					}
					if urlStr != "" {
						extras = append(extras, urlStr)
					}
					if len(extras) > 0 {
						line = line + " (" + strings.Join(extras, ", ") + ")"
					}
					lines = append(lines, line)
					if len(lines) >= 3 {
						break
					}
				}
				if len(lines) == 0 {
					answer = fmt.Sprintf("Creatives were found for '%s', but the records were missing readable fields.", token)
				} else {
					answer = fmt.Sprintf("Found creatives matching '%s':\n%s", token, strings.Join(lines, "\n"))
				}
			}
		}
	}
	if onToken != nil && answer != "" {
		onToken(answer)
	}
	return models.ChatResponse{Answer: answer, Steps: []models.Step{step}}, true, nil
}

func (c *ChatService) handleTopPostersFromCity(ctx context.Context, req models.ChatRequest, onToken func(string)) (models.ChatResponse, bool, error) {
	msgLower := strings.ToLower(req.Message)
	if !isTopPostersFromCityIntent(msgLower) {
		return models.ChatResponse{}, false, nil
	}
	city := c.detectCityCode(ctx, msgLower)
	if city == "" {
		return models.ChatResponse{Answer: "Please specify a city code (for example: kcmo, kc, brt, dart)."}, true, nil
	}
	if c.Gateway == nil {
		return models.ChatResponse{Answer: "Tool gateway is not configured."}, true, nil
	}
	metric := "clicks"
	if strings.Contains(msgLower, "play") {
		metric = "plays"
	}
	path := "/pop/stats?group_by=poster&metric=" + metric + "&order=top&limit=10&city=" + urlEscape(city)
	status, body, err := c.Gateway.Get(path)
	step := models.Step{Tool: "popStats", Status: status}
	if err != nil {
		step.Error = err.Error()
	} else {
		step.Body = clipString(strings.TrimSpace(string(body)), 2000)
	}
	answer := ""
	if err != nil {
		answer = "Failed to fetch POP stats: " + err.Error()
	} else if status < 200 || status >= 300 {
		answer = fmt.Sprintf("Failed to fetch POP stats (status %d).", status)
	} else {
		var parsed map[string]any
		_ = json.Unmarshal(body, &parsed)
		itemsAny, _ := parsed["items"].([]any)
		if len(itemsAny) == 0 {
			answer = fmt.Sprintf("No poster %s stats found for city '%s'.", metric, city)
		} else {
			lines := make([]string, 0, len(itemsAny))
			for i, it := range itemsAny {
				if i >= 10 {
					break
				}
				row, ok := it.(map[string]any)
				if !ok {
					continue
				}
				name, _ := row["PosterName"].(string)
				if strings.TrimSpace(name) == "" {
					name, _ = row["Key"].(string)
				}
				val := 0.0
				switch v := row["Metric"].(type) {
				case float64:
					val = v
				case int:
					val = float64(v)
				}
				if strings.TrimSpace(name) == "" {
					continue
				}
				lines = append(lines, fmt.Sprintf("%d. %s — %.0f %s", len(lines)+1, name, val, metric))
			}
			answer = fmt.Sprintf("Top posters in %s by %s:\n%s", city, metric, strings.Join(lines, "\n"))
		}
	}
	if onToken != nil {
		for i := 0; i < len(answer); i += 20 {
			end := i + 20
			if end > len(answer) {
				end = len(answer)
			}
			onToken(answer[i:end])
		}
	}
	return models.ChatResponse{Answer: answer, Steps: []models.Step{step}}, true, nil
}

func (c *ChatService) handleTopDevicesFromCity(ctx context.Context, req models.ChatRequest, onToken func(string)) (models.ChatResponse, bool, error) {
	msgLower := strings.ToLower(req.Message)
	if !isTopDevicesFromCityIntent(msgLower) {
		return models.ChatResponse{}, false, nil
	}
	city := c.detectCityCode(ctx, msgLower)
	if city == "" {
		return models.ChatResponse{Answer: "Please specify a city code (for example: kcmo, kc, brt, dart)."}, true, nil
	}
	if c.Gateway == nil {
		return models.ChatResponse{Answer: "Tool gateway is not configured."}, true, nil
	}
	metric := "clicks"
	if strings.Contains(msgLower, "play") {
		metric = "plays"
	}
	path := "/pop/stats?group_by=device&metric=" + metric + "&order=top&limit=10&city=" + urlEscape(city)
	status, body, err := c.Gateway.Get(path)
	step := models.Step{Tool: "popStats", Status: status}
	if err != nil {
		step.Error = err.Error()
	} else {
		step.Body = clipString(strings.TrimSpace(string(body)), 2000)
	}
	answer := ""
	if err != nil {
		answer = "Failed to fetch POP stats: " + err.Error()
	} else if status < 200 || status >= 300 {
		answer = fmt.Sprintf("Failed to fetch POP stats (status %d).", status)
	} else {
		var parsed map[string]any
		_ = json.Unmarshal(body, &parsed)
		itemsAny, _ := parsed["items"].([]any)
		if len(itemsAny) == 0 {
			answer = fmt.Sprintf("No device %s stats found for city '%s'.", metric, city)
		} else {
			lines := make([]string, 0, len(itemsAny))
			for i, it := range itemsAny {
				if i >= 10 {
					break
				}
				row, ok := it.(map[string]any)
				if !ok {
					continue
				}
				k, _ := row["Key"].(string)
				val := 0.0
				switch v := row["Metric"].(type) {
				case float64:
					val = v
				case int:
					val = float64(v)
				}
				if strings.TrimSpace(k) == "" {
					continue
				}
				lines = append(lines, fmt.Sprintf("%d. %s — %.0f %s", len(lines)+1, k, val, metric))
			}
			answer = fmt.Sprintf("Top devices in %s by %s:\n%s", city, metric, strings.Join(lines, "\n"))
		}
	}
	if onToken != nil {
		for i := 0; i < len(answer); i += 20 {
			end := i + 20
			if end > len(answer) {
				end = len(answer)
			}
			onToken(answer[i:end])
		}
	}
	return models.ChatResponse{Answer: answer, Steps: []models.Step{step}}, true, nil
}

type gwCampaignImpressionsResponse struct {
	Data *models.CampaignImpressions `json:"data"`
}

func clipString(s string, max int) string {
	if max <= 0 {
		return ""
	}
	if len(s) <= max {
		return s
	}
	return s[:max]
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func extractDateRangeRFC3339(msgLower string) (string, string) {
	// Accept a simple pattern in the user message: "from YYYY-MM-DD to YYYY-MM-DD".
	// POP API expects RFC3339; normalize to UTC day boundaries.
	s := strings.ToLower(strings.TrimSpace(msgLower))
	if s == "" {
		return "", ""
	}
	idx := strings.Index(s, " to ")
	if idx < 0 {
		return "", ""
	}
	left := strings.TrimSpace(s[:idx])
	right := strings.TrimSpace(s[idx+4:])
	fromTok := left
	if j := strings.LastIndex(left, "from "); j >= 0 {
		fromTok = strings.TrimSpace(left[j+5:])
	}
	toTok := right
	if k := strings.IndexAny(toTok, "\n,;."); k >= 0 {
		toTok = strings.TrimSpace(toTok[:k])
	}
	if len(fromTok) < 10 || len(toTok) < 10 {
		return "", ""
	}
	fromTok = strings.TrimSpace(fromTok[:10])
	toTok = strings.TrimSpace(toTok[:10])
	fromT, err1 := time.Parse("2006-01-02", fromTok)
	toT, err2 := time.Parse("2006-01-02", toTok)
	if err1 != nil || err2 != nil {
		return "", ""
	}
	from := time.Date(fromT.Year(), fromT.Month(), fromT.Day(), 0, 0, 0, 0, time.UTC)
	to := time.Date(toT.Year(), toT.Month(), toT.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, 1)
	if !from.Before(to) {
		return "", ""
	}
	return from.Format(time.RFC3339), to.Format(time.RFC3339)
}

func extractCampaignID(s string) string {
	tokens := strings.FieldsFunc(s, func(r rune) bool {
		return r == ' ' || r == '\n' || r == '\t' || r == ',' || r == ';'
	})
	for _, t := range tokens {
		u := strings.TrimSpace(t)
		u = strings.Trim(u, "()[]{}\"' ")
		if looksLikeUUID(u) {
			return u
		}
	}
	return ""
}

func extractStatusFilter(msgLower string) string {
	if strings.Contains(msgLower, "scheduled") {
		return "scheduled"
	}
	if strings.Contains(msgLower, "paused") {
		return "paused"
	}
	if strings.Contains(msgLower, "active") {
		return "active"
	}
	return ""
}

func extractAfterKeyword(msgLower, keyword string) string {
	idx := strings.Index(msgLower, keyword)
	if idx < 0 {
		return ""
	}
	rest := strings.TrimSpace(msgLower[idx+len(keyword):])
	if rest == "" {
		return ""
	}
	stop := len(rest)
	for _, sep := range []string{"\n", ",", ";", " with ", " where ", " from ", " status ", " active", " scheduled", " paused", " selected", " day", " days", " time", " slot", " slots", " device", " devices"} {
		if j := strings.Index(rest, sep); j >= 0 && j < stop {
			stop = j
		}
	}
	name := strings.TrimSpace(rest[:stop])
	name = strings.Trim(name, "\"' ")
	return name
}

func urlEscape(s string) string {
	replacer := strings.NewReplacer("%", "%25", "/", "%2F", "?", "%3F", "#", "%23")
	return replacer.Replace(s)
}

func cityFromDeviceKey(key string) string {
	k := strings.ToLower(strings.TrimSpace(key))
	if k == "" {
		return ""
	}
	// Normalize separators so keys like "moco_brt_web" are parsed similarly.
	k = strings.ReplaceAll(k, "_", "-")
	parts := strings.Split(k, "-")
	if len(parts) == 0 {
		return ""
	}
	known := map[string]string{
		"brt":  "BRT",
		"dart": "DART",
		"kc":   "KC",
		"kcmo": "KCMO",
		"jct":  "JCT",
		"au":   "AU",
		"da":   "DA",
	}
	if v, ok := known[strings.TrimSpace(parts[0])]; ok {
		return v
	}
	if len(parts) > 1 {
		if v, ok := known[strings.TrimSpace(parts[1])]; ok {
			return v
		}
	}
	return ""
}

func (c *ChatService) prefetchImpressions(ctx context.Context, msg string) (*models.ChatData, []models.Step, map[string]any) {
	steps := make([]models.Step, 0)
	data := &models.ChatData{}
	var toolData map[string]any

	msgLower := strings.ToLower(msg)
	// City ranking queries like "which city has most clicks" are answered by aggregating device click stats.
	isCityMostClicks := (strings.Contains(msgLower, "city") && strings.Contains(msgLower, "click") && (strings.Contains(msgLower, "most") || strings.Contains(msgLower, "highest") || strings.Contains(msgLower, "top")))

	cityCodeForQuery := c.detectCityCode(ctx, msgLower)
	regionCodeForQuery := c.detectRegionCode(ctx, msgLower)
	if strings.Contains(msgLower, "impression") {
		campaignID := extractCampaignID(msg)
		if campaignID != "" {
			status, body, err := c.Gateway.Get("/ads/campaigns/" + urlEscape(campaignID) + "/impressions")
			step := models.Step{Tool: "adsCampaignImpressions", CampaignID: campaignID, Status: status}
			if err != nil {
				step.Error = err.Error()
			} else {
				step.Body = clipString(strings.TrimSpace(string(body)), 2000)
			}
			steps = append(steps, step)
			if err == nil && status >= 200 && status < 300 {
				var parsed any
				if json.Unmarshal(body, &parsed) == nil {
					if toolData == nil {
						toolData = map[string]any{}
					}
					toolData["ads_campaign_impressions"] = parsed
				}
				var gw gwCampaignImpressionsResponse
				if json.Unmarshal(body, &gw) == nil {
					data.CampaignImpressions = gw.Data
				}
			}
		}
	}

	if strings.Contains(msgLower, "advertiser") {
		status, body, err := c.Gateway.Get("/ads/advertisers")
		step := models.Step{Tool: "adsAdvertisers", Status: status}
		if err != nil {
			step.Error = err.Error()
		} else {
			step.Body = clipString(strings.TrimSpace(string(body)), 2000)
		}
		steps = append(steps, step)
		if err == nil && status >= 200 && status < 300 {
			var parsed any
			if json.Unmarshal(body, &parsed) == nil {
				if toolData == nil {
					toolData = map[string]any{}
				}
				toolData["ads_advertisers"] = parsed
			}
		}
	}

	if strings.Contains(msgLower, "campaign") && (strings.Contains(msgLower, "list") || strings.Contains(msgLower, "show") || strings.Contains(msgLower, "all")) {
		advertiserID := ""
		statusFilter := extractStatusFilter(msgLower)
		// Try to pick an explicit UUID if user included it.
		if strings.Contains(msgLower, "advertiser") {
			advertiserID = extractCampaignID(msg)
		}
		// Resolve advertiser name to ID from cached advertisers, or fetch advertisers if needed.
		advName := ""
		if advertiserID == "" && strings.Contains(msgLower, "advertiser") {
			advName = extractAfterKeyword(msgLower, "advertiser")
			if advName == "" {
				advName = extractAfterKeyword(msgLower, "advertiser ")
			}
		}
		if advertiserID == "" && advName != "" {
			// Ensure advertisers are available.
			if toolData == nil || toolData["ads_advertisers"] == nil {
				status, body, err := c.Gateway.Get("/ads/advertisers")
				step := models.Step{Tool: "adsAdvertisers", Status: status}
				if err != nil {
					step.Error = err.Error()
				} else {
					step.Body = clipString(strings.TrimSpace(string(body)), 2000)
				}
				steps = append(steps, step)
				if err == nil && status >= 200 && status < 300 {
					var parsed any
					if json.Unmarshal(body, &parsed) == nil {
						if toolData == nil {
							toolData = map[string]any{}
						}
						toolData["ads_advertisers"] = parsed
					}
				}
			}
			if toolData != nil {
				if root, ok := toolData["ads_advertisers"].(map[string]any); ok {
					if rows, ok := root["data"].([]any); ok {
						for _, it := range rows {
							m, ok := it.(map[string]any)
							if !ok {
								continue
							}
							name, _ := m["name"].(string)
							id, _ := m["id"].(string)
							if id != "" && strings.Contains(strings.ToLower(name), strings.ToLower(advName)) {
								advertiserID = id
								break
							}
						}
					}
				}
			}
		}

		// Fetch campaigns (max page_size) and filter locally when needed.
		status, body, err := c.Gateway.Get("/ads/campaigns?page=1&page_size=200")
		step := models.Step{Tool: "adsCampaigns", Status: status}
		if err != nil {
			step.Error = err.Error()
		} else {
			step.Body = clipString(strings.TrimSpace(string(body)), 2000)
		}
		steps = append(steps, step)
		if err == nil && status >= 200 && status < 300 {
			var parsed map[string]any
			if json.Unmarshal(body, &parsed) == nil {
				if toolData == nil {
					toolData = map[string]any{}
				}
				toolData["ads_campaigns"] = parsed
				// Optional filtered view
				if advertiserID != "" || statusFilter != "" {
					out := make([]any, 0)
					if rows, ok := parsed["data"].([]any); ok {
						for _, it := range rows {
							m, ok := it.(map[string]any)
							if !ok {
								continue
							}
							if advertiserID != "" {
								advID, _ := m["advertiser_id"].(string)
								if advID != advertiserID {
									continue
								}
							}
							if statusFilter != "" {
								st, _ := m["status"].(string)
								if strings.ToLower(strings.TrimSpace(st)) != statusFilter {
									continue
								}
							}
							out = append(out, m)
						}
					}
					toolData["ads_campaigns_filtered"] = map[string]any{"data": out}
				}
			}
		}
	}

	if strings.Contains(msgLower, "creative") && (strings.Contains(msgLower, "list") || strings.Contains(msgLower, "show") || strings.Contains(msgLower, "all")) {
		campaignID := ""
		if strings.Contains(msgLower, "campaign") {
			campaignID = extractCampaignID(msg)
		}
		campName := ""
		if campaignID == "" && strings.Contains(msgLower, "campaign") {
			campName = extractAfterKeyword(msgLower, "campaign")
			if campName == "" {
				campName = extractAfterKeyword(msgLower, "campaign ")
			}
		}
		if campaignID == "" && campName != "" {
			// Ensure campaigns are available.
			if toolData == nil || toolData["ads_campaigns"] == nil {
				status, body, err := c.Gateway.Get("/ads/campaigns?page=1&page_size=200")
				step := models.Step{Tool: "adsCampaigns", Status: status}
				if err != nil {
					step.Error = err.Error()
				} else {
					step.Body = clipString(strings.TrimSpace(string(body)), 2000)
				}
				steps = append(steps, step)
				if err == nil && status >= 200 && status < 300 {
					var parsed map[string]any
					if json.Unmarshal(body, &parsed) == nil {
						if toolData == nil {
							toolData = map[string]any{}
						}
						toolData["ads_campaigns"] = parsed
					}
				}
			}
			if toolData != nil {
				if root, ok := toolData["ads_campaigns"].(map[string]any); ok {
					if rows, ok := root["data"].([]any); ok {
						for _, it := range rows {
							m, ok := it.(map[string]any)
							if !ok {
								continue
							}
							name, _ := m["name"].(string)
							id, _ := m["id"].(string)
							if id != "" && strings.Contains(strings.ToLower(name), strings.ToLower(campName)) {
								campaignID = id
								break
							}
						}
					}
				}
			}
		}

		path := "/ads/creatives?page=1&page_size=50"
		stepTool := "adsCreatives"
		if campaignID != "" {
			path = "/ads/creatives/campaign/" + urlEscape(campaignID) + "?page=1&page_size=200"
			stepTool = "adsCreativesByCampaign"
		}
		status, body, err := c.Gateway.Get(path)
		step := models.Step{Tool: "adsCreatives", Status: status}
		step.Tool = stepTool
		if err != nil {
			step.Error = err.Error()
		} else {
			step.Body = clipString(strings.TrimSpace(string(body)), 2000)
		}
		steps = append(steps, step)
		if err == nil && status >= 200 && status < 300 {
			var parsed any
			if json.Unmarshal(body, &parsed) == nil {
				if toolData == nil {
					toolData = map[string]any{}
				}
				if campaignID != "" {
					toolData["ads_creatives_by_campaign"] = parsed
				} else {
					toolData["ads_creatives"] = parsed
				}
			}
		}
	}

	if (strings.Contains(msgLower, "device") || strings.Contains(msgLower, "kiosk")) && (strings.Contains(msgLower, "from") || strings.Contains(msgLower, "in") || strings.Contains(msgLower, "city")) {
		if cityCodeForQuery != "" {
			status, body, err := c.Gateway.Get("/ads/devices?page=1&page_size=100&city=" + cityCodeForQuery)
			step := models.Step{Tool: "adsDevices", Status: status}
			if err != nil {
				step.Error = err.Error()
			} else {
				step.Body = clipString(strings.TrimSpace(string(body)), 2000)
			}
			steps = append(steps, step)
			if err == nil && status >= 200 && status < 300 {
				var parsed any
				if json.Unmarshal(body, &parsed) == nil {
					if toolData == nil {
						toolData = map[string]any{}
					}
					toolData["ads_devices"] = parsed
				}
			}
		}
	}

	if (strings.Contains(msgLower, "device") || strings.Contains(msgLower, "kiosk")) && strings.Contains(msgLower, "count") {
		path := "/ads/devices/counts/regions"
		if cityCodeForQuery != "" {
			path += "?city=" + cityCodeForQuery
		}
		status, body, err := c.Gateway.Get(path)
		step := models.Step{Tool: "adsDevicesCountsRegions", Status: status}
		if err != nil {
			step.Error = err.Error()
		} else {
			step.Body = clipString(strings.TrimSpace(string(body)), 2000)
		}
		steps = append(steps, step)
		if err == nil && status >= 200 && status < 300 {
			var parsed any
			if json.Unmarshal(body, &parsed) == nil {
				if toolData == nil {
					toolData = map[string]any{}
				}
				toolData["ads_devices_counts_regions"] = parsed
			}
		}
	}

	if strings.Contains(msgLower, "metric") || strings.Contains(msgLower, "metrics") {
		if strings.Contains(msgLower, "history") {
			status, body, err := c.Gateway.Get("/metrics/history?page=1&page_size=50&include_totals=false")
			step := models.Step{Tool: "metricsHistory", Status: status}
			if err != nil {
				step.Error = err.Error()
			} else {
				step.Body = clipString(strings.TrimSpace(string(body)), 2000)
			}
			steps = append(steps, step)
			if err == nil && status >= 200 && status < 300 {
				var parsed any
				if json.Unmarshal(body, &parsed) == nil {
					if toolData == nil {
						toolData = map[string]any{}
					}
					toolData["metrics_history"] = parsed
				}
			}
		} else {
			status, body, err := c.Gateway.Get("/metrics/latest?page=1&page_size=50&include_totals=false")
			step := models.Step{Tool: "metricsLatest", Status: status}
			if err != nil {
				step.Error = err.Error()
			} else {
				step.Body = clipString(strings.TrimSpace(string(body)), 2000)
			}
			steps = append(steps, step)
			if err == nil && status >= 200 && status < 300 {
				var parsed any
				if json.Unmarshal(body, &parsed) == nil {
					if toolData == nil {
						toolData = map[string]any{}
					}
					toolData["metrics_latest"] = parsed
				}
			}
		}
	}

	hostTokens := detectHostTokens(msg)
	wantsDeviceDetail := strings.Contains(msgLower, "device") || strings.Contains(msgLower, "kiosk") || strings.Contains(msgLower, "host") || strings.Contains(msgLower, "server")
	if wantsDeviceDetail && len(hostTokens) > 0 && c.Gateway != nil {
		const maxHosts = 3
		var hostHistories map[string]any
		for idx, host := range hostTokens {
			if idx >= maxHosts {
				break
			}
			if strings.TrimSpace(host) == "" {
				continue
			}
			path := "/metrics/history?page=1&page_size=50&include_totals=false&server_id=" + urlEscape(strings.ToLower(host))
			status, body, err := c.Gateway.Get(path)
			step := models.Step{Tool: "metricsHistory", Status: status}
			if err != nil {
				step.Error = err.Error()
			} else {
				step.Body = clipString(strings.TrimSpace(string(body)), 2000)
			}
			steps = append(steps, step)
			if err != nil || status < 200 || status >= 300 {
				continue
			}
			var parsed any
			if json.Unmarshal(body, &parsed) != nil {
				continue
			}
			if toolData == nil {
				toolData = map[string]any{}
			}
			if hostHistories == nil {
				hostHistories = map[string]any{}
				toolData["metrics_history_hosts"] = hostHistories
			}
			hostHistories[strings.ToLower(host)] = parsed
		}
	}

	// Add POP data prefetch
	// Note: some user requests (e.g. "show brt stats") don't mention "pop" but still map to /pop/stats.
	hasCityCode := c.detectCityCode(ctx, msgLower) != ""
	hasRegionCode := regionCodeForQuery != ""
	if strings.Contains(msgLower, "pop") ||
		(strings.Contains(msgLower, "device") && (strings.Contains(msgLower, "click") || strings.Contains(msgLower, "most") || strings.Contains(msgLower, "more"))) ||
		(hasCityCode && strings.Contains(msgLower, "stat")) ||
		(hasRegionCode && (strings.Contains(msgLower, "stat") || strings.Contains(msgLower, "top") || strings.Contains(msgLower, "data"))) ||
		isCityMostClicks {
		// First check for stats queries - top posters, devices, etc.
		var statsQueryPath string
		var groupBy string
		
		// Determine group_by parameter
		if strings.Contains(msgLower, "top poster") || strings.Contains(msgLower, "best poster") {
			groupBy = "poster"
		} else if strings.Contains(msgLower, "top device") || strings.Contains(msgLower, "best device") {
			groupBy = "device"
		} else if strings.Contains(msgLower, "top kiosk") || strings.Contains(msgLower, "best kiosk") {
			groupBy = "kiosk"
		} else if strings.Contains(msgLower, "device") && (strings.Contains(msgLower, "click") || strings.Contains(msgLower, "most") || strings.Contains(msgLower, "more")) {
			groupBy = "device"
		} else if isCityMostClicks {
			// We'll aggregate device click stats into city totals.
			groupBy = "device"
		} else if strings.Contains(msgLower, "stat") {
			// Default for generic "stats" queries.
			groupBy = "poster"
		}
		
		// If we found a valid group_by, proceed with building the query
		if groupBy != "" {
			limit := 10
			if isCityMostClicks {
				// Pull more rows so city aggregation is meaningful.
				limit = 200
			}
			statsQueryPath = "/pop/stats?group_by=" + groupBy + "&order=top&limit=" + fmt.Sprintf("%d", limit)
			
			// Determine metric
			if strings.Contains(msgLower, "click") {
				statsQueryPath += "&metric=clicks"
			} else if strings.Contains(msgLower, "play") {
				statsQueryPath += "&metric=plays"
			} else if strings.Contains(msgLower, "count") {
				statsQueryPath += "&metric=count"
			} else {
				statsQueryPath += "&metric=clicks" // Default to clicks
			}

			// Include an explicit date range when the user provides one.
			fromRFC, toRFC := extractDateRangeRFC3339(msgLower)
			if fromRFC != "" && toRFC != "" {
				statsQueryPath += "&from=" + urlEscape(fromRFC) + "&to=" + urlEscape(toRFC)
			}

			// City vs region targeting.
			useRegionForStats := false
			if regionCodeForQuery != "" {
				// If we couldn't detect a city code (i.e. "brt" is not a city), use region.
				// If the user explicitly says "region", also use region.
				if strings.Contains(msgLower, "region") || cityCodeForQuery == "" {
					useRegionForStats = true
				}
			}
			if useRegionForStats {
				statsQueryPath += "&region=" + regionCodeForQuery
			} else if cityCodeForQuery != "" {
				statsQueryPath += "&city=" + cityCodeForQuery
			}
			
			debugLogf("gateway GET %s", statsQueryPath)
			status, body, err := c.Gateway.Get(statsQueryPath)
			debugLogf("gateway GET %s -> status=%d err=%v", statsQueryPath, status, err)
			
			step := models.Step{Tool: "popStats", Status: status}
			if err != nil {
				step.Error = err.Error()
			} else {
				step.Body = clipString(strings.TrimSpace(string(body)), 2000)
			}
			steps = append(steps, step)
			if err == nil && status >= 200 && status < 300 {
				var parsed map[string]any
				if json.Unmarshal(body, &parsed) == nil {
					if toolData == nil {
						toolData = map[string]any{}
					}

					// For city stats queries, if poster-grouped stats are empty but device stats exist,
					// retry with group_by=device for the same city/metric.
					fbItems, fbHasItems := parsed["items"]
					fbEmpty := false
					if !fbHasItems || fbItems == nil {
						fbEmpty = true
					} else if slice, ok := fbItems.([]interface{}); ok {
						if len(slice) == 0 {
							fbEmpty = true
						}
					}
					if fbEmpty && cityCodeForQuery != "" && strings.Contains(msgLower, "stat") && groupBy == "poster" {
						fallbackPath := strings.Replace(statsQueryPath, "group_by=poster", "group_by=device", 1)
						debugLogf("gateway GET %s", fallbackPath)
						status2, body2, err2 := c.Gateway.Get(fallbackPath)
						debugLogf("gateway GET %s -> status=%d err=%v", fallbackPath, status2, err2)
						step2 := models.Step{Tool: "popStats", Status: status2}
						if err2 != nil {
							step2.Error = err2.Error()
						} else {
							step2.Body = clipString(strings.TrimSpace(string(body2)), 2000)
						}
						steps = append(steps, step2)
						if err2 == nil && status2 >= 200 && status2 < 300 {
							var parsed2 map[string]any
							if json.Unmarshal(body2, &parsed2) == nil {
								parsed = parsed2
							}
						}
					}

					// Region fallback: if stats are empty and we have a region code (e.g. brt), retry with region.
					itemsAny, hasItemsAny := parsed["items"]
					emptyAny := false
					if !hasItemsAny || itemsAny == nil {
						emptyAny = true
					} else if slice, ok := itemsAny.([]interface{}); ok {
						if len(slice) == 0 {
							emptyAny = true
						}
					}
					if emptyAny && !useRegionForStats && regionCodeForQuery != "" {
						regionPath := statsQueryPath
						if cityCodeForQuery != "" {
							regionPath = strings.Replace(regionPath, "&city="+cityCodeForQuery, "&region="+regionCodeForQuery, 1)
						} else {
							regionPath += "&region=" + regionCodeForQuery
						}
						debugLogf("gateway GET %s", regionPath)
						statusR, bodyR, errR := c.Gateway.Get(regionPath)
						debugLogf("gateway GET %s -> status=%d err=%v", regionPath, statusR, errR)
						stepR := models.Step{Tool: "popStats", Status: statusR}
						if errR != nil {
							stepR.Error = errR.Error()
						} else {
							stepR.Body = clipString(strings.TrimSpace(string(bodyR)), 2000)
						}
						steps = append(steps, stepR)
						if errR == nil && statusR >= 200 && statusR < 300 {
							var parsedR map[string]any
							if json.Unmarshal(bodyR, &parsedR) == nil {
								parsed = parsedR
								useRegionForStats = true
							}
						}
					}

					// If the user asked for city ranking by clicks, aggregate device stats into city totals.
					if isCityMostClicks {
						cityTotals := map[string]float64{}
						if rawItems, ok := parsed["items"].([]any); ok {
							for _, it := range rawItems {
								row, ok := it.(map[string]any)
								if !ok {
									continue
								}
								key, _ := row["Key"].(string)
								metric := 0.0
								switch v := row["Metric"].(type) {
								case float64:
									metric = v
								case int:
									metric = float64(v)
								}
								city := cityFromDeviceKey(key)
								if city != "" {
									cityTotals[city] += metric
								}
							}
						}

						// Prepare sorted output.
						type cityKV struct {
							City   string
							Clicks float64
						}
						out := make([]cityKV, 0, len(cityTotals))
						for k, v := range cityTotals {
							out = append(out, cityKV{City: k, Clicks: v})
						}
						sort.Slice(out, func(i, j int) bool { return out[i].Clicks > out[j].Clicks })
						// Convert to JSON-friendly shape.
						outJSON := make([]map[string]any, 0, len(out))
						for _, kv := range out {
							outJSON = append(outJSON, map[string]any{"city": kv.City, "clicks": kv.Clicks})
						}
						toolData["city_click_totals"] = outJSON
						if len(out) > 0 {
							toolData["city_click_winner"] = map[string]any{"city": out[0].City, "clicks": out[0].Clicks}
						}
					}
					
					items, hasItems := parsed["items"]
					emptyItems := false
					if !hasItems || items == nil {
						emptyItems = true
					} else if slice, ok := items.([]interface{}); ok {
						if len(slice) == 0 {
							emptyItems = true
						}
					}

					// Handle empty data case (no items key or items is nil)
					if emptyItems {
						// Store relevant info to be used later when modifying userContent
						scopeKey := ""
						if useRegionForStats {
							if region, ok := parsed["region"]; ok {
								regStr, _ := region.(string)
								if regStr != "" {
									scopeKey = regStr
								}
							}
							if scopeKey == "" {
								scopeKey = regionCodeForQuery
							}
						} else {
							if city, ok := parsed["city"]; ok {
								cityStr, _ := city.(string)
								if cityStr != "" {
									scopeKey = cityStr
								}
							}
							if scopeKey == "" {
								scopeKey = cityCodeForQuery
							}
						}
						if scopeKey == "" {
							scopeKey = c.detectCityCode(ctx, msgLower)
							if scopeKey == "" {
								scopeKey = c.detectRegionCode(ctx, msgLower)
							}
						}

						// Ensure toolData is initialized
						if toolData == nil {
							toolData = map[string]any{}
						}
						
						// Create a custom empty data marker
						emptyMessage := fmt.Sprintf("No statistical data found for %s. The database returned empty results, not an access error.", 
							scopeKey)
						
						// Store empty response info directly in toolData
						emptyData := map[string]any{
							"found": true,
							"endpoint": "pop_stats",
							"message": emptyMessage,
						}
						
						toolData["empty_data"] = emptyData
						
						// Create a user-friendly empty entry instead of null
						emptyNote := map[string]any{
							"note": "This is an empty data response, not an access restriction",
							"empty_data_notice": "true",
							"city": scopeKey,
							"message": "No statistics data found for the specified parameters. The data may not exist yet.",
							"items": []any{}, // Empty array instead of null
						}
						toolData["pop_stats"] = emptyNote
					} else {
						toolData["pop_stats"] = parsed
					}
				}
			}
		} else {
			// Regular POP data prefetch
			cityCode := cityCodeForQuery
			regionCode := regionCodeForQuery

			// If user didn't explicitly say "city" and we have a region match, prefer region.
			useRegion := false
			if regionCode != "" {
				if strings.Contains(msgLower, "region") || !strings.Contains(msgLower, "city") {
					useRegion = true
				}
			}
			
			// Construct the query path
			queryPath := "/pop"
			if useRegion {
				queryPath += "?region=" + regionCode + "&page=1&page_size=1"
			} else if cityCode != "" {
				queryPath += "?city=" + cityCode + "&page=1&page_size=1"
			} else if regionCode != "" {
				queryPath += "?region=" + regionCode + "&page=1&page_size=1"
			}
			
			debugLogf("gateway GET %s", queryPath)
			status, body, err := c.Gateway.Get(queryPath)
			debugLogf("gateway GET %s -> status=%d err=%v", queryPath, status, err)
			step := models.Step{Tool: "popData", Status: status}
			if useRegion && regionCode != "" {
				step.CampaignID = regionCode // Reuse this field for region/city code
			} else if cityCode != "" {
				step.CampaignID = cityCode // Reuse this field for region/city code
			} else if regionCode != "" {
				step.CampaignID = regionCode // Reuse this field for region/city code
			}
			if err != nil {
				step.Error = err.Error()
			} else {
				step.Body = clipString(strings.TrimSpace(string(body)), 2000)
			}
			steps = append(steps, step)
			if err == nil && status >= 200 && status < 300 {
				var parsed any
				if json.Unmarshal(body, &parsed) == nil {
					if toolData == nil {
						toolData = map[string]any{}
					}
					toolData["pop_data"] = parsed
				}
			}
		}
	}

	if data.CampaignImpressions == nil {
		data = nil
	}
	return data, steps, toolData
}

func (c *ChatService) buildHistory(ctx context.Context, ownerKey, conversationID string) []OpenAIMessage {
	if strings.TrimSpace(conversationID) == "" {
		return nil
	}
	msgs, err := c.Store.ListMessages(ctx, ownerKey, conversationID, 10)
	if err != nil {
		return nil
	}
	if len(msgs) > 10 {
		msgs = msgs[len(msgs)-10:]
	}
	out := make([]OpenAIMessage, 0, len(msgs))
	for _, m := range msgs {
		if m.Role != "user" && m.Role != "assistant" {
			continue
		}
		out = append(out, OpenAIMessage{Role: m.Role, Content: clipString(m.Content, 1000)})
	}
	return out
}

func (c *ChatService) Chat(ctx context.Context, ownerKey string, req models.ChatRequest) (models.ChatResponse, error) {
	if resp, handled, err := c.handleTopPostersFromCity(ctx, req, nil); handled {
		return resp, err
	}
	if resp, handled, err := c.handleTopDevicesFromCity(ctx, req, nil); handled {
		return resp, err
	}
	if resp, handled, err := c.handleDeviceTelemetry(ctx, req, nil); handled {
		return resp, err
	}
	if resp, handled, err := c.handleKioskCountFromCity(ctx, req, nil); handled {
		return resp, err
	}
	if resp, handled, err := c.handleCreativeUpload(ctx, ownerKey, req); handled {
		return resp, err
	}
	if resp, handled, err := c.handlePosterDetails(ctx, req, nil); handled {
		return resp, err
	}

	conversationID := strings.TrimSpace(req.ConversationID)
	if conversationID != "" {
		_ = c.Store.AppendMessage(ctx, ownerKey, conversationID, "user", req.Message)
	}
	
	// Check for specific stat requests that need special handling
	var isStatsRequest bool
	var cityCode string
	msgLower := strings.ToLower(req.Message)
	if strings.Contains(msgLower, "stats") || strings.Contains(msgLower, "top") {
		cityCode = c.detectCityCode(ctx, msgLower)
		if cityCode != "" {
			isStatsRequest = true
		}
		
		if strings.Contains(msgLower, "top") {
			isStatsRequest = true
		}
	}
	
	data, steps, toolData := c.prefetchImpressions(ctx, req.Message)

	if c.MockMode {
		answer := "(mock) I am running without OpenAI."
		if toolData != nil {
			b, _ := json.MarshalIndent(toolData, "", "  ")
			answer = answer + "\n\nFetched data:\n" + string(b)
		} else {
			answer = answer + "\n\nTip: include a campaign id and ask for impressions to see real data."
		}
		if conversationID != "" {
			_ = c.Store.AppendMessage(ctx, ownerKey, conversationID, "assistant", answer)
		}
		return models.ChatResponse{Answer: answer, Data: data, Steps: steps}, nil
	}

	system := `You are SmartCity Media dashboard assistant. Answer concisely and ALWAYS call the scm_request tool when retrieving data.

You can access SCM Tool Gateway endpoints (OpenAPI): /ads/* (advertisers, campaigns, creatives, devices, venues, projects), /pop (list/search/stats/trend/impressions), /metrics (latest/history), and /context.
Use pagination by default for list endpoints (page=1, page_size=20 unless user asks for more). For POP stats use limit=10 by default.

Endpoint mapping for data requests:

1. DASHBOARD DATA
- Advertisers: GET /ads/advertisers
- Campaigns: GET /ads/campaigns
  With filter: GET /ads/campaigns?advertiser_id=<id>
- Creatives: GET /ads/creatives
  For campaign: GET /ads/creatives/campaign/<campaign_id>
- Projects: GET /ads/projects
  Specific: GET /ads/projects/{name}

2. POP DATA
- POP list: GET /pop (supports multiple query parameters)
  For city: GET /pop?city=<city_code>&page=1&page_size=1
  Example: GET /pop?city=brt&page=1&page_size=1
- POP search: GET /pop/search?q=<search_term>
- POP statistics: 
  * City stats: GET /pop/stats?group_by=poster&city=brt&metric=clicks&limit=10
  * Top posters by clicks: GET /pop/stats?group_by=poster&metric=clicks&order=top&limit=10
  * Top devices by plays: GET /pop/stats?group_by=device&metric=plays&order=top&limit=10
  * Top kiosks by count: GET /pop/stats?group_by=kiosk&metric=count&order=top&limit=10
  * Bottom performers: Use order=bottom instead of top
  * Filter by city: Add &city=<city_code>
- POP trends: GET /pop/trend?dimension=<poster|device|city>&key=<value>&metric=<plays|clicks|count>

3. DEVICES & VENUES
- Devices count by region: GET /ads/devices/counts/regions
  For specific city: GET /ads/devices/counts/regions?city=<city_code>
- Device list: GET /ads/devices (supports pagination, filters)
  By host: GET /ads/devices/{hostName}
- Venues: GET /ads/venues
  By ID: GET /ads/venues/{id}
  Venue devices: GET /ads/venues/{id}/devices

4. CREATIVE UPLOADS
- Upload by URL: POST /ads/creatives/uploadByUrl
  Body: {"campaign_id":"...","selected_days":"...","time_slots":"...","file_url":"..."}
- Upload multiple: POST /ads/creatives/uploadByUrls
  Body: {"campaign_id":"...","selected_days":"...","time_slots":"...","file_urls":["..."]}

5. METRICS & SERVERS
- Server inventory: GET /metrics/servers
- Server status overall: GET /metrics/servers/status
- Server status by city: GET /metrics/servers/status/city?city=<optional>
- Latest metrics snapshot: GET /metrics/latest
- Metrics history: GET /metrics/history

6. CONTEXT MEMORY
- Get: GET /context/{key}
- Set: PUT /context/{key} with body {"value": any}

ALWAYS use these exact paths - do not guess or make up paths.
Poster IDs returned from POP stats (e.g., values starting with "vistar_") are NOT campaign IDs; look them up via GET /ads/creatives/search?query=<poster_id>.
Always use city_code (like "kcmo") or region (like "brt") in query params when asking about specific areas.`
	userContent := req.Message
	if toolData != nil {
		b, _ := json.Marshal(toolData)
		userContent = userContent + "\n\nContext JSON (from internal APIs):\n" + clipString(string(b), 8000)
	}

	all := make([]OpenAIMessage, 0)
	all = append(all, OpenAIMessage{Role: "system", Content: system})
	all = append(all, c.buildHistory(ctx, ownerKey, conversationID)...)
	all = append(all, OpenAIMessage{Role: "user", Content: userContent})

	if c.MaxToolCalls <= 0 {
		c.MaxToolCalls = 6
	}
	if c.MaxToolBytes <= 0 {
		c.MaxToolBytes = 1_000_000
	}

	tools := []OpenAITool{
		{
			Type: "function",
			Function: OpenAIToolFunction{
				Name:        "scm_request",
				Description: "Call an SCM Tool Gateway OpenAPI endpoint by method and path.",
				Parameters: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"method": map[string]any{"type": "string"},
						"path": map[string]any{"type": "string"},
						"query": map[string]any{"type": "object", "additionalProperties": map[string]any{"type": "string"}},
						"body": map[string]any{"type": "object"},
						"multipart": map[string]any{
							"type": "object",
							"properties": map[string]any{
								"fields": map[string]any{
									"type": "object",
									"additionalProperties": map[string]any{
										"type": "array",
										"items": map[string]any{"type": "string"},
									},
								},
								"files": map[string]any{
									"type": "array",
									"items": map[string]any{
										"type": "object",
										"properties": map[string]any{
											"field_name": map[string]any{"type": "string"},
											"file_name": map[string]any{"type": "string"},
											"content_type": map[string]any{"type": "string"},
											"base64": map[string]any{"type": "string"},
										},
										"required": []any{"base64"},
									},
								},
							},
						},
					},
					"required": []any{"method", "path"},
				},
			},
		},
	}

	// Always force tool usage to ensure consistent behavior like ChatGPT does
	toolChoice := "required"
	// First get the answer from the model
	answer, err := c.chatWithToolLoop(ctx, all, tools, toolChoice)
	if err != nil {
		return models.ChatResponse{}, err
	}
	
	// Check if we got empty data and should override the answer
	if isStatsRequest {
		// Check if we have empty data in the toolData
		if toolData != nil {
			if emptyData, ok := toolData["empty_data"].(map[string]any); ok && emptyData["found"] == true {
				// Override the answer
				emptyMsg, _ := emptyData["message"].(string)
				if emptyMsg != "" {
					if cityCode != "" {
						answer = fmt.Sprintf("There are currently no statistics available for city '%s'. "+
							"This means the database returned an empty result set, not that you lack permission to access this data.", cityCode)
					} else {
						answer = "The statistics you requested returned no data. This means the database returned an empty result set, not that you lack permission to access this data."
					}
				}
			}
		}
	}
	
	if conversationID != "" {
		_ = c.Store.AppendMessage(ctx, ownerKey, conversationID, "assistant", answer)
	}

	return models.ChatResponse{Answer: answer, Data: data, Steps: steps}, nil
}

func (c *ChatService) ChatStream(ctx context.Context, ownerKey string, req models.ChatRequest, onToken func(string)) (models.ChatResponse, error) {
	if resp, handled, err := c.handleTopPostersFromCity(ctx, req, onToken); handled {
		return resp, err
	}
	if resp, handled, err := c.handleTopDevicesFromCity(ctx, req, onToken); handled {
		return resp, err
	}
	if resp, handled, err := c.handleDeviceTelemetry(ctx, req, onToken); handled {
		return resp, err
	}
	if resp, handled, err := c.handleKioskCountFromCity(ctx, req, onToken); handled {
		return resp, err
	}
	if resp, handled, err := c.handleCreativeUpload(ctx, ownerKey, req); handled {
		return resp, err
	}
	if resp, handled, err := c.handlePosterDetails(ctx, req, onToken); handled {
		return resp, err
	}

	conversationID := strings.TrimSpace(req.ConversationID)
	if conversationID != "" {
		_ = c.Store.AppendMessage(ctx, ownerKey, conversationID, "user", req.Message)
	}

	data, steps, toolData := c.prefetchImpressions(ctx, req.Message)

	if c.MockMode {
		mockText := "(mock) I am running without OpenAI."
		if toolData != nil {
			mockText += " I fetched impressions data."
		}
		for i := 0; i < len(mockText); i += 20 {
			end := i + 20
			if end > len(mockText) {
				end = len(mockText)
			}
			if onToken != nil {
				onToken(mockText[i:end])
			}
		}
		if conversationID != "" {
			_ = c.Store.AppendMessage(ctx, ownerKey, conversationID, "assistant", mockText)
		}
		return models.ChatResponse{Answer: mockText, Data: data, Steps: steps}, nil
	}

	system := `You are SmartCity Media dashboard assistant. Answer concisely and ALWAYS call the scm_request tool when retrieving data.

You can access SCM Tool Gateway endpoints (OpenAPI): /ads/* (advertisers, campaigns, creatives, devices, venues, projects), /pop (list/search/stats/trend/impressions), /metrics (latest/history), and /context.
Use pagination by default for list endpoints (page=1, page_size=20 unless user asks for more). For POP stats use limit=10 by default.

Endpoint mapping for data requests:

1. DASHBOARD DATA
- Advertisers: GET /ads/advertisers
- Campaigns: GET /ads/campaigns
  With filter: GET /ads/campaigns?advertiser_id=<id>
- Creatives: GET /ads/creatives
  For campaign: GET /ads/creatives/campaign/<campaign_id>
- Projects: GET /ads/projects
  Specific: GET /ads/projects/{name}

2. POP DATA
- POP list: GET /pop (supports multiple query parameters)
  For city: GET /pop?city=<city_code>&page=1&page_size=1
  Example: GET /pop?city=brt&page=1&page_size=1
- POP search: GET /pop/search?q=<search_term>
- POP statistics: 
  * City stats: GET /pop/stats?group_by=poster&city=brt&metric=clicks&limit=10
  * Top posters by clicks: GET /pop/stats?group_by=poster&metric=clicks&order=top&limit=10
  * Top devices by plays: GET /pop/stats?group_by=device&metric=plays&order=top&limit=10
  * Top kiosks by count: GET /pop/stats?group_by=kiosk&metric=count&order=top&limit=10
  * Bottom performers: Use order=bottom instead of top
- POP trends: GET /pop/trend?dimension=<poster|device|city>&key=<value>&metric=<plays|clicks|count>

3. DEVICES & VENUES
- Devices count by region: GET /ads/devices/counts/regions
  For specific city: GET /ads/devices/counts/regions?city=<city_code>
- Device list: GET /ads/devices (supports pagination, filters)
  By host: GET /ads/devices/{hostName}
- Venues: GET /ads/venues
  By ID: GET /ads/venues/{id}
  Venue devices: GET /ads/venues/{id}/devices

4. CREATIVE UPLOADS
- Upload by URL: POST /ads/creatives/uploadByUrl
  Body: {"campaign_id":"...","selected_days":"...","time_slots":"...","file_url":"..."}
- Upload multiple: POST /ads/creatives/uploadByUrls
  Body: {"campaign_id":"...","selected_days":"...","time_slots":"...","file_urls":["..."]}

5. CONTEXT MEMORY
- Get: GET /context/{key}
- Set: PUT /context/{key} with body {"value": any}

ALWAYS use these exact paths - do not guess or make up paths.
Poster IDs returned from POP stats (e.g., values starting with "vistar_") are NOT campaign IDs; look them up via GET /ads/creatives/search?query=<poster_id>.
For ANY stats or metrics request, use the appropriate /pop/stats endpoint with the correct parameters.
For area-specific queries, ALWAYS include either city=<code> or region=<code> in the query parameters.

IMPORTANT: When receiving empty data from the API (where items is null or empty), do NOT report this as an access restriction or authorization issue. Instead, clearly state that no data was found for the query parameters. For example: "There are currently no statistics available for [city/metric] based on the available data."`
	userContent := req.Message
	// Check if we have empty data to emphasize for the model
	if toolData != nil {
		// Check for empty data to add notice
		if emptyData, ok := toolData["empty_data"].(map[string]any); ok && emptyData["found"] == true {
			// Add explicit instructions to the user message
			emptyMsg, _ := emptyData["message"].(string)
			userContent = "IMPORTANT DATA NOTICE: " + emptyMsg + "\n\n" + userContent
		}
		
		// Add the context JSON with all tool data
		b, _ := json.Marshal(toolData)
		userContent = userContent + "\n\nContext JSON (from internal APIs):\n" + clipString(string(b), 8000)
	}

	all := make([]OpenAIMessage, 0)
	all = append(all, OpenAIMessage{Role: "system", Content: system})
	all = append(all, c.buildHistory(ctx, ownerKey, conversationID)...)
	all = append(all, OpenAIMessage{Role: "user", Content: userContent})

	if c.MaxToolCalls <= 0 {
		c.MaxToolCalls = 6
	}
	if c.MaxToolBytes <= 0 {
		c.MaxToolBytes = 1_000_000
	}

	tools := []OpenAITool{
		{
			Type: "function",
			Function: OpenAIToolFunction{
				Name:        "scm_request",
				Description: "Call an SCM Tool Gateway OpenAPI endpoint by method and path.",
				Parameters: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"method": map[string]any{"type": "string"},
						"path": map[string]any{"type": "string"},
						"query": map[string]any{"type": "object", "additionalProperties": map[string]any{"type": "string"}},
						"body": map[string]any{"type": "object"},
						"multipart": map[string]any{
							"type": "object",
							"properties": map[string]any{
								"fields": map[string]any{
									"type": "object",
									"additionalProperties": map[string]any{
										"type": "array",
										"items": map[string]any{"type": "string"},
									},
								},
								"files": map[string]any{
									"type": "array",
									"items": map[string]any{
										"type": "object",
										"properties": map[string]any{
											"field_name": map[string]any{"type": "string"},
											"file_name": map[string]any{"type": "string"},
											"content_type": map[string]any{"type": "string"},
											"base64": map[string]any{"type": "string"},
										},
										"required": []any{"base64"},
									},
								},
							},
						},
					},
					"required": []any{"method", "path"},
				},
			},
		},
	}

	// Always force tool usage to ensure consistent behavior like ChatGPT does
	toolChoice := "required"
	full, err := c.chatWithToolLoop(ctx, all, tools, toolChoice)
	if err != nil {
		return models.ChatResponse{}, err
	}
	for i := 0; i < len(full); i += 20 {
		end := i + 20
		if end > len(full) {
			end = len(full)
		}
		if onToken != nil {
			onToken(full[i:end])
		}
	}
	if conversationID != "" {
		_ = c.Store.AppendMessage(ctx, ownerKey, conversationID, "assistant", full)
	}

	return models.ChatResponse{Answer: full, Data: data, Steps: steps}, nil
}
