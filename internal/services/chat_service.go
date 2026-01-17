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
	"unicode"

	"openai-agent-service/internal/models"
)

func debugEnabled() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("GO_LOG")))
	return v == "debug" || v == "1" || v == "true"
}

func (c *ChatService) handlePosterAnalyticsByID(ctx context.Context, req models.ChatRequest, onToken func(string)) (models.ChatResponse, bool, error) {
	msgLower := strings.ToLower(req.Message)
	if !strings.Contains(msgLower, "analytics") {
		return models.ChatResponse{}, false, nil
	}
	if !strings.Contains(msgLower, "poster") {
		return models.ChatResponse{}, false, nil
	}
	posterID := extractCampaignID(req.Message)
	if !looksLikeUUID(posterID) {
		return models.ChatResponse{}, false, nil
	}
	if c.Gateway == nil {
		return models.ChatResponse{Answer: "Tool gateway is not configured."}, true, nil
	}

	conversationID := strings.TrimSpace(req.ConversationID)
	city := c.detectCityCode(ctx, msgLower)
	region := c.detectRegionCode(ctx, msgLower)
	if city == "" && region == "" && conversationID != "" {
		if st := c.getConversationState(conversationID); st != nil {
			if strings.TrimSpace(st.PosterRegion) != "" {
				region = strings.ToLower(strings.TrimSpace(st.PosterRegion))
			}
			if strings.TrimSpace(st.PosterCity) != "" {
				city = strings.ToLower(strings.TrimSpace(st.PosterCity))
			}
			if region == "" && strings.TrimSpace(st.Region) != "" {
				region = strings.ToLower(strings.TrimSpace(st.Region))
			}
			if city == "" && strings.TrimSpace(st.City) != "" {
				city = strings.ToLower(strings.TrimSpace(st.City))
			}
		}
	}

	type popItem struct {
		PosterName string    `json:"poster_name"`
		PosterID   string    `json:"poster_id"`
		HostName   string    `json:"host_name"`
		KioskName  string    `json:"kiosk_name"`
		PopTime    time.Time `json:"pop_datetime"`
		City       string    `json:"city"`
		Region     string    `json:"region"`
		PlayCount  int64     `json:"play_count"`
	}
	type popListResponse struct {
		Items    []popItem `json:"items"`
		Total    int64     `json:"total"`
		Page     int       `json:"page"`
		PageSize int       `json:"page_size"`
	}

	page := 1
	pageSize := 200
	maxPages := 10
	steps := make([]models.Step, 0, 2)
	items := make([]popItem, 0, 64)
	for {
		path := fmt.Sprintf("/pop?poster_id=%s&page=%d&page_size=%d", urlEscape(posterID), page, pageSize)
		if strings.TrimSpace(region) != "" {
			path += "&region=" + urlEscape(region)
		} else if strings.TrimSpace(city) != "" {
			path += "&city=" + urlEscape(city)
		}
		status, body, err := c.Gateway.Get(path)
		step := models.Step{Tool: "popList", Status: status}
		if err != nil {
			step.Error = err.Error()
		} else {
			step.Body = clipString(strings.TrimSpace(string(body)), 2000)
		}
		steps = append(steps, step)
		if err != nil {
			return models.ChatResponse{Answer: "Failed to fetch POP data: " + err.Error(), Steps: steps}, true, nil
		}
		if status < 200 || status >= 300 {
			return models.ChatResponse{Answer: fmt.Sprintf("Failed to fetch POP data (status %d).", status), Steps: steps}, true, nil
		}
		var resp popListResponse
		if json.Unmarshal(body, &resp) != nil {
			return models.ChatResponse{Answer: "POP list response could not be parsed.", Steps: steps}, true, nil
		}
		if len(resp.Items) == 0 {
			break
		}
		items = append(items, resp.Items...)
		if resp.Total > 0 {
			if int64(page*pageSize) >= resp.Total {
				break
			}
		} else if len(resp.Items) < pageSize {
			break
		}
		page++
		if page > maxPages {
			break
		}
	}
	if len(items) == 0 {
		return models.ChatResponse{Answer: fmt.Sprintf("No POP rows found for poster %s.", posterID), Steps: steps}, true, nil
	}

	posterName := strings.TrimSpace(items[0].PosterName)
	label := posterID
	if posterName != "" {
		label = posterName + " (" + posterID + ")"
	}
	scopeLabel := ""
	if strings.TrimSpace(region) != "" {
		scopeLabel = "region '" + strings.TrimSpace(region) + "'"
	} else if strings.TrimSpace(city) != "" {
		scopeLabel = "city '" + strings.TrimSpace(city) + "'"
	}

	totalPlays := int64(0)
	byKiosk := map[string]int64{}
	for _, it := range items {
		totalPlays += it.PlayCount
		k := strings.TrimSpace(it.KioskName)
		if k == "" {
			k = strings.TrimSpace(it.HostName)
		}
		if k == "" {
			continue
		}
		byKiosk[k] += it.PlayCount
	}

	type kv struct {
		Key   string
		Plays int64
	}
	rows := make([]kv, 0, len(byKiosk))
	for k, v := range byKiosk {
		rows = append(rows, kv{Key: k, Plays: v})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Plays > rows[j].Plays })
	if len(rows) > 10 {
		rows = rows[:10]
	}
	lines := make([]string, 0, len(rows)+3)
	if scopeLabel != "" {
		lines = append(lines, fmt.Sprintf("Analytics for poster %s in %s: %d plays", label, scopeLabel, totalPlays))
	} else {
		lines = append(lines, fmt.Sprintf("Analytics for poster %s: %d plays", label, totalPlays))
	}
	lines = append(lines, fmt.Sprintf("Kiosks matched: %d", len(byKiosk)))
	lines = append(lines, "Top kiosks:")
	for i, r := range rows {
		lines = append(lines, fmt.Sprintf("%d. %s — %d plays", i+1, r.Key, r.Plays))
	}
	answer := strings.Join(lines, "\n")
	if onToken != nil {
		onToken(answer)
	}
	if conversationID != "" {
		c.updateConversationPoster(conversationID, posterName, city, region)
		c.updateConversationPosterID(conversationID, posterID)
		c.clearPending(conversationID)
	}
	return models.ChatResponse{Answer: answer, Steps: steps}, true, nil
}

func (c *ChatService) handleCampaignCreatives(ctx context.Context, req models.ChatRequest, onToken func(string)) (models.ChatResponse, bool, error) {
	msg := strings.TrimSpace(req.Message)
	msgLower := strings.ToLower(msg)
	if msg == "" {
		return models.ChatResponse{}, false, nil
	}
	if !(strings.Contains(msgLower, "creative") || strings.Contains(msgLower, "creatives")) {
		return models.ChatResponse{}, false, nil
	}
	if !(strings.Contains(msgLower, "campaign") || strings.Contains(msgLower, "campaigns")) {
		return models.ChatResponse{}, false, nil
	}
	if strings.Contains(msgLower, "upload") {
		return models.ChatResponse{}, false, nil
	}
	if !(strings.Contains(msgLower, "show") || strings.Contains(msgLower, "list") || strings.Contains(msgLower, "get")) {
		return models.ChatResponse{}, false, nil
	}
	if c.Gateway == nil {
		return models.ChatResponse{Answer: "Tool gateway is not configured."}, true, nil
	}

	conversationID := strings.TrimSpace(req.ConversationID)

	campaignID := ""
	if strings.Contains(msgLower, "campaign") {
		campaignID = extractCampaignID(msg)
	}

	// Parse a campaign name from phrases like:
	// "show Bet 365 campaign creatives"
	// "list creatives for Bet 365 campaign"
	campaignName := ""
	if campaignID == "" {
		if strings.Contains(msgLower, "campaign") {
			beforeCampaign := strings.TrimSpace(strings.SplitN(msg, "campaign", 2)[0])
			beforeCampaignLower := strings.ToLower(beforeCampaign)
			for _, w := range []string{"show", "list", "get", "me", "the", "all", "for", "of", "creatives", "creative"} {
				beforeCampaignLower = strings.ReplaceAll(beforeCampaignLower, w, " ")
			}
			campaignName = strings.TrimSpace(strings.Join(strings.Fields(beforeCampaignLower), " "))
		}
		if campaignName == "" {
			// Fallback: try to extract text between "for" and "campaign".
			if strings.Contains(msgLower, " for ") && strings.Contains(msgLower, "campaign") {
				parts := strings.SplitN(msgLower, " for ", 2)
				if len(parts) == 2 {
					candidate := strings.TrimSpace(strings.SplitN(parts[1], "campaign", 2)[0])
					campaignName = strings.TrimSpace(candidate)
				}
			}
		}
	}

	steps := make([]models.Step, 0, 2)
	if campaignID == "" {
		if strings.TrimSpace(campaignName) == "" {
			return models.ChatResponse{Answer: "Please specify a campaign name (for example: show Bet 365 campaign creatives) or provide a campaign id."}, true, nil
		}
		status, body, err := c.Gateway.Get("/ads/campaigns/search?query=" + urlEscape(campaignName) + "&page=1&page_size=20")
		step := models.Step{Tool: "adsCampaignsSearch", Status: status}
		if err != nil {
			step.Error = err.Error()
		} else {
			step.Body = clipString(strings.TrimSpace(string(body)), 2000)
		}
		steps = append(steps, step)
		if err != nil {
			return models.ChatResponse{Answer: "Failed to search campaigns: " + err.Error(), Steps: steps}, true, nil
		}
		if status < 200 || status >= 300 {
			return models.ChatResponse{Answer: fmt.Sprintf("Failed to search campaigns (status %d).", status), Steps: steps}, true, nil
		}
		var parsed map[string]any
		if json.Unmarshal(body, &parsed) != nil {
			return models.ChatResponse{Answer: "Campaign search response could not be parsed.", Steps: steps}, true, nil
		}
		rows := extractCampaignRows(parsed)
		bestID := ""
		bestName := ""
		q := strings.ToLower(strings.TrimSpace(campaignName))
		for _, it := range rows {
			m, ok := it.(map[string]any)
			if !ok {
				continue
			}
			id, _ := m["id"].(string)
			name, _ := m["name"].(string)
			id = strings.TrimSpace(id)
			name = strings.TrimSpace(name)
			if id == "" || name == "" {
				continue
			}
			nameLower := strings.ToLower(name)
			if nameLower == q || strings.Contains(nameLower, q) {
				bestID = id
				bestName = name
				break
			}
		}
		if bestID == "" {
			// Fall back to first row.
			if len(rows) > 0 {
				if m, ok := rows[0].(map[string]any); ok {
					bestID, _ = m["id"].(string)
					bestName, _ = m["name"].(string)
					bestID = strings.TrimSpace(bestID)
					bestName = strings.TrimSpace(bestName)
				}
			}
		}
		if bestID == "" {
			return models.ChatResponse{Answer: fmt.Sprintf("No campaigns found matching '%s'.", campaignName), Steps: steps}, true, nil
		}
		campaignID = bestID
		if conversationID != "" {
			c.updateConversationCampaignID(conversationID, campaignID)
		}
		_ = bestName
	}

	path := "/ads/creatives/campaign/" + urlEscape(campaignID) + "?page=1&page_size=200"
	statusC, bodyC, errC := c.Gateway.Get(path)
	stepC := models.Step{Tool: "adsCreativesByCampaign", Status: statusC}
	if errC != nil {
		stepC.Error = errC.Error()
	} else {
		stepC.Body = clipString(strings.TrimSpace(string(bodyC)), 2000)
	}
	steps = append(steps, stepC)
	if errC != nil {
		return models.ChatResponse{Answer: "Failed to fetch campaign creatives: " + errC.Error(), Steps: steps}, true, nil
	}
	if statusC < 200 || statusC >= 300 {
		return models.ChatResponse{Answer: fmt.Sprintf("Failed to fetch campaign creatives (status %d).", statusC), Steps: steps}, true, nil
	}
	rows := parseRows(bodyC)
	if len(rows) == 0 {
		return models.ChatResponse{Answer: fmt.Sprintf("No creatives found for campaign %s.", campaignID), Steps: steps}, true, nil
	}

	lines := make([]string, 0, 12)
	lines = append(lines, fmt.Sprintf("Creatives for campaign %s:", campaignID))
	limit := 10
	for _, it := range rows {
		if len(lines)-1 >= limit {
			break
		}
		m, ok := it.(map[string]any)
		if !ok {
			continue
		}
		id := ""
		switch v := m["id"].(type) {
		case string:
			id = v
		case float64:
			id = strconv.Itoa(int(v))
		}
		name, _ := m["name"].(string)
		typeStr, _ := m["type"].(string)
		fileURL, _ := m["file_url"].(string)
		if strings.TrimSpace(fileURL) == "" {
			fileURL, _ = m["fileUrl"].(string)
		}
		name = strings.TrimSpace(name)
		id = strings.TrimSpace(id)
		typeStr = strings.TrimSpace(typeStr)
		if name == "" && id == "" {
			continue
		}
		label := name
		if label == "" {
			label = id
		}
		if typeStr != "" {
			label += " — " + typeStr
		}
		if strings.TrimSpace(fileURL) != "" {
			label += " — " + strings.TrimSpace(fileURL)
		}
		lines = append(lines, fmt.Sprintf("%d. %s", len(lines), label))
	}
	answer := strings.Join(lines, "\n")
	if onToken != nil {
		onToken(answer)
	}
	return models.ChatResponse{Answer: answer, Steps: steps}, true, nil
}

func (c *ChatService) handleKioskPosterPlayCount(ctx context.Context, req models.ChatRequest, onToken func(string)) (models.ChatResponse, bool, error) {
	msgLower := strings.ToLower(req.Message)
	if !(strings.Contains(msgLower, "played") || strings.Contains(msgLower, "play")) {
		return models.ChatResponse{}, false, nil
	}
	if !strings.Contains(msgLower, "poster") {
		return models.ChatResponse{}, false, nil
	}
	if !(strings.Contains(msgLower, "kiosk") || strings.Contains(msgLower, "device")) {
		return models.ChatResponse{}, false, nil
	}
	if c.Gateway == nil {
		return models.ChatResponse{Answer: "Tool gateway is not configured."}, true, nil
	}

	type popItem struct {
		PosterName string `json:"poster_name"`
		PosterID   string `json:"poster_id"`
		HostName   string `json:"host_name"`
		KioskName  string `json:"kiosk_name"`
		PlayCount  int64  `json:"play_count"`
	}
	type popListResponse struct {
		Items    []popItem `json:"items"`
		Total    int64     `json:"total"`
		Page     int       `json:"page"`
		PageSize int       `json:"page_size"`
	}

	conversationID := strings.TrimSpace(req.ConversationID)
	low := strings.ToLower(req.Message)
	playedIdx := strings.Index(low, " has played ")
	if playedIdx < 0 {
		playedIdx = strings.Index(low, " played ")
	}
	if playedIdx < 0 {
		return models.ChatResponse{}, false, nil
	}
	kioskName := strings.TrimSpace(req.Message[:playedIdx])
	// Strip common leading question prefixes so we don't feed "How much ..." into device search.
	{
		knLower := strings.ToLower(strings.TrimSpace(kioskName))
		prefixes := []string{"how much ", "how many ", "what is ", "what's ", "whats ", "tell me ", "show me ", "give me "}
		for _, p := range prefixes {
			if strings.HasPrefix(knLower, p) {
				kioskName = strings.TrimSpace(kioskName[len(p):])
				knLower = strings.ToLower(strings.TrimSpace(kioskName))
				break
			}
		}
	}
	if kioskName == "" {
		return models.ChatResponse{}, false, nil
	}

	rest := strings.TrimSpace(req.Message[playedIdx:])
	restLow := strings.ToLower(rest)
	hasPlayedPrefix := " has played "
	if j := strings.Index(restLow, hasPlayedPrefix); j >= 0 {
		rest = strings.TrimSpace(rest[j+len(hasPlayedPrefix):])
	} else if j := strings.Index(restLow, " played "); j >= 0 {
		rest = strings.TrimSpace(rest[j+len(" played "):])
	}
	posterEndIdx := strings.Index(strings.ToLower(rest), " poster")
	posterName := rest
	if posterEndIdx >= 0 {
		posterName = strings.TrimSpace(rest[:posterEndIdx])
	}
	posterName = strings.TrimSpace(strings.Trim(posterName, "\"' "))
	if posterName == "" {
		if conversationID != "" {
			if st := c.getConversationState(conversationID); st != nil {
				if strings.TrimSpace(st.PosterName) != "" {
					posterName = strings.TrimSpace(st.PosterName)
				}
			}
		}
	}
	if posterName == "" {
		return models.ChatResponse{Answer: "Please specify a poster name."}, true, nil
	}

	resolvedHost, resolveStep := c.resolveHostFromDeviceName(ctx, conversationID, kioskName)
	resolvedHost = strings.ToLower(strings.TrimSpace(resolvedHost))
	if resolvedHost == "" {
		// Fallback: try direct POP filtering by kiosk name first; then query by poster_name + scope and filter by kiosk_name.
		steps := make([]models.Step, 0, 2)
		if resolveStep != nil {
			steps = append(steps, *resolveStep)
		}
		city := ""
		region := ""
		if conversationID != "" {
			if st := c.getConversationState(conversationID); st != nil {
				if strings.TrimSpace(st.PosterRegion) != "" {
					region = strings.ToLower(strings.TrimSpace(st.PosterRegion))
				}
				if strings.TrimSpace(st.PosterCity) != "" {
					city = strings.ToLower(strings.TrimSpace(st.PosterCity))
				}
				if region == "" && strings.TrimSpace(st.Region) != "" {
					region = strings.ToLower(strings.TrimSpace(st.Region))
				}
				if city == "" && strings.TrimSpace(st.City) != "" {
					city = strings.ToLower(strings.TrimSpace(st.City))
				}
			}
		}

		page := 1
		pageSize := 200
		maxPages := 10
		matchedPlays := int64(0)
		matched := 0
		posterIDFound := ""
		kioskLower := strings.ToLower(strings.TrimSpace(kioskName))

		// Attempt server-side filtering when supported.
		{
			path := fmt.Sprintf("/pop?poster_name=%s&kiosk_name=%s&page=1&page_size=%d", urlEscape(posterName), urlEscape(kioskName), pageSize)
			if strings.TrimSpace(region) != "" {
				path += "&region=" + urlEscape(region)
			} else if strings.TrimSpace(city) != "" {
				path += "&city=" + urlEscape(city)
			}
			status, body, err := c.Gateway.Get(path)
			step := models.Step{Tool: "popList", Status: status}
			if err != nil {
				step.Error = err.Error()
			} else {
				step.Body = clipString(strings.TrimSpace(string(body)), 2000)
			}
			steps = append(steps, step)
			if err == nil && status == 400 {
				path2 := fmt.Sprintf("/pop?poster_name=%s&kiosk=%s&page=1&page_size=%d", urlEscape(posterName), urlEscape(kioskName), pageSize)
				if strings.TrimSpace(region) != "" {
					path2 += "&region=" + urlEscape(region)
				} else if strings.TrimSpace(city) != "" {
					path2 += "&city=" + urlEscape(city)
				}
				status2, body2, err2 := c.Gateway.Get(path2)
				step2 := models.Step{Tool: "popList", Status: status2}
				if err2 != nil {
					step2.Error = err2.Error()
				} else {
					step2.Body = clipString(strings.TrimSpace(string(body2)), 2000)
				}
				steps = append(steps, step2)
				status, body, err = status2, body2, err2
			}
			if err == nil && status >= 200 && status < 300 {
				var resp popListResponse
				if json.Unmarshal(body, &resp) == nil {
					if len(resp.Items) > 0 {
						total := int64(0)
						for _, it := range resp.Items {
							// Still double-check kiosk match just in case the server-side filter is fuzzy.
							kn := strings.ToLower(strings.TrimSpace(it.KioskName))
							if kn == "" {
								continue
							}
							if kn == kioskLower || strings.Contains(kn, kioskLower) || strings.Contains(kioskLower, kn) {
								total += it.PlayCount
							}
						}
						if total > 0 {
							scope := ""
							if region != "" {
								scope = " in region '" + region + "'"
							} else if city != "" {
								scope = " in city '" + city + "'"
							}
							answer := fmt.Sprintf("Kiosk '%s'%s has played poster '%s': %d plays.", kioskName, scope, posterName, total)
							if onToken != nil {
								onToken(answer)
							}
							return models.ChatResponse{Answer: answer, Steps: steps}, true, nil
						}
					}
				}
			}
		}
		for {
			path := fmt.Sprintf("/pop?poster_name=%s&page=%d&page_size=%d", urlEscape(posterName), page, pageSize)
			if strings.TrimSpace(region) != "" {
				path += "&region=" + urlEscape(region)
			} else if strings.TrimSpace(city) != "" {
				path += "&city=" + urlEscape(city)
			}
			status, body, err := c.Gateway.Get(path)
			step := models.Step{Tool: "popList", Status: status}
			if err != nil {
				step.Error = err.Error()
			} else {
				step.Body = clipString(strings.TrimSpace(string(body)), 2000)
			}
			steps = append(steps, step)
			if err != nil {
				return models.ChatResponse{Answer: "Failed to fetch POP data: " + err.Error(), Steps: steps}, true, nil
			}
			if status < 200 || status >= 300 {
				break
			}
			var resp popListResponse
			if json.Unmarshal(body, &resp) != nil {
				break
			}
			if len(resp.Items) == 0 {
				break
			}
			for _, it := range resp.Items {
				kn := strings.ToLower(strings.TrimSpace(it.KioskName))
				if kn == "" {
					continue
				}
				if kn == kioskLower || strings.Contains(kn, kioskLower) || strings.Contains(kioskLower, kn) {
					matchedPlays += it.PlayCount
					matched++
					if posterIDFound == "" && looksLikeUUID(it.PosterID) {
						posterIDFound = it.PosterID
					}
				}
			}
			if resp.Total > 0 {
				if int64(page*pageSize) >= resp.Total {
					break
				}
			} else if len(resp.Items) < pageSize {
				break
			}
			page++
			if page > maxPages {
				break
			}
		}
		if matched == 0 {
			return models.ChatResponse{Answer: "I couldn't resolve that kiosk name to a host, and I couldn't find matching POP rows by kiosk name. Please provide the server/host name.", Steps: steps}, true, nil
		}
		scope := ""
		if region != "" {
			scope = " in region '" + region + "'"
		} else if city != "" {
			scope = " in city '" + city + "'"
		}
		answer := fmt.Sprintf("Kiosk '%s'%s has played poster '%s': %d plays.", kioskName, scope, posterName, matchedPlays)
		if onToken != nil {
			onToken(answer)
		}
		if conversationID != "" {
			c.updateConversationPoster(conversationID, posterName, city, region)
			c.updateConversationPosterID(conversationID, posterIDFound)
			c.clearPending(conversationID)
		}
		return models.ChatResponse{Answer: answer, Steps: steps}, true, nil
	}

	page := 1
	pageSize := 200
	maxPages := 10
	steps := make([]models.Step, 0, 2)
	if resolveStep != nil {
		steps = append(steps, *resolveStep)
	}
	items := make([]popItem, 0, 64)
	for {
		path := fmt.Sprintf("/pop?poster_name=%s&host_name=%s&page=%d&page_size=%d", urlEscape(posterName), urlEscape(resolvedHost), page, pageSize)
		status, body, err := c.Gateway.Get(path)
		if err == nil && status == 400 {
			path2 := fmt.Sprintf("/pop?poster_name=%s&host=%s&page=%d&page_size=%d", urlEscape(posterName), urlEscape(resolvedHost), page, pageSize)
			status2, body2, err2 := c.Gateway.Get(path2)
			step2 := models.Step{Tool: "popList", Status: status2}
			if err2 != nil {
				step2.Error = err2.Error()
			} else {
				step2.Body = clipString(strings.TrimSpace(string(body2)), 2000)
			}
			steps = append(steps, step2)
			status, body, err = status2, body2, err2
		}
		step := models.Step{Tool: "popList", Status: status}
		if err != nil {
			step.Error = err.Error()
		} else {
			step.Body = clipString(strings.TrimSpace(string(body)), 2000)
		}
		steps = append(steps, step)
		if err != nil {
			return models.ChatResponse{Answer: "Failed to fetch POP data: " + err.Error(), Steps: steps}, true, nil
		}
		if status < 200 || status >= 300 {
			return models.ChatResponse{Answer: fmt.Sprintf("Failed to fetch POP data (status %d).", status), Steps: steps}, true, nil
		}
		var resp popListResponse
		if json.Unmarshal(body, &resp) != nil {
			return models.ChatResponse{Answer: "POP list response could not be parsed.", Steps: steps}, true, nil
		}
		if len(resp.Items) == 0 {
			break
		}
		items = append(items, resp.Items...)
		if resp.Total > 0 {
			if int64(page*pageSize) >= resp.Total {
				break
			}
		} else if len(resp.Items) < pageSize {
			break
		}
		page++
		if page > maxPages {
			break
		}
	}

	if len(items) == 0 {
		answer := fmt.Sprintf("No play counts found for poster '%s' on kiosk '%s'.", posterName, kioskName)
		if onToken != nil {
			onToken(answer)
		}
		return models.ChatResponse{Answer: answer, Steps: steps}, true, nil
	}

	totalPlays := int64(0)
	for _, it := range items {
		totalPlays += it.PlayCount
	}
	answer := fmt.Sprintf("Kiosk '%s' (%s) has played poster '%s': %d plays.", kioskName, resolvedHost, posterName, totalPlays)
	if onToken != nil {
		onToken(answer)
	}
	if conversationID != "" {
		c.updateConversationPoster(conversationID, posterName, "", "")
		c.updateConversationHost(conversationID, resolvedHost)
		c.clearPending(conversationID)
	}
	return models.ChatResponse{Answer: answer, Steps: steps}, true, nil
}

func extractNaturalDateRangeRFC3339(msg string) (string, string) {
	s := strings.ToLower(strings.TrimSpace(msg))
	if s == "" {
		return "", ""
	}
	idx := strings.Index(s, "from ")
	if idx < 0 {
		return "", ""
	}
	rest := strings.TrimSpace(s[idx+5:])
	endIdx := -1
	for _, sep := range []string{" till ", " to "} {
		if j := strings.Index(rest, sep); j >= 0 {
			endIdx = j
			break
		}
	}
	if endIdx < 0 {
		return "", ""
	}
	fromPart := strings.TrimSpace(rest[:endIdx])
	toPart := strings.TrimSpace(rest[endIdx:])
	toPart = strings.TrimSpace(strings.TrimPrefix(toPart, "till"))
	toPart = strings.TrimSpace(strings.TrimPrefix(toPart, "to"))
	toPart = strings.TrimSpace(toPart)

	fromPart = strings.ReplaceAll(fromPart, ",", " ")
	fromPart = strings.ReplaceAll(fromPart, "st", "")
	fromPart = strings.ReplaceAll(fromPart, "nd", "")
	fromPart = strings.ReplaceAll(fromPart, "rd", "")
	fromPart = strings.ReplaceAll(fromPart, "th", "")
	fromPart = strings.Join(strings.Fields(fromPart), " ")

	fromT, err := time.Parse("january 2 2006", fromPart)
	if err != nil {
		fromT, err = time.Parse("jan 2 2006", fromPart)
		if err != nil {
			return "", ""
		}
	}
	from := time.Date(fromT.Year(), fromT.Month(), fromT.Day(), 0, 0, 0, 0, time.UTC)

	var to time.Time
	toPart = strings.ReplaceAll(toPart, ",", " ")
	toPart = strings.Join(strings.Fields(toPart), " ")
	if strings.Contains(toPart, "today") {
		now := time.Now().UTC()
		to = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, 1)
	} else {
		toPart = strings.ReplaceAll(toPart, "st", "")
		toPart = strings.ReplaceAll(toPart, "nd", "")
		toPart = strings.ReplaceAll(toPart, "rd", "")
		toPart = strings.ReplaceAll(toPart, "th", "")
		toPart = strings.Join(strings.Fields(toPart), " ")
		toT, err2 := time.Parse("january 2 2006", toPart)
		if err2 != nil {
			toT, err2 = time.Parse("jan 2 2006", toPart)
			if err2 != nil {
				return "", ""
			}
		}
		to = time.Date(toT.Year(), toT.Month(), toT.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, 1)
	}
	if !from.Before(to) {
		return "", ""
	}
	return from.Format(time.RFC3339), to.Format(time.RFC3339)
}

func (c *ChatService) handlePopForPosterID(ctx context.Context, req models.ChatRequest, onToken func(string)) (models.ChatResponse, bool, error) {
	msgLower := strings.ToLower(req.Message)
	if !strings.Contains(msgLower, "pop") {
		return models.ChatResponse{}, false, nil
	}
	if !strings.Contains(msgLower, "poster") {
		return models.ChatResponse{}, false, nil
	}
	posterID := extractCampaignID(req.Message)
	if !looksLikeUUID(posterID) {
		return models.ChatResponse{}, false, nil
	}
	if c.Gateway == nil {
		return models.ChatResponse{Answer: "Tool gateway is not configured."}, true, nil
	}

	conversationID := strings.TrimSpace(req.ConversationID)
	isKioskWise := strings.Contains(msgLower, "kiosk wise") || strings.Contains(msgLower, "kiosk-wise") || strings.Contains(msgLower, "kioskwise") || strings.Contains(msgLower, "by kiosk")

	city := c.detectCityCode(ctx, msgLower)
	region := c.detectRegionCode(ctx, msgLower)
	if city == "" && region == "" && conversationID != "" {
		if st := c.getConversationState(conversationID); st != nil {
			if strings.TrimSpace(st.PosterRegion) != "" {
				region = strings.ToLower(strings.TrimSpace(st.PosterRegion))
			}
			if strings.TrimSpace(st.PosterCity) != "" {
				city = strings.ToLower(strings.TrimSpace(st.PosterCity))
			}
			if region == "" && strings.TrimSpace(st.Region) != "" {
				region = strings.ToLower(strings.TrimSpace(st.Region))
			}
			if city == "" && strings.TrimSpace(st.City) != "" {
				city = strings.ToLower(strings.TrimSpace(st.City))
			}
		}
	}

	// Pull POP rows filtered by poster_id + optional scope.
	type popItem struct {
		PosterName  string    `json:"poster_name"`
		PosterID    string    `json:"poster_id"`
		HostName    string    `json:"host_name"`
		KioskName   string    `json:"kiosk_name"`
		PosterType  string    `json:"poster_type"`
		PopDatetime time.Time `json:"pop_datetime"`
		City        string    `json:"city"`
		Region      string    `json:"region"`
		PlayCount   int64     `json:"play_count"`
	}
	type popListResponse struct {
		Items    []popItem `json:"items"`
		Total    int64     `json:"total"`
		Page     int       `json:"page"`
		PageSize int       `json:"page_size"`
	}

	page := 1
	pageSize := 200
	maxPages := 10
	steps := make([]models.Step, 0, 2)
	items := make([]popItem, 0, 64)
	for {
		path := fmt.Sprintf("/pop?poster_id=%s&page=%d&page_size=%d", urlEscape(posterID), page, pageSize)
		if strings.TrimSpace(region) != "" {
			path += "&region=" + urlEscape(region)
		} else if strings.TrimSpace(city) != "" {
			path += "&city=" + urlEscape(city)
		}
		status, body, err := c.Gateway.Get(path)
		step := models.Step{Tool: "popList", Status: status}
		if err != nil {
			step.Error = err.Error()
		} else {
			step.Body = clipString(strings.TrimSpace(string(body)), 2000)
		}
		steps = append(steps, step)
		if err != nil {
			return models.ChatResponse{Answer: "Failed to fetch POP data: " + err.Error(), Steps: steps}, true, nil
		}
		if status < 200 || status >= 300 {
			return models.ChatResponse{Answer: fmt.Sprintf("Failed to fetch POP data (status %d).", status), Steps: steps}, true, nil
		}
		var resp popListResponse
		if json.Unmarshal(body, &resp) != nil {
			return models.ChatResponse{Answer: "POP list response could not be parsed.", Steps: steps}, true, nil
		}
		if len(resp.Items) == 0 {
			break
		}
		items = append(items, resp.Items...)
		if resp.Total > 0 {
			if int64(page*pageSize) >= resp.Total {
				break
			}
		} else if len(resp.Items) < pageSize {
			break
		}
		page++
		if page > maxPages {
			break
		}
	}

	if len(items) == 0 {
		answer := fmt.Sprintf("No POP rows found for poster %s.", posterID)
		if onToken != nil {
			onToken(answer)
		}
		return models.ChatResponse{Answer: answer, Steps: steps}, true, nil
	}

	totalPlays := int64(0)
	for _, it := range items {
		totalPlays += it.PlayCount
	}

	posterName := strings.TrimSpace(items[0].PosterName)
	label := posterID
	if posterName != "" {
		label = posterName + " (" + posterID + ")"
	}
	scopeLabel := ""
	if strings.TrimSpace(region) != "" {
		scopeLabel = "region '" + strings.TrimSpace(region) + "'"
	} else if strings.TrimSpace(city) != "" {
		scopeLabel = "city '" + strings.TrimSpace(city) + "'"
	}
	if scopeLabel != "" {
		label = label + " in " + scopeLabel
	}

	if !isKioskWise {
		answer := fmt.Sprintf("POP for poster %s: %d plays.", label, totalPlays)
		if onToken != nil {
			onToken(answer)
		}
		return models.ChatResponse{Answer: answer, Steps: steps}, true, nil
	}

	type kv struct {
		Key   string
		Plays int64
	}
	byKiosk := map[string]int64{}
	for _, it := range items {
		k := strings.TrimSpace(it.KioskName)
		if k == "" {
			k = strings.TrimSpace(it.HostName)
		}
		if k == "" {
			continue
		}
		byKiosk[k] += it.PlayCount
	}
	rows := make([]kv, 0, len(byKiosk))
	for k, v := range byKiosk {
		rows = append(rows, kv{Key: k, Plays: v})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Plays > rows[j].Plays })
	if len(rows) > 10 {
		rows = rows[:10]
	}
	lines := make([]string, 0, len(rows)+2)
	lines = append(lines, fmt.Sprintf("POP for poster %s: %d plays", label, totalPlays))
	lines = append(lines, "Kiosk-wise:")
	for i, r := range rows {
		lines = append(lines, fmt.Sprintf("%d. %s — %d plays", i+1, r.Key, r.Plays))
	}
	answer := strings.Join(lines, "\n")
	if onToken != nil {
		onToken(answer)
	}
	if conversationID != "" {
		c.updateConversationPoster(conversationID, posterName, city, region)
		c.updateConversationPosterID(conversationID, posterID)
		c.clearPending(conversationID)
	}
	return models.ChatResponse{Answer: answer, Steps: steps}, true, nil
}

func parseMonthYearRangeRFC3339(msg string) (string, string) {
	s := strings.ToLower(strings.TrimSpace(msg))
	if s == "" {
		return "", ""
	}
	months := map[string]time.Month{
		"january":   time.January,
		"jan":       time.January,
		"february":  time.February,
		"feb":       time.February,
		"march":     time.March,
		"mar":       time.March,
		"april":     time.April,
		"apr":       time.April,
		"may":       time.May,
		"june":      time.June,
		"jun":       time.June,
		"july":      time.July,
		"jul":       time.July,
		"august":    time.August,
		"aug":       time.August,
		"september": time.September,
		"sep":       time.September,
		"sept":      time.September,
		"october":   time.October,
		"oct":       time.October,
		"november":  time.November,
		"nov":       time.November,
		"december":  time.December,
		"dec":       time.December,
	}
	words := tokenizeWords(s)
	month := time.Month(0)
	year := 0
	for i := 0; i < len(words); i++ {
		w := strings.ToLower(strings.TrimSpace(words[i]))
		if month == 0 {
			if m, ok := months[w]; ok {
				month = m
				// Year might be next token.
				if i+1 < len(words) {
					if y, err := strconv.Atoi(words[i+1]); err == nil && y >= 2000 && y <= 2100 {
						year = y
					}
				}
				continue
			}
		}
		if year == 0 {
			if y, err := strconv.Atoi(w); err == nil && y >= 2000 && y <= 2100 {
				year = y
			}
		}
	}
	if month == 0 || year == 0 {
		return "", ""
	}
	from := time.Date(year, month, 1, 0, 0, 0, 0, time.UTC)
	to := from.AddDate(0, 1, 0)
	return from.Format(time.RFC3339), to.Format(time.RFC3339)
}

func (c *ChatService) handlePosterMonthData(ctx context.Context, req models.ChatRequest, onToken func(string)) (models.ChatResponse, bool, error) {
	msgLower := strings.ToLower(req.Message)
	if !(strings.Contains(msgLower, "month") || strings.Contains(msgLower, "data")) {
		return models.ChatResponse{}, false, nil
	}
	fromRFC, toRFC := parseMonthYearRangeRFC3339(req.Message)
	if fromRFC == "" || toRFC == "" {
		return models.ChatResponse{}, false, nil
	}
	if c.Gateway == nil {
		return models.ChatResponse{Answer: "Tool gateway is not configured."}, true, nil
	}

	conversationID := strings.TrimSpace(req.ConversationID)
	if conversationID == "" {
		return models.ChatResponse{Answer: "Please provide a conversation id so I can reuse the last poster context."}, true, nil
	}
	st := c.getConversationState(conversationID)
	if st == nil {
		return models.ChatResponse{Answer: "Please ask for a poster first (by name or id), then ask for month data."}, true, nil
	}

	posterID := strings.TrimSpace(st.PosterID)
	posterName := strings.TrimSpace(st.PosterName)
	if posterID == "" && posterName == "" {
		return models.ChatResponse{Answer: "Please specify a poster (by name or id) before asking for month data."}, true, nil
	}

	city := c.detectCityCode(ctx, msgLower)
	region := c.detectRegionCode(ctx, msgLower)
	if city == "" && region == "" {
		if st != nil {
			if strings.TrimSpace(st.PosterRegion) != "" {
				region = strings.ToLower(strings.TrimSpace(st.PosterRegion))
			}
			if strings.TrimSpace(st.PosterCity) != "" {
				city = strings.ToLower(strings.TrimSpace(st.PosterCity))
			}
			if region == "" && strings.TrimSpace(st.Region) != "" {
				region = strings.ToLower(strings.TrimSpace(st.Region))
			}
			if city == "" && strings.TrimSpace(st.City) != "" {
				city = strings.ToLower(strings.TrimSpace(st.City))
			}
		}
	}
	if city == "" && region == "" {
		return models.ChatResponse{Answer: "Please specify a city or region code (for example: moco or brt)."}, true, nil
	}

	isKioskWise := strings.Contains(msgLower, "kiosk wise") || strings.Contains(msgLower, "kiosk-wise") || strings.Contains(msgLower, "by kiosk")

	type popItem struct {
		PosterName string `json:"poster_name"`
		PosterID   string `json:"poster_id"`
		HostName   string `json:"host_name"`
		KioskName  string `json:"kiosk_name"`
		City       string `json:"city"`
		Region     string `json:"region"`
		PlayCount  int64  `json:"play_count"`
	}
	type popListResponse struct {
		Items    []popItem `json:"items"`
		Total    int64     `json:"total"`
		Page     int       `json:"page"`
		PageSize int       `json:"page_size"`
	}

	page := 1
	pageSize := 200
	maxPages := 10
	steps := make([]models.Step, 0, 2)
	items := make([]popItem, 0, 64)
	for {
		path := fmt.Sprintf("/pop?from=%s&to=%s&page=%d&page_size=%d", urlEscape(fromRFC), urlEscape(toRFC), page, pageSize)
		if looksLikeUUID(posterID) {
			path += "&poster_id=" + urlEscape(posterID)
		} else {
			path += "&poster_name=" + urlEscape(posterName)
		}
		if strings.TrimSpace(region) != "" {
			path += "&region=" + urlEscape(region)
		} else if strings.TrimSpace(city) != "" {
			path += "&city=" + urlEscape(city)
		}
		status, body, err := c.Gateway.Get(path)
		step := models.Step{Tool: "popList", Status: status}
		if err != nil {
			step.Error = err.Error()
		} else {
			step.Body = clipString(strings.TrimSpace(string(body)), 2000)
		}
		steps = append(steps, step)
		if err != nil {
			return models.ChatResponse{Answer: "Failed to fetch POP data: " + err.Error(), Steps: steps}, true, nil
		}
		if status < 200 || status >= 300 {
			return models.ChatResponse{Answer: fmt.Sprintf("Failed to fetch POP data (status %d).", status), Steps: steps}, true, nil
		}
		var resp popListResponse
		if json.Unmarshal(body, &resp) != nil {
			return models.ChatResponse{Answer: "POP list response could not be parsed.", Steps: steps}, true, nil
		}
		if len(resp.Items) == 0 {
			break
		}
		items = append(items, resp.Items...)
		if resp.Total > 0 {
			if int64(page*pageSize) >= resp.Total {
				break
			}
		} else if len(resp.Items) < pageSize {
			break
		}
		page++
		if page > maxPages {
			break
		}
	}

	if len(items) == 0 {
		return models.ChatResponse{Answer: "No POP rows found for that month.", Steps: steps}, true, nil
	}
	totalPlays := int64(0)
	for _, it := range items {
		totalPlays += it.PlayCount
	}
	actualPosterName := strings.TrimSpace(items[0].PosterName)
	actualPosterID := strings.TrimSpace(items[0].PosterID)
	label := strings.TrimSpace(posterName)
	if label == "" {
		label = strings.TrimSpace(actualPosterName)
	}
	if label == "" {
		label = strings.TrimSpace(posterID)
	}
	if label == "" {
		label = strings.TrimSpace(actualPosterID)
	}
	monthLabel := strings.TrimSpace(req.Message)
	if idx := strings.Index(strings.ToLower(monthLabel), "month"); idx > 0 {
		monthLabel = strings.TrimSpace(monthLabel[:idx])
	}
	if conversationID != "" {
		// Keep poster memory consistent with what we actually queried/received.
		if actualPosterName != "" {
			c.updateConversationPoster(conversationID, actualPosterName, city, region)
		} else {
			c.updateConversationPoster(conversationID, posterName, city, region)
		}
		if looksLikeUUID(actualPosterID) {
			c.updateConversationPosterID(conversationID, actualPosterID)
		} else if looksLikeUUID(posterID) {
			c.updateConversationPosterID(conversationID, posterID)
		}
		c.clearPending(conversationID)
	}
	if !isKioskWise {
		answer := fmt.Sprintf("POP for poster '%s' for %s: %d plays.", label, monthLabel, totalPlays)
		if onToken != nil {
			onToken(answer)
		}
		return models.ChatResponse{Answer: answer, Steps: steps}, true, nil
	}

	// Kiosk-wise.
	byKiosk := map[string]int64{}
	for _, it := range items {
		k := strings.TrimSpace(it.KioskName)
		if k == "" {
			k = strings.TrimSpace(it.HostName)
		}
		if k == "" {
			continue
		}
		byKiosk[k] += it.PlayCount
	}
	type kv struct {
		Key   string
		Plays int64
	}
	rows := make([]kv, 0, len(byKiosk))
	for k, v := range byKiosk {
		rows = append(rows, kv{Key: k, Plays: v})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Plays > rows[j].Plays })
	if len(rows) > 10 {
		rows = rows[:10]
	}
	lines := make([]string, 0, len(rows)+2)
	lines = append(lines, fmt.Sprintf("POP for poster '%s' for %s: %d plays", label, monthLabel, totalPlays))
	lines = append(lines, "Kiosk-wise:")
	for i, r := range rows {
		lines = append(lines, fmt.Sprintf("%d. %s — %d plays", i+1, r.Key, r.Plays))
	}
	answer := strings.Join(lines, "\n")
	if onToken != nil {
		onToken(answer)
	}
	return models.ChatResponse{Answer: answer, Steps: steps}, true, nil
}

func (c *ChatService) handleLowUptimeDevices(ctx context.Context, req models.ChatRequest, onToken func(string)) (models.ChatResponse, bool, error) {
	msgLower := strings.ToLower(req.Message)
	if !strings.Contains(msgLower, "uptime") {
		return models.ChatResponse{}, false, nil
	}
	if !(strings.Contains(msgLower, "low") || strings.Contains(msgLower, "lowest") || strings.Contains(msgLower, "down") || strings.Contains(msgLower, "unstable")) {
		return models.ChatResponse{}, false, nil
	}
	if !(strings.Contains(msgLower, "device") || strings.Contains(msgLower, "devices") || strings.Contains(msgLower, "kiosk") || strings.Contains(msgLower, "kiosks")) {
		return models.ChatResponse{}, false, nil
	}
	// If a specific host is requested, let the per-host telemetry handler answer.
	if len(detectHostTokens(req.Message)) > 0 {
		return models.ChatResponse{}, false, nil
	}
	if c.Gateway == nil {
		return models.ChatResponse{Answer: "Tool gateway is not configured."}, true, nil
	}

	conversationID := strings.TrimSpace(req.ConversationID)
	city := c.detectCityCode(ctx, msgLower)
	region := c.detectRegionCode(ctx, msgLower)
	if city == "" && region == "" && conversationID != "" {
		if st := c.getConversationState(conversationID); st != nil {
			if strings.TrimSpace(st.City) != "" {
				city = strings.ToLower(strings.TrimSpace(st.City))
			}
			if strings.TrimSpace(st.Region) != "" {
				region = strings.ToLower(strings.TrimSpace(st.Region))
			}
		}
	}

	filterCity := strings.ToLower(strings.TrimSpace(city))
	filterRegion := strings.ToLower(strings.TrimSpace(region))

	type row struct {
		ServerID string
		City     string
		Region   string
		Uptime   int64
		Time     time.Time
	}

	rows := make([]row, 0, 128)
	steps := make([]models.Step, 0, 3)
	page := 1
	pageSize := 200
	maxPages := 5
	for {
		path := fmt.Sprintf("/metrics/latest?page=%d&page_size=%d&include_totals=false", page, pageSize)
		status, body, err := c.Gateway.Get(path)
		step := models.Step{Tool: "metricsLatest", Status: status}
		if err != nil {
			step.Error = err.Error()
		} else {
			step.Body = clipString(strings.TrimSpace(string(body)), 2000)
		}
		steps = append(steps, step)
		if err != nil {
			return models.ChatResponse{Answer: "Failed to fetch latest metrics: " + err.Error(), Steps: steps}, true, nil
		}
		if status < 200 || status >= 300 {
			return models.ChatResponse{Answer: fmt.Sprintf("Failed to fetch latest metrics (status %d).", status), Steps: steps}, true, nil
		}

		var payload struct {
			Data []struct {
				Time     time.Time `json:"time"`
				Uptime   int64     `json:"uptime"`
				ServerID string    `json:"server_id"`
				City     string    `json:"city"`
				Region   string    `json:"region"`
			} `json:"data"`
			Pagination struct {
				HasMore bool `json:"has_more"`
			} `json:"pagination"`
		}
		if json.Unmarshal(body, &payload) != nil {
			return models.ChatResponse{Answer: "Latest metrics response could not be parsed.", Steps: steps}, true, nil
		}

		for _, it := range payload.Data {
			sid := strings.ToLower(strings.TrimSpace(it.ServerID))
			if sid == "" {
				continue
			}
			itCity := strings.ToLower(strings.TrimSpace(it.City))
			itRegion := strings.ToLower(strings.TrimSpace(it.Region))
			if filterCity != "" && itCity != filterCity {
				continue
			}
			if filterRegion != "" && itRegion != filterRegion {
				continue
			}
			rows = append(rows, row{ServerID: sid, City: itCity, Region: itRegion, Uptime: it.Uptime, Time: it.Time})
		}

		if !payload.Pagination.HasMore {
			break
		}
		page++
		if page > maxPages {
			break
		}
	}

	if len(rows) == 0 {
		scope := "the current scope"
		if filterRegion != "" || filterCity != "" {
			parts := make([]string, 0, 2)
			if filterRegion != "" {
				parts = append(parts, "region '"+filterRegion+"'")
			}
			if filterCity != "" {
				parts = append(parts, "city '"+filterCity+"'")
			}
			scope = strings.Join(parts, ", ")
		}
		answer := fmt.Sprintf("No devices with uptime metrics were found for %s.", scope)
		if onToken != nil {
			onToken(answer)
		}
		return models.ChatResponse{Answer: answer, Steps: steps}, true, nil
	}

	sort.Slice(rows, func(i, j int) bool {
		ui := rows[i].Uptime
		uj := rows[j].Uptime
		if ui == 0 && uj > 0 {
			return true
		}
		if uj == 0 && ui > 0 {
			return false
		}
		if ui == uj {
			return rows[i].ServerID < rows[j].ServerID
		}
		return ui < uj
	})

	limit := 10
	if n := extractTopN(msgLower); n > 0 {
		limit = n
	}
	if limit > 50 {
		limit = 50
	}
	if limit > len(rows) {
		limit = len(rows)
	}

	lines := make([]string, 0, limit)
	for i := 0; i < limit; i++ {
		r := rows[i]
		label := r.ServerID
		if r.City != "" || r.Region != "" {
			label = fmt.Sprintf("%s (%s/%s)", r.ServerID, r.City, r.Region)
		}
		d := time.Duration(r.Uptime) * time.Second
		if r.Uptime == 0 {
			lines = append(lines, fmt.Sprintf("%d. %s — uptime unknown/0", i+1, label))
		} else {
			lines = append(lines, fmt.Sprintf("%d. %s — %s", i+1, label, d.String()))
		}
	}

	answer := "Devices with lowest uptime:\n" + strings.Join(lines, "\n")
	if onToken != nil {
		onToken(answer)
	}
	return models.ChatResponse{Answer: answer, Steps: steps}, true, nil
}

func (c *ChatService) handlePopYesterdayByHost(ctx context.Context, req models.ChatRequest, onToken func(string)) (models.ChatResponse, bool, error) {
	msgLower := strings.ToLower(req.Message)
	if !strings.Contains(msgLower, "pop") {
		return models.ChatResponse{}, false, nil
	}
	if !(strings.Contains(msgLower, "yesterday") || strings.Contains(msgLower, "yesterday's") || strings.Contains(msgLower, "yesterdays")) {
		return models.ChatResponse{}, false, nil
	}
	showMinutes := strings.Contains(msgLower, "minute") || strings.Contains(msgLower, "minutes")

	if c.Gateway == nil {
		return models.ChatResponse{Answer: "Tool gateway is not configured."}, true, nil
	}

	conversationID := strings.TrimSpace(req.ConversationID)
	host := ""
	if tokens := detectHostTokens(req.Message); len(tokens) > 0 {
		candidate := strings.ToLower(strings.TrimSpace(tokens[0]))
		parts := strings.Split(strings.ReplaceAll(candidate, "_", "-"), "-")
		if len(parts) >= 3 {
			host = candidate
		}
	}

	// If user didn't provide a host-like token, try last host or resolve display name.
	var resolveStep *models.Step
	if host == "" {
		if conversationID != "" {
			if st := c.getConversationState(conversationID); st != nil {
				if strings.TrimSpace(st.Host) != "" {
					host = strings.ToLower(strings.TrimSpace(st.Host))
				}
			}
		}
	}
	if host == "" {
		// Try to extract the display name portion (avoid passing the whole sentence into the resolver).
		lookup := extractAfterKeyword(msgLower, "for")
		if lookup == "" {
			lookup = extractAfterKeyword(msgLower, "of")
		}
		if lookup == "" {
			lookup = extractAfterKeyword(msgLower, "info")
		}
		if lookup == "" {
			lookup = extractAfterKeyword(msgLower, "details")
		}
		if lookup == "" {
			lookup = extractAfterKeyword(msgLower, "about")
		}
		lookup = strings.TrimSpace(lookup)
		if lookup == "" {
			lookup = req.Message
		}
		if resolved, step := c.resolveHostFromDeviceName(ctx, conversationID, lookup); strings.TrimSpace(resolved) != "" {
			host = strings.ToLower(strings.TrimSpace(resolved))
			resolveStep = step
		} else if lookup != req.Message {
			// Fallback: some phrasings may not extract cleanly; try resolving using the full message.
			if resolved2, step2 := c.resolveHostFromDeviceName(ctx, conversationID, req.Message); strings.TrimSpace(resolved2) != "" {
				host = strings.ToLower(strings.TrimSpace(resolved2))
				resolveStep = step2
			}
		}
	}
	if host == "" {
		return models.ChatResponse{Answer: "Please specify the device host/server id (for example: moco-brt-briggs-001) or a kiosk display name."}, true, nil
	}
	if conversationID != "" {
		c.updateConversationHost(conversationID, host)
		c.clearPending(conversationID)
	}

	// Pull yesterday's POP rows for this host.
	type popItem struct {
		PosterName  string    `json:"poster_name"`
		PosterID    string    `json:"poster_id"`
		HostName    string    `json:"host_name"`
		KioskName   string    `json:"kiosk_name"`
		PosterType  string    `json:"poster_type"`
		PopDatetime time.Time `json:"pop_datetime"`
		KioskLat    float64   `json:"kiosk_lat"`
		KioskLong   float64   `json:"kiosk_long"`
		City        string    `json:"city"`
		Region      string    `json:"region"`
		PlayCount   int64     `json:"play_count"`
		Value       int64     `json:"value"`
		Type        string    `json:"type"`
		Url         string    `json:"url"`
	}
	type popListResponse struct {
		Items    []popItem `json:"items"`
		Total    int64     `json:"total"`
		Page     int       `json:"page"`
		PageSize int       `json:"page_size"`
	}

	page := 1
	pageSize := 200
	maxPages := 10
	steps := make([]models.Step, 0, 2)
	items := make([]popItem, 0, 64)
	// Prefer explicit RFC3339 date range for determinism.
	// Use UTC day boundaries: [yesterday 00:00, today 00:00).
	todayStartUTC := time.Now().UTC().Truncate(24 * time.Hour)
	yesterdayStartUTC := todayStartUTC.Add(-24 * time.Hour)
	fromRFC := yesterdayStartUTC.Format(time.RFC3339)
	toRFC := todayStartUTC.Format(time.RFC3339)
	useDateRange := true
	// Tool gateway preset values have differed across deployments; try a few common aliases.
	presets := []string{"yesterday", "previous_day", "prev_day", "last_day"}
	selectedPreset := ""
	forLoop:
	for {
		if useDateRange {
			path := fmt.Sprintf("/pop?host_name=%s&from=%s&to=%s&page=%d&page_size=%d", urlEscape(host), urlEscape(fromRFC), urlEscape(toRFC), page, pageSize)
			status, body, err := c.Gateway.Get(path)
			step := models.Step{Tool: "popList", Status: status}
			if err != nil {
				step.Error = err.Error()
			} else {
				step.Body = clipString(strings.TrimSpace(string(body)), 2000)
			}
			steps = append(steps, step)
			if err != nil {
				return models.ChatResponse{Answer: "Failed to fetch POP data: " + err.Error(), Steps: steps}, true, nil
			}
			if status >= 200 && status < 300 {
				var resp popListResponse
				if json.Unmarshal(body, &resp) != nil {
					return models.ChatResponse{Answer: "POP list response could not be parsed.", Steps: steps}, true, nil
				}
				if len(resp.Items) == 0 {
					break
				}
				items = append(items, resp.Items...)
				if resp.Total > 0 {
					if int64(page*pageSize) >= resp.Total {
						break
					}
				} else if len(resp.Items) < pageSize {
					break
				}
				page++
				if page > maxPages {
					break
				}
				continue
			}
			// If the gateway doesn't support from/to filtering, fall back to preset probing.
			if status == 400 {
				useDateRange = false
				// Reset pagination for preset mode.
				page = 1
				continue
			}
			return models.ChatResponse{Answer: fmt.Sprintf("Failed to fetch POP data (status %d).", status), Steps: steps}, true, nil
		}

		if selectedPreset == "" {
			// Probe which preset works on the first page.
			for _, p := range presets {
				probePath := fmt.Sprintf("/pop?host_name=%s&preset=%s&page=%d&page_size=%d", urlEscape(host), urlEscape(p), page, pageSize)
				status, body, err := c.Gateway.Get(probePath)
				step := models.Step{Tool: "popList", Status: status}
				if err != nil {
					step.Error = err.Error()
				} else {
					step.Body = clipString(strings.TrimSpace(string(body)), 2000)
				}
				steps = append(steps, step)
				if err != nil {
					return models.ChatResponse{Answer: "Failed to fetch POP data: " + err.Error(), Steps: steps}, true, nil
				}
				if status >= 200 && status < 300 {
					selectedPreset = p
					var resp popListResponse
					if json.Unmarshal(body, &resp) != nil {
						return models.ChatResponse{Answer: "POP list response could not be parsed.", Steps: steps}, true, nil
					}
					if len(resp.Items) == 0 {
						return models.ChatResponse{Answer: fmt.Sprintf("No POP data was found for '%s' yesterday.", host), Steps: steps}, true, nil
					}
					items = append(items, resp.Items...)
					if resp.Total > 0 {
						if int64(page*pageSize) >= resp.Total {
							break
						}
					} else if len(resp.Items) < pageSize {
						break
					}
					page++
					continue forLoop
				}
				// If preset is invalid, gateway typically returns 400 with {"error":"invalid preset"}.
				if status == 400 && strings.Contains(strings.ToLower(strings.TrimSpace(string(body))), "invalid preset") {
					continue
				}
				// Any other non-2xx is a real failure.
				return models.ChatResponse{Answer: fmt.Sprintf("Failed to fetch POP data (status %d).", status), Steps: steps}, true, nil
			}
			return models.ChatResponse{Answer: "This POP endpoint does not appear to support a 'yesterday' preset on this gateway.", Steps: steps}, true, nil
		}

		path := fmt.Sprintf("/pop?host_name=%s&preset=%s&page=%d&page_size=%d", urlEscape(host), urlEscape(selectedPreset), page, pageSize)
		status, body, err := c.Gateway.Get(path)
		step := models.Step{Tool: "popList", Status: status}
		if err != nil {
			step.Error = err.Error()
		} else {
			step.Body = clipString(strings.TrimSpace(string(body)), 2000)
		}
		steps = append(steps, step)
		if err != nil {
			return models.ChatResponse{Answer: "Failed to fetch POP data: " + err.Error(), Steps: steps}, true, nil
		}
		if status < 200 || status >= 300 {
			return models.ChatResponse{Answer: fmt.Sprintf("Failed to fetch POP data (status %d).", status), Steps: steps}, true, nil
		}
		var resp popListResponse
		if json.Unmarshal(body, &resp) != nil {
			return models.ChatResponse{Answer: "POP list response could not be parsed.", Steps: steps}, true, nil
		}
		if len(resp.Items) == 0 {
			break
		}
		items = append(items, resp.Items...)
		// Stop once we are past the last page when total is available.
		if resp.Total > 0 {
			if int64(page*pageSize) >= resp.Total {
				break
			}
		} else if len(resp.Items) < pageSize {
			break
		}
		page++
		if page > maxPages {
			break
		}
	}
	if resolveStep != nil {
		steps = append([]models.Step{*resolveStep}, steps...)
	}

	if len(items) == 0 {
		return models.ChatResponse{Answer: fmt.Sprintf("No POP data was found for '%s' yesterday.", host), Steps: steps}, true, nil
	}

	// Aggregate by poster.
	type agg struct {
		PosterID   string
		PosterName string
		PosterType string
		Url        string
		PlayCount  int64
		Value      int64
		LastSeen   time.Time
		KioskName  string
		KioskLat   float64
		KioskLong  float64
		City       string
		Region     string
		HostName   string
	}
	byPoster := map[string]*agg{}
	for _, it := range items {
		key := strings.TrimSpace(it.PosterID)
		if key == "" {
			key = strings.TrimSpace(it.PosterName)
		}
		if key == "" {
			continue
		}
		a := byPoster[key]
		if a == nil {
			a = &agg{
				PosterID:   strings.TrimSpace(it.PosterID),
				PosterName: strings.TrimSpace(it.PosterName),
				PosterType: strings.TrimSpace(it.PosterType),
				Url:        strings.TrimSpace(it.Url),
				KioskName:  strings.TrimSpace(it.KioskName),
				KioskLat:   it.KioskLat,
				KioskLong:  it.KioskLong,
				City:       strings.ToLower(strings.TrimSpace(it.City)),
				Region:     strings.ToLower(strings.TrimSpace(it.Region)),
				HostName:   strings.ToLower(strings.TrimSpace(it.HostName)),
			}
			byPoster[key] = a
		}
		a.PlayCount += it.PlayCount
		a.Value += it.Value
		if it.PopDatetime.After(a.LastSeen) {
			a.LastSeen = it.PopDatetime
			if strings.TrimSpace(it.Url) != "" {
				a.Url = strings.TrimSpace(it.Url)
			}
			if strings.TrimSpace(it.PosterType) != "" {
				a.PosterType = strings.TrimSpace(it.PosterType)
			}
		}
		if strings.TrimSpace(a.PosterName) == "" && strings.TrimSpace(it.PosterName) != "" {
			a.PosterName = strings.TrimSpace(it.PosterName)
		}
	}

	rows := make([]*agg, 0, len(byPoster))
	for _, a := range byPoster {
		rows = append(rows, a)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].PlayCount > rows[j].PlayCount })
	if len(rows) > 10 {
		rows = rows[:10]
	}

	first := rows[0]
	lines := make([]string, 0, len(rows)+2)
	if showMinutes {
		lines = append(lines, fmt.Sprintf("Yesterday's POP for '%s' (%s) in minutes:", host, strings.TrimSpace(first.KioskName)))
		lines = append(lines, "(Minutes computed from POP 'value' duration; if missing, estimated assuming 10 seconds per play.)")
	} else {
		lines = append(lines, fmt.Sprintf("Yesterday's POP for '%s' (%s):", host, strings.TrimSpace(first.KioskName)))
	}
	for i, r := range rows {
		name := r.PosterName
		if strings.TrimSpace(name) == "" {
			name = r.PosterID
		}
		if strings.TrimSpace(name) == "" {
			continue
		}
		extra := ""
		if strings.TrimSpace(r.PosterType) != "" {
			extra = " (" + strings.TrimSpace(r.PosterType) + ")"
		}
		if showMinutes {
			seconds := r.Value
			if seconds <= 0 {
				seconds = r.PlayCount * 10
			}
			mins := float64(seconds) / 60.0
			lines = append(lines, fmt.Sprintf("%d. %s — %.1f minutes%s", i+1, name, mins, extra))
		} else {
			lines = append(lines, fmt.Sprintf("%d. %s — %d plays%s", i+1, name, r.PlayCount, extra))
		}
	}
	lines = append(lines, fmt.Sprintf("Location: %.6f, %.6f | Last update: %s", first.KioskLat, first.KioskLong, first.LastSeen.UTC().Format(time.RFC3339)))
	answer := strings.Join(lines, "\n")
	if onToken != nil {
		onToken(answer)
	}
	return models.ChatResponse{Answer: answer, Steps: steps}, true, nil
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

func (c *ChatService) isKnownRegionCode(ctx context.Context, code string) bool {
	code = strings.ToLower(strings.TrimSpace(code))
	if code == "" {
		return false
	}
	c.cityMu.Lock()
	defer c.cityMu.Unlock()

	c.ensureCityRegionCachesLocked(ctx)
	if c.regionCache == nil {
		return false
	}
	_, ok := c.regionCache[code]
	return ok
}

func (c *ChatService) isKnownProjectCityCode(ctx context.Context, code string) bool {
	code = strings.ToLower(strings.TrimSpace(code))
	if code == "" {
		return false
	}
	c.projectMu.Lock()
	defer c.projectMu.Unlock()

	c.ensureProjectLookupsLocked(ctx)
	if c.projectCityCache == nil {
		return false
	}
	_, ok := c.projectCityCache[code]
	return ok
}

func (c *ChatService) normalizePopQueryLocation(ctx context.Context, path string, query map[string]string) map[string]string {
	if query == nil {
		return query
	}
	p := strings.TrimSpace(path)
	if p != "/pop/stats" {
		return query
	}
	city := strings.ToLower(strings.TrimSpace(query["city"]))
	if city == "" {
		return query
	}
	// If model incorrectly passes a region code in city param (e.g. city=brt, city=kc),
	// rewrite to region=<code> and drop city to enforce correct scoping.
	// Heuristic for overlaps (tokens that can exist as both city and region):
	// - Short codes (<=3) default to region unless user explicitly sets region/city elsewhere.
	// - Longer codes (e.g. moco, kcmo) default to city.
	if strings.TrimSpace(query["region"]) == "" && c.isKnownRegionCode(ctx, city) {
		if len(city) <= 3 || !c.isKnownProjectCityCode(ctx, city) {
			query["region"] = city
			delete(query, "city")
		}
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

func normalizeCitySelection(city, region, msgLower string) (string, bool) {
	city = strings.ToLower(strings.TrimSpace(city))
	region = strings.ToLower(strings.TrimSpace(region))
	cityExplicit := false
	if city != "" {
		if len(city) >= 3 || region == "" || strings.Contains(msgLower, "city") {
			cityExplicit = true
		}
	}
	if region != "" && !cityExplicit {
		city = ""
	}
	return city, cityExplicit
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
		// Allow follow-ups like "show top posters" to be handled deterministically using the
		// remembered city/region scope in the conversation.
		if strings.Contains(s, "from") || strings.Contains(s, "in") || strings.Contains(s, "city") {
			return true
		}
		if strings.Contains(s, "perform") {
			return true
		}
		return true
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

func (c *ChatService) handlePopTodayByHost(ctx context.Context, req models.ChatRequest, onToken func(string)) (models.ChatResponse, bool, error) {
	msgLower := strings.ToLower(req.Message)
	// Intent:
	// - "pop today", "current day pop"
	// - "stats for <device>" (treat as today's stats by default)
	isPop := strings.Contains(msgLower, "pop") || strings.Contains(msgLower, "stats")
	isToday := strings.Contains(msgLower, "today") || strings.Contains(msgLower, "today's") || strings.Contains(msgLower, "todays") || strings.Contains(msgLower, "current_day")
	isStatsForDevice := strings.Contains(msgLower, "stats") && (strings.Contains(msgLower, "device") || strings.Contains(msgLower, "kiosk") || strings.Contains(msgLower, "server"))
	// Follow-up like "show pop" after a device info query: reuse last resolved host.
	msgTrim := strings.ToLower(strings.TrimSpace(msgLower))
	isShowPopFollowup := msgTrim == "pop" || msgTrim == "show pop" || msgTrim == "show me pop" || msgTrim == "show the pop" || msgTrim == "get pop" || msgTrim == "get me pop"
	if !isPop || (!isToday && !isStatsForDevice && !isShowPopFollowup) {
		return models.ChatResponse{}, false, nil
	}
	showMinutes := strings.Contains(msgLower, "minute") || strings.Contains(msgLower, "minutes")

	if c.Gateway == nil {
		return models.ChatResponse{Answer: "Tool gateway is not configured."}, true, nil
	}

	conversationID := strings.TrimSpace(req.ConversationID)
	host := ""
	if tokens := detectHostTokens(req.Message); len(tokens) > 0 {
		candidate := strings.ToLower(strings.TrimSpace(tokens[0]))
		parts := strings.Split(strings.ReplaceAll(candidate, "_", "-"), "-")
		if len(parts) >= 3 {
			host = candidate
		}
	}

	// If user didn't provide a host-like token, try last host or resolve display name.
	var resolveStep *models.Step
	if host == "" {
		if conversationID != "" {
			if st := c.getConversationState(conversationID); st != nil {
				if strings.TrimSpace(st.Host) != "" {
					host = strings.ToLower(strings.TrimSpace(st.Host))
				}
			}
		}
	}
	if host == "" {
		// Try to extract the display name portion (avoid passing the whole sentence into the resolver).
		lookup := extractAfterKeyword(msgLower, "stats for")
		if lookup == "" {
			lookup = extractAfterKeyword(msgLower, "for")
		}
		if lookup == "" {
			lookup = extractAfterKeyword(msgLower, "info")
		}
		if lookup == "" {
			lookup = extractAfterKeyword(msgLower, "details")
		}
		if lookup == "" {
			lookup = extractAfterKeyword(msgLower, "about")
		}
		lookup = strings.TrimSpace(lookup)
		if lookup == "" {
			lookup = req.Message
		}
		if resolved, step := c.resolveHostFromDeviceName(ctx, conversationID, lookup); strings.TrimSpace(resolved) != "" {
			host = strings.ToLower(strings.TrimSpace(resolved))
			resolveStep = step
		} else if lookup != req.Message {
			// Fallback: some phrasings may not extract cleanly; try resolving using the full message.
			if resolved2, step2 := c.resolveHostFromDeviceName(ctx, conversationID, req.Message); strings.TrimSpace(resolved2) != "" {
				host = strings.ToLower(strings.TrimSpace(resolved2))
				resolveStep = step2
			}
		}
	}
	if host == "" {
		return models.ChatResponse{Answer: "Please specify the device host/server id (for example: moco-brt-briggs-001) or a kiosk display name."}, true, nil
	}
	if conversationID != "" {
		c.updateConversationHost(conversationID, host)
		c.clearPending(conversationID)
	}

	// Pull today's POP rows for this host.
	type popItem struct {
		PosterName  string    `json:"poster_name"`
		PosterID    string    `json:"poster_id"`
		HostName    string    `json:"host_name"`
		KioskName   string    `json:"kiosk_name"`
		PosterType  string    `json:"poster_type"`
		PopDatetime time.Time `json:"pop_datetime"`
		KioskLat    float64   `json:"kiosk_lat"`
		KioskLong   float64   `json:"kiosk_long"`
		City        string    `json:"city"`
		Region      string    `json:"region"`
		PlayCount   int64     `json:"play_count"`
		Value       int64     `json:"value"`
		Type        string    `json:"type"`
		Url         string    `json:"url"`
	}
	type popListResponse struct {
		Items    []popItem `json:"items"`
		Total    int64     `json:"total"`
		Page     int       `json:"page"`
		PageSize int       `json:"page_size"`
	}

	page := 1
	pageSize := 200
	maxPages := 10
	steps := make([]models.Step, 0, 2)
	items := make([]popItem, 0, 64)
	for {
		path := fmt.Sprintf("/pop?host_name=%s&preset=today&page=%d&page_size=%d", urlEscape(host), page, pageSize)
		status, body, err := c.Gateway.Get(path)
		step := models.Step{Tool: "popList", Status: status}
		if err != nil {
			step.Error = err.Error()
		} else {
			step.Body = clipString(strings.TrimSpace(string(body)), 2000)
		}
		steps = append(steps, step)
		if err != nil {
			return models.ChatResponse{Answer: "Failed to fetch POP data: " + err.Error(), Steps: steps}, true, nil
		}
		if status < 200 || status >= 300 {
			return models.ChatResponse{Answer: fmt.Sprintf("Failed to fetch POP data (status %d).", status), Steps: steps}, true, nil
		}
		var resp popListResponse
		if json.Unmarshal(body, &resp) != nil {
			return models.ChatResponse{Answer: "POP list response could not be parsed.", Steps: steps}, true, nil
		}
		if len(resp.Items) == 0 {
			break
		}
		items = append(items, resp.Items...)
		// Stop once we are past the last page when total is available.
		if resp.Total > 0 {
			if int64(page*pageSize) >= resp.Total {
				break
			}
		} else if len(resp.Items) < pageSize {
			break
		}
		page++
		if page > maxPages {
			break
		}
	}
	if resolveStep != nil {
		steps = append([]models.Step{*resolveStep}, steps...)
	}

	if len(items) == 0 {
		return models.ChatResponse{Answer: fmt.Sprintf("No POP data was found for '%s' today.", host), Steps: steps}, true, nil
	}

	// Aggregate by poster.
	type agg struct {
		PosterID    string
		PosterName  string
		PosterType  string
		Url         string
		PlayCount   int64
		Value       int64
		LastSeen    time.Time
		KioskName   string
		KioskLat    float64
		KioskLong   float64
		City        string
		Region      string
		HostName    string
	}
	byPoster := map[string]*agg{}
	for _, it := range items {
		key := strings.TrimSpace(it.PosterID)
		if key == "" {
			key = strings.TrimSpace(it.PosterName)
		}
		if key == "" {
			continue
		}
		a := byPoster[key]
		if a == nil {
			a = &agg{
				PosterID:   strings.TrimSpace(it.PosterID),
				PosterName: strings.TrimSpace(it.PosterName),
				PosterType: strings.TrimSpace(it.PosterType),
				Url:        strings.TrimSpace(it.Url),
				KioskName:  strings.TrimSpace(it.KioskName),
				KioskLat:   it.KioskLat,
				KioskLong:  it.KioskLong,
				City:       strings.ToLower(strings.TrimSpace(it.City)),
				Region:     strings.ToLower(strings.TrimSpace(it.Region)),
				HostName:   strings.ToLower(strings.TrimSpace(it.HostName)),
			}
			byPoster[key] = a
		}
		a.PlayCount += it.PlayCount
		a.Value += it.Value
		if it.PopDatetime.After(a.LastSeen) {
			a.LastSeen = it.PopDatetime
			if strings.TrimSpace(it.Url) != "" {
				a.Url = strings.TrimSpace(it.Url)
			}
			if strings.TrimSpace(it.PosterType) != "" {
				a.PosterType = strings.TrimSpace(it.PosterType)
			}
		}
		if strings.TrimSpace(a.PosterName) == "" && strings.TrimSpace(it.PosterName) != "" {
			a.PosterName = strings.TrimSpace(it.PosterName)
		}
	}

	rows := make([]*agg, 0, len(byPoster))
	for _, a := range byPoster {
		rows = append(rows, a)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].PlayCount > rows[j].PlayCount })
	if len(rows) > 10 {
		rows = rows[:10]
	}

	first := rows[0]
	lines := make([]string, 0, len(rows)+2)
	if showMinutes {
		lines = append(lines, fmt.Sprintf("Today's POP for '%s' (%s) in minutes:", host, strings.TrimSpace(first.KioskName)))
		lines = append(lines, "(Minutes computed from POP 'value' duration; if missing, estimated assuming 10 seconds per play.)")
	} else {
		lines = append(lines, fmt.Sprintf("Today's POP for '%s' (%s):", host, strings.TrimSpace(first.KioskName)))
	}
	for i, r := range rows {
		name := r.PosterName
		if strings.TrimSpace(name) == "" {
			name = r.PosterID
		}
		if strings.TrimSpace(name) == "" {
			continue
		}
		extra := ""
		if strings.TrimSpace(r.PosterType) != "" {
			extra = " (" + strings.TrimSpace(r.PosterType) + ")"
		}
		if showMinutes {
			seconds := r.Value
			if seconds <= 0 {
				seconds = r.PlayCount * 10
			}
			mins := float64(seconds) / 60.0
			lines = append(lines, fmt.Sprintf("%d. %s — %.1f minutes%s", i+1, name, mins, extra))
		} else {
			lines = append(lines, fmt.Sprintf("%d. %s — %d plays%s", i+1, name, r.PlayCount, extra))
		}
	}
	lines = append(lines, fmt.Sprintf("Location: %.6f, %.6f | Last update: %s", first.KioskLat, first.KioskLong, first.LastSeen.UTC().Format(time.RFC3339)))
	answer := strings.Join(lines, "\n")
	if onToken != nil {
		onToken(answer)
	}
	return models.ChatResponse{Answer: answer, Steps: steps}, true, nil
}

func (c *ChatService) handleVenueSearchList(ctx context.Context, req models.ChatRequest, onToken func(string)) (models.ChatResponse, bool, error) {
	msgLower := strings.ToLower(req.Message)
	if !(strings.Contains(msgLower, "venue") || strings.Contains(msgLower, "venues")) {
		return models.ChatResponse{}, false, nil
	}
	if c.Gateway == nil {
		return models.ChatResponse{Answer: "Tool gateway is not configured."}, true, nil
	}
	isSearch := strings.Contains(msgLower, "search") || strings.Contains(msgLower, "find")
	isList := strings.Contains(msgLower, "list") || strings.Contains(msgLower, "show") || strings.Contains(msgLower, "all")
	if !(isSearch || isList) {
		return models.ChatResponse{}, false, nil
	}
	query := ""
	if isSearch {
		query = extractAfterKeyword(msgLower, "venues")
		if query == "" {
			query = extractAfterKeyword(msgLower, "venue")
		}
		query = strings.TrimSpace(query)
		if query == "" {
			return models.ChatResponse{Answer: "Please provide a venue search query (for example: search venues Union Station)."}, true, nil
		}
	}

	status, body, err := c.Gateway.Get("/ads/venues?page=1&page_size=50")
	step := models.Step{Tool: "adsVenues", Status: status}
	if err != nil {
		step.Error = err.Error()
	} else {
		step.Body = clipString(strings.TrimSpace(string(body)), 2000)
	}
	steps := []models.Step{step}
	if err != nil {
		return models.ChatResponse{Answer: "Failed to fetch venues: " + err.Error(), Steps: steps}, true, nil
	}
	if status < 200 || status >= 300 {
		return models.ChatResponse{Answer: fmt.Sprintf("Failed to fetch venues (status %d).", status), Steps: steps}, true, nil
	}
	var parsed map[string]any
	if json.Unmarshal(body, &parsed) != nil {
		return models.ChatResponse{Answer: "Venues response could not be parsed.", Steps: steps}, true, nil
	}
	rowsAny, _ := parsed["data"].([]any)
	if len(rowsAny) == 0 {
		return models.ChatResponse{Answer: "No venues found.", Steps: steps}, true, nil
	}

	qLower := strings.ToLower(query)
	rows := make([]map[string]any, 0, len(rowsAny))
	for _, it := range rowsAny {
		m, ok := it.(map[string]any)
		if !ok {
			continue
		}
		name, _ := m["name"].(string)
		idNum := 0
		switch v := m["id"].(type) {
		case float64:
			idNum = int(v)
		case int:
			idNum = v
		}
		name = strings.TrimSpace(name)
		if name == "" || idNum <= 0 {
			continue
		}
		if isSearch {
			if !strings.Contains(strings.ToLower(name), qLower) {
				continue
			}
		}
		rows = append(rows, m)
	}
	if len(rows) == 0 {
		return models.ChatResponse{Answer: fmt.Sprintf("No venues found for query '%s'.", query), Steps: steps}, true, nil
	}

	limit := 10
	lines := make([]string, 0, limit+1)
	if isSearch {
		lines = append(lines, fmt.Sprintf("Venue search results for '%s':", query))
	} else {
		lines = append(lines, "Venues:")
	}
	for _, m := range rows {
		if len(lines)-1 >= limit {
			break
		}
		name, _ := m["name"].(string)
		idNum := 0
		switch v := m["id"].(type) {
		case float64:
			idNum = int(v)
		case int:
			idNum = v
		}
		name = strings.TrimSpace(name)
		if name == "" || idNum <= 0 {
			continue
		}
		lines = append(lines, fmt.Sprintf("- %s (%d)", name, idNum))
	}
	answer := strings.Join(lines, "\n")
	if onToken != nil {
		onToken(answer)
	}
	return models.ChatResponse{Answer: answer, Steps: steps}, true, nil
}

func (c *ChatService) handleVenueDevices(ctx context.Context, req models.ChatRequest, onToken func(string)) (models.ChatResponse, bool, error) {
	msgLower := strings.ToLower(req.Message)
	// "venues for <device>" should be handled by handleDeviceVenues.
	if strings.Contains(msgLower, "venues for") {
		return models.ChatResponse{}, false, nil
	}
	if !(strings.Contains(msgLower, "venue") || strings.Contains(msgLower, "venues")) {
		return models.ChatResponse{}, false, nil
	}
	if !(strings.Contains(msgLower, "device") || strings.Contains(msgLower, "devices")) {
		return models.ChatResponse{}, false, nil
	}
	if !(strings.Contains(msgLower, "in") || strings.Contains(msgLower, "for") || strings.Contains(msgLower, "from") || strings.Contains(msgLower, "show")) {
		return models.ChatResponse{}, false, nil
	}
	if c.Gateway == nil {
		return models.ChatResponse{Answer: "Tool gateway is not configured."}, true, nil
	}
	conversationID := strings.TrimSpace(req.ConversationID)
	venueID := extractFirstInt(req.Message)
	var resolveStep *models.Step
	if venueID <= 0 {
		// Try resolving venue name.
		lookup := extractAfterKeyword(msgLower, "venue")
		if lookup == "" {
			lookup = extractAfterKeyword(msgLower, "venues")
		}
		if lookup == "" && (strings.Contains(msgLower, "from") || strings.Contains(msgLower, "in")) {
			if idx := strings.LastIndex(msgLower, "from"); idx >= 0 {
				lookup = strings.TrimSpace(msgLower[idx+4:])
			} else if idx := strings.LastIndex(msgLower, "in"); idx >= 0 {
				lookup = strings.TrimSpace(msgLower[idx+2:])
			}
		}
		if lookup != "" {
			if id, stp := c.resolveVenueIDFromName(ctx, conversationID, lookup); id > 0 {
				venueID = id
				resolveStep = stp
			}
		}
	}
	if venueID <= 0 && conversationID != "" {
		if st := c.getConversationState(conversationID); st != nil {
			venueID = st.VenueID
		}
	}
	if venueID <= 0 {
		return models.ChatResponse{Answer: "Please provide a venue id (number) or name. Example: show devices in venue Union Station."}, true, nil
	}
	if conversationID != "" {
		c.updateConversationVenueID(conversationID, venueID)
		c.clearPending(conversationID)
	}

	path := fmt.Sprintf("/ads/venues/%d/devices?page=1&page_size=20", venueID)
	status, body, err := c.Gateway.Get(path)
	step := models.Step{Tool: "adsVenueDevices", Status: status}
	if err != nil {
		step.Error = err.Error()
	} else {
		step.Body = clipString(strings.TrimSpace(string(body)), 2000)
	}
	steps := []models.Step{step}
	if resolveStep != nil {
		steps = append([]models.Step{*resolveStep}, steps...)
	}
	if err != nil {
		return models.ChatResponse{Answer: "Failed to fetch venue devices: " + err.Error(), Steps: steps}, true, nil
	}
	if status < 200 || status >= 300 {
		return models.ChatResponse{Answer: fmt.Sprintf("Failed to fetch venue devices (status %d).", status), Steps: steps}, true, nil
	}
	var parsed map[string]any
	if json.Unmarshal(body, &parsed) != nil {
		return models.ChatResponse{Answer: "Venue devices response could not be parsed.", Steps: steps}, true, nil
	}
	rowsAny, _ := parsed["data"].([]any)
	if len(rowsAny) == 0 {
		return models.ChatResponse{Answer: fmt.Sprintf("No devices found for venue %d.", venueID), Steps: steps}, true, nil
	}
	lines := []string{fmt.Sprintf("Devices in venue %d:", venueID)}
	for _, it := range rowsAny {
		if len(lines)-1 >= 10 {
			break
		}
		m, ok := it.(map[string]any)
		if !ok {
			continue
		}
		nm, _ := m["name"].(string)
		hn, _ := m["host_name"].(string)
		nm = strings.TrimSpace(nm)
		hn = strings.TrimSpace(hn)
		if nm == "" {
			nm = hn
		}
		if nm == "" {
			continue
		}
		if hn != "" {
			lines = append(lines, fmt.Sprintf("- %s (%s)", nm, hn))
		} else {
			lines = append(lines, "- "+nm)
		}
	}
	answer := strings.Join(lines, "\n")
	if onToken != nil {
		onToken(answer)
	}
	return models.ChatResponse{Answer: answer, Steps: steps}, true, nil
}

func (c *ChatService) handleDeviceVenues(ctx context.Context, req models.ChatRequest, onToken func(string)) (models.ChatResponse, bool, error) {
	msgLower := strings.ToLower(req.Message)
	if !(strings.Contains(msgLower, "venue") || strings.Contains(msgLower, "venues")) {
		return models.ChatResponse{}, false, nil
	}
	// Allow host-only phrasing like "show venues for moco-brt-...".
	if !(strings.Contains(msgLower, "device") || strings.Contains(msgLower, "kiosk") || strings.Contains(msgLower, "server") || len(detectHostTokens(req.Message)) > 0) {
		return models.ChatResponse{}, false, nil
	}
	if !(strings.Contains(msgLower, "for") || strings.Contains(msgLower, "of") || strings.Contains(msgLower, "show") || strings.Contains(msgLower, "list")) {
		return models.ChatResponse{}, false, nil
	}
	if c.Gateway == nil {
		return models.ChatResponse{Answer: "Tool gateway is not configured."}, true, nil
	}
	conversationID := strings.TrimSpace(req.ConversationID)
	deviceID := extractExplicitDeviceID(msgLower)
	resolvedHost := ""
	var resolveStep *models.Step
	if deviceID <= 0 {
		// Try host token or last host, then resolve display name.
		if tokens := detectHostTokens(req.Message); len(tokens) > 0 {
			candidate := strings.ToLower(strings.TrimSpace(tokens[0]))
			parts := strings.Split(strings.ReplaceAll(candidate, "_", "-"), "-")
			if len(parts) >= 3 {
				resolvedHost = candidate
			}
		}
		if resolvedHost == "" && conversationID != "" {
			if st := c.getConversationState(conversationID); st != nil {
				if strings.TrimSpace(st.Host) != "" {
					resolvedHost = strings.ToLower(strings.TrimSpace(st.Host))
				}
			}
		}
		if resolvedHost == "" {
			lookup := extractAfterKeyword(msgLower, "device")
			if lookup == "" {
				lookup = extractAfterKeyword(msgLower, "kiosk")
			}
			lookup = strings.TrimSpace(lookup)
			if lookup == "" {
				lookup = req.Message
			}
			if h, stp := c.resolveHostFromDeviceName(ctx, conversationID, lookup); strings.TrimSpace(h) != "" {
				resolvedHost = strings.ToLower(strings.TrimSpace(h))
				resolveStep = stp
			}
		}
		if resolvedHost != "" {
			// Resolve host -> device id via adsDevice.
			statusD, bodyD, errD := c.Gateway.Get("/ads/devices/" + url.PathEscape(resolvedHost))
			stepD := models.Step{Tool: "adsDevice", Status: statusD}
			if errD != nil {
				stepD.Error = errD.Error()
			} else {
				stepD.Body = clipString(strings.TrimSpace(string(bodyD)), 2000)
			}
			if resolveStep != nil {
				// Return this as part of steps later.
			}
			if errD == nil && statusD >= 200 && statusD < 300 {
				var parsed map[string]any
				if json.Unmarshal(bodyD, &parsed) == nil {
					obj := parsed
					if d, ok := parsed["data"].(map[string]any); ok {
						obj = d
					}
					switch v := obj["id"].(type) {
					case float64:
						deviceID = int(v)
					case int:
						deviceID = v
					}
				}
			}
			// Stash host in conversation for follow-ups.
			if conversationID != "" && resolvedHost != "" {
				c.updateConversationHost(conversationID, resolvedHost)
			}
			// If we couldn't determine device ID, surface device lookup step.
			if deviceID <= 0 {
				steps := []models.Step{}
				if resolveStep != nil {
					steps = append(steps, *resolveStep)
				}
				steps = append(steps, stepD)
				return models.ChatResponse{Answer: "Couldn't resolve device id for that host. Please provide a numeric device id.", Steps: steps}, true, nil
			}
		}
	}

	if deviceID <= 0 {
		return models.ChatResponse{Answer: "Please provide a numeric device id (or a host name) to list its venues."}, true, nil
	}
	if conversationID != "" {
		c.clearPending(conversationID)
	}

	path := fmt.Sprintf("/ads/devices/%d/venues?page=1&page_size=20", deviceID)
	status, body, err := c.Gateway.Get(path)
	step := models.Step{Tool: "adsDeviceVenues", Status: status}
	if err != nil {
		step.Error = err.Error()
	} else {
		step.Body = clipString(strings.TrimSpace(string(body)), 2000)
	}
	steps := []models.Step{step}
	if resolveStep != nil {
		steps = append([]models.Step{*resolveStep}, steps...)
	}
	if err != nil {
		return models.ChatResponse{Answer: "Failed to fetch device venues: " + err.Error(), Steps: steps}, true, nil
	}
	// Fallback: some deployments treat the path parameter as host_name/server_id instead of numeric device ID.
	// If the numeric endpoint fails server-side and we have a host, try the host-based variant.
	if status >= 500 && strings.TrimSpace(resolvedHost) != "" {
		hostPath := "/ads/devices/" + url.PathEscape(strings.TrimSpace(resolvedHost)) + "/venues?page=1&page_size=20"
		statusH, bodyH, errH := c.Gateway.Get(hostPath)
		stepH := models.Step{Tool: "adsDeviceVenuesByHost", Status: statusH}
		if errH != nil {
			stepH.Error = errH.Error()
		} else {
			stepH.Body = clipString(strings.TrimSpace(string(bodyH)), 2000)
		}
		steps = append(steps, stepH)
		if errH == nil && statusH >= 200 && statusH < 300 {
			// Use host-based response.
			body = bodyH
			status = statusH
		}
	}
	if status < 200 || status >= 300 {
		// Known backend issue: tool-gateway may return 500 due to a SQL scan mismatch.
		if status >= 500 {
			b := strings.TrimSpace(string(body))
			if strings.Contains(b, "Failed to list venues by device") || strings.Contains(b, "expected") {
				answer := "The venues-by-device endpoint is currently failing server-side (500). " +
					"Workaround: use `list venues` to find a venue id, then `show devices in venue <id>` to see membership."
				return models.ChatResponse{Answer: answer, Steps: steps}, true, nil
			}
		}
		return models.ChatResponse{Answer: fmt.Sprintf("Failed to fetch device venues (status %d).", status), Steps: steps}, true, nil
	}
	var parsed map[string]any
	if json.Unmarshal(body, &parsed) != nil {
		return models.ChatResponse{Answer: "Device venues response could not be parsed.", Steps: steps}, true, nil
	}
	rowsAny, _ := parsed["data"].([]any)
	if len(rowsAny) == 0 {
		return models.ChatResponse{Answer: fmt.Sprintf("No venues found for device %d.", deviceID), Steps: steps}, true, nil
	}
	header := fmt.Sprintf("Venues for device %d:", deviceID)
	if strings.TrimSpace(resolvedHost) != "" {
		header = fmt.Sprintf("Venues for %s (device %d):", strings.TrimSpace(resolvedHost), deviceID)
	}
	lines := []string{header}
	for _, it := range rowsAny {
		if len(lines)-1 >= 10 {
			break
		}
		m, ok := it.(map[string]any)
		if !ok {
			continue
		}
		name, _ := m["name"].(string)
		idNum := 0
		switch v := m["id"].(type) {
		case float64:
			idNum = int(v)
		case int:
			idNum = v
		}
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if idNum > 0 {
			lines = append(lines, fmt.Sprintf("- %s (%d)", name, idNum))
		} else {
			lines = append(lines, "- "+name)
		}
	}
	answer := strings.Join(lines, "\n")
	if onToken != nil {
		onToken(answer)
	}
	return models.ChatResponse{Answer: answer, Steps: steps}, true, nil
}

func (c *ChatService) handleDeviceDetails(ctx context.Context, req models.ChatRequest, onToken func(string)) (models.ChatResponse, bool, error) {
	msgLower := strings.ToLower(req.Message)
	if !(strings.Contains(msgLower, "device") || strings.Contains(msgLower, "kiosk") || strings.Contains(msgLower, "server")) {
		return models.ChatResponse{}, false, nil
	}
	// If the user is asking for kiosk-wise analytics in a conversation that already has a poster context,
	// let the poster POP/play-count handlers handle it instead of returning device details.
	if strings.Contains(msgLower, "kiosk wise") || strings.Contains(msgLower, "kiosk-wise") || strings.Contains(msgLower, "kioskwise") || strings.Contains(msgLower, "kiosks wise") || strings.Contains(msgLower, "kiosks-wise") {
		conversationID := strings.TrimSpace(req.ConversationID)
		if conversationID != "" {
			if st := c.getConversationState(conversationID); st != nil {
				if strings.TrimSpace(st.PosterID) != "" || strings.TrimSpace(st.PosterName) != "" {
					return models.ChatResponse{}, false, nil
				}
			}
		}
	}
	// POP questions should be handled by POP handlers.
	if strings.Contains(msgLower, "pop") {
		return models.ChatResponse{}, false, nil
	}
	// Venue-related queries should be handled by venue-specific handlers.
	if strings.Contains(msgLower, "venue") || strings.Contains(msgLower, "venues") {
		return models.ChatResponse{}, false, nil
	}
	// Avoid colliding with telemetry handler.
	if strings.Contains(msgLower, "metric") || strings.Contains(msgLower, "telemetry") || strings.Contains(msgLower, "internet") || strings.Contains(msgLower, "usage") {
		return models.ChatResponse{}, false, nil
	}
	if !(strings.Contains(msgLower, "detail") || strings.Contains(msgLower, "info") || strings.Contains(msgLower, "show") || strings.Contains(msgLower, "get")) {
		return models.ChatResponse{}, false, nil
	}
	if c.Gateway == nil {
		return models.ChatResponse{Answer: "Tool gateway is not configured."}, true, nil
	}

	conversationID := strings.TrimSpace(req.ConversationID)
	host := ""
	if tokens := detectHostTokens(req.Message); len(tokens) > 0 {
		candidate := strings.ToLower(strings.TrimSpace(tokens[0]))
		parts := strings.Split(strings.ReplaceAll(candidate, "_", "-"), "-")
		if len(parts) >= 3 {
			host = candidate
		}
	}
	var resolveStep *models.Step
	resolveNameCandidate := func(msg string) string {
		cleaned := msg
		// Strip common command words but keep meaningful display-name tokens like "Kiosk-1".
		for _, w := range []string{"show", "get", "give", "tell", "me", "the", "a", "an", "please", "device", "devices", "kiosk", "kiosks", "server", "info", "detail", "details"} {
			cleaned = strings.ReplaceAll(cleaned, w, "")
			cleaned = strings.ReplaceAll(cleaned, strings.Title(w), "")
		}
		cleaned = strings.ReplaceAll(cleaned, "?", " ")
		cleaned = strings.ReplaceAll(cleaned, ":", " ")
		cleaned = strings.TrimSpace(cleaned)
		cleaned = strings.Join(strings.Fields(cleaned), " ")
		return cleaned
	}
	if host == "" && conversationID != "" {
		// Only reuse the remembered host for *generic* follow-ups.
		// If the user provided a kiosk/display name, resolve fresh instead of inheriting a stale host.
		candidate := resolveNameCandidate(req.Message)
		if strings.TrimSpace(candidate) == "" {
			if st := c.getConversationState(conversationID); st != nil {
				if strings.TrimSpace(st.Host) != "" {
					host = strings.ToLower(strings.TrimSpace(st.Host))
				}
			}
		}
	}
	if host == "" {
		candidate := resolveNameCandidate(req.Message)
		query := req.Message
		resolveConversationID := conversationID
		if candidate != "" {
			query = candidate
			// For explicit kiosk/display-name queries, don't constrain resolution by remembered city/region.
			resolveConversationID = ""
		}
		if resolved, step := c.resolveHostFromDeviceName(ctx, resolveConversationID, query); strings.TrimSpace(resolved) != "" {
			host = strings.ToLower(strings.TrimSpace(resolved))
			resolveStep = step
		}
	}
	if host == "" {
		// Second pass: if the first pass failed, try resolving with just the name parts.
		// Sometimes the search query is too specific with "device info" or "show".
		cleaned := req.Message
		cleaned = strings.ReplaceAll(cleaned, "show", "")
		cleaned = strings.ReplaceAll(cleaned, "device", "")
		cleaned = strings.ReplaceAll(cleaned, "info", "")
		cleaned = strings.ReplaceAll(cleaned, "detail", "")
		cleaned = strings.TrimSpace(cleaned)
		if cleaned != "" && cleaned != req.Message {
			// Second pass is also name-driven; avoid stale geo constraints.
			if resolved, step := c.resolveHostFromDeviceName(ctx, "", cleaned); strings.TrimSpace(resolved) != "" {
				host = strings.ToLower(strings.TrimSpace(resolved))
				resolveStep = step
			}
		}
	}
	if host == "" {
		return models.ChatResponse{Answer: "Please specify a device/kiosk host (for example: moco-brt-briggs-001) or a kiosk display name."}, true, nil
	}
	if conversationID != "" {
		c.updateConversationHost(conversationID, host)
		c.clearPending(conversationID)
	}

	path := "/ads/devices/" + url.PathEscape(host)
	status, body, err := c.Gateway.Get(path)
	step := models.Step{Tool: "adsDevice", Status: status}
	if err != nil {
		step.Error = err.Error()
	} else {
		step.Body = clipString(strings.TrimSpace(string(body)), 2000)
	}
	steps := []models.Step{step}
	if resolveStep != nil {
		steps = append([]models.Step{*resolveStep}, steps...)
	}
	if err != nil {
		return models.ChatResponse{Answer: "Failed to fetch device details: " + err.Error(), Steps: steps}, true, nil
	}
	if status < 200 || status >= 300 {
		return models.ChatResponse{Answer: fmt.Sprintf("Failed to fetch device details (status %d).", status), Steps: steps}, true, nil
	}

	var parsed map[string]any
	if json.Unmarshal(body, &parsed) != nil {
		return models.ChatResponse{Answer: "Device details response could not be parsed.", Steps: steps}, true, nil
	}
	// Accept both {data:{...}} and direct object.
	obj := parsed
	if d, ok := parsed["data"].(map[string]any); ok {
		obj = d
	}
	name, _ := obj["name"].(string)
	desc, _ := obj["description"].(string)
	hostName, _ := obj["host_name"].(string)
	if strings.TrimSpace(hostName) == "" {
		hostName = host
	}
	deviceID := 0
	switch v := obj["id"].(type) {
	case float64:
		deviceID = int(v)
	case int:
		deviceID = v
	}
	city := ""
	region := ""
	if cfg, ok := obj["device_config"].(map[string]any); ok {
		if s, ok := cfg["city"].(string); ok {
			city = s
		}
	}
	if regObj, ok := obj["region"].(map[string]any); ok {
		if s, ok := regObj["code"].(string); ok {
			region = s
		}
	}

	lines := []string{
		fmt.Sprintf("Device details for %s:", strings.TrimSpace(hostName)),
	}
	lines = append(lines, "Host: "+strings.TrimSpace(hostName))
	if deviceID > 0 {
		lines = append(lines, fmt.Sprintf("Device ID: %d", deviceID))
	}
	if strings.TrimSpace(name) != "" {
		lines = append(lines, "Name: "+strings.TrimSpace(name))
	}
	if strings.TrimSpace(city) != "" {
		lines = append(lines, "City: "+strings.TrimSpace(city))
	}
	if strings.TrimSpace(region) != "" {
		lines = append(lines, "Region: "+strings.TrimSpace(region))
	}
	if strings.TrimSpace(desc) != "" {
		lines = append(lines, "Description: "+strings.TrimSpace(desc))
	}
	answer := strings.Join(lines, "\n")
	if onToken != nil {
		onToken(answer)
	}
	return models.ChatResponse{Answer: answer, Steps: steps}, true, nil
}

func (c *ChatService) handleAdvertiserSearchList(ctx context.Context, req models.ChatRequest, onToken func(string)) (models.ChatResponse, bool, error) {
	msgLower := strings.ToLower(req.Message)
	if !(strings.Contains(msgLower, "advertiser") || strings.Contains(msgLower, "advertisers")) {
		return models.ChatResponse{}, false, nil
	}
	if c.Gateway == nil {
		return models.ChatResponse{Answer: "Tool gateway is not configured."}, true, nil
	}

	isSearch := strings.Contains(msgLower, "search") || strings.Contains(msgLower, "find")
	isList := strings.Contains(msgLower, "list") || strings.Contains(msgLower, "show") || strings.Contains(msgLower, "all")
	if !(isSearch || isList) {
		return models.ChatResponse{}, false, nil
	}

	query := ""
	if isSearch {
		query = extractAfterKeyword(msgLower, "advertisers")
		if query == "" {
			query = extractAfterKeyword(msgLower, "advertiser")
		}
		query = strings.TrimSpace(query)
		if query == "" {
			return models.ChatResponse{Answer: "Please provide an advertiser search query (for example: search advertisers Pepsi)."}, true, nil
		}
	}

	status, body, err := c.Gateway.Get("/ads/advertisers")
	step := models.Step{Tool: "adsAdvertisers", Status: status}
	if err != nil {
		step.Error = err.Error()
	} else {
		step.Body = clipString(strings.TrimSpace(string(body)), 2000)
	}
	steps := []models.Step{step}
	if err != nil {
		return models.ChatResponse{Answer: "Failed to fetch advertisers: " + err.Error(), Steps: steps}, true, nil
	}
	if status < 200 || status >= 300 {
		return models.ChatResponse{Answer: fmt.Sprintf("Failed to fetch advertisers (status %d).", status), Steps: steps}, true, nil
	}
	var parsed map[string]any
	if json.Unmarshal(body, &parsed) != nil {
		return models.ChatResponse{Answer: "Advertisers response could not be parsed.", Steps: steps}, true, nil
	}
	rowsAny, _ := parsed["data"].([]any)
	if len(rowsAny) == 0 {
		return models.ChatResponse{Answer: "No advertisers found.", Steps: steps}, true, nil
	}

	// Filter for search.
	rows := make([]map[string]any, 0, len(rowsAny))
	qLower := strings.ToLower(query)
	for _, it := range rowsAny {
		m, ok := it.(map[string]any)
		if !ok {
			continue
		}
		name, _ := m["name"].(string)
		id, _ := m["id"].(string)
		name = strings.TrimSpace(name)
		id = strings.TrimSpace(id)
		if name == "" || id == "" {
			continue
		}
		if isSearch {
			if !strings.Contains(strings.ToLower(name), qLower) {
				continue
			}
		}
		rows = append(rows, m)
	}

	if len(rows) == 0 {
		return models.ChatResponse{Answer: fmt.Sprintf("No advertisers found for query '%s'.", query), Steps: steps}, true, nil
	}

	limit := 10
	lines := make([]string, 0, limit+1)
	if isSearch {
		lines = append(lines, fmt.Sprintf("Advertiser search results for '%s':", query))
	} else {
		lines = append(lines, "Advertisers:")
	}
	for _, m := range rows {
		if len(lines)-1 >= limit {
			break
		}
		name, _ := m["name"].(string)
		id, _ := m["id"].(string)
		name = strings.TrimSpace(name)
		id = strings.TrimSpace(id)
		if name == "" || id == "" {
			continue
		}
		lines = append(lines, fmt.Sprintf("- %s (%s)", name, id))
	}
	answer := strings.Join(lines, "\n")
	if onToken != nil {
		onToken(answer)
	}
	return models.ChatResponse{Answer: answer, Steps: steps}, true, nil
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
			args.Query = c.normalizePopQueryLocation(ctx, path, args.Query)

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

	convMu    sync.Mutex
	convState map[string]*conversationState

	cityMu       sync.Mutex
	cityCache    map[string]struct{}
	cityCacheAt  time.Time
	cityCacheTTL time.Duration

	regionCache   map[string]struct{}
	regionCacheAt time.Time
	regionToCity  map[string]string

	projectMu           sync.Mutex
	projectCityCache    map[string]struct{}
	projectLookups      []projectLookup
	projectCityCacheAt  time.Time
	projectCacheTTL     time.Duration

	projectMappingsOnce sync.Once
}

type conversationState struct {
	City           string
	Region         string
	Host           string
	PosterName     string
	PosterID       string
	PosterCity     string
	PosterRegion   string
	CampaignID     string
	VenueID        int
	PendingHandler string
	PendingMessage string
	UpdatedAt      time.Time
}

func (c *ChatService) ensureConversationStateHydrated(ctx context.Context, ownerKey, conversationID string) {
	id := strings.TrimSpace(conversationID)
	if id == "" {
		return
	}
	st := c.getConversationState(id)
	if st == nil {
		return
	}
	// If we already have any useful state, don't re-hydrate.
	if strings.TrimSpace(st.City) != "" || strings.TrimSpace(st.Region) != "" || strings.TrimSpace(st.Host) != "" || strings.TrimSpace(st.PosterName) != "" || strings.TrimSpace(st.PosterID) != "" || strings.TrimSpace(st.PosterCity) != "" || strings.TrimSpace(st.PosterRegion) != "" {
		return
	}
	if c.Store == nil {
		return
	}
	msgs, err := c.Store.ListMessages(ctx, ownerKey, id, 50)
	if err != nil || len(msgs) == 0 {
		return
	}

	// Prefer newest hints, but preserve ordering for incremental inference.
	// We scan from oldest->newest so later messages can overwrite earlier guesses.
	city := ""
	region := ""
	host := ""

	// Patterns like "city 'moco'" and "region 'brt'" appear in deterministic answers.
	cityRe := regexp.MustCompile(`(?i)city\s+'([a-z0-9_-]+)'`)
	regionRe := regexp.MustCompile(`(?i)region\s+'([a-z0-9_-]+)'`)

	campaignID := ""
	venueID := 0
	posterName := ""
	posterID := ""
	posterCity := ""
	posterRegion := ""

	posterRe := regexp.MustCompile(`(?i)poster\s+'([^']+)'`)
	posterCityRe := regexp.MustCompile(`(?i)poster\s+city\s+'([a-z0-9_-]+)'`)
	posterRegionRe := regexp.MustCompile(`(?i)poster\s+region\s+'([a-z0-9_-]+)'`)

	for _, m := range msgs {
		content := strings.TrimSpace(m.Content)
		if content == "" {
			continue
		}
		lower := strings.ToLower(content)

		// Best-effort: infer poster memory from user questions like:
		// "play count of poster Lorla Studio from brt region".
		if strings.Contains(lower, "poster") && (strings.Contains(lower, "play count") || strings.Contains(lower, "plays")) {
			if posterName == "" {
				pn := strings.TrimSpace(extractAfterKeywordOriginal(content, "poster"))
				pnLower := strings.ToLower(pn)
				if strings.Contains(pnLower, " from ") {
					pn = strings.TrimSpace(strings.SplitN(pn, " from ", 2)[0])
					pnLower = strings.ToLower(pn)
				}
				if strings.Contains(pnLower, " in ") {
					pn = strings.TrimSpace(strings.SplitN(pn, " in ", 2)[0])
					pnLower = strings.ToLower(pn)
				}
				if strings.Contains(pnLower, " for ") {
					pn = strings.TrimSpace(strings.SplitN(pn, " for ", 2)[0])
					pnLower = strings.ToLower(pn)
				}
				if strings.TrimSpace(pn) != "" {
					posterName = strings.TrimSpace(pn)
				}
			}
			if posterCity == "" {
				if cty := c.detectCityCode(ctx, lower); cty != "" {
					posterCity = strings.ToLower(strings.TrimSpace(cty))
				}
			}
			if posterRegion == "" {
				if reg := c.detectRegionCode(ctx, lower); reg != "" {
					posterRegion = strings.ToLower(strings.TrimSpace(reg))
				}
			}
		}

		// Best-effort: infer poster id from messages like:
		// "POP for poster <name> (<uuid>) ..." or user messages containing a UUID.
		if posterID == "" {
			if strings.Contains(lower, "poster") {
				if id := extractCampaignID(content); looksLikeUUID(id) {
					posterID = id
				}
			}
		}

		if mm := posterRe.FindStringSubmatch(content); len(mm) == 2 {
			if strings.TrimSpace(mm[1]) != "" {
				posterName = strings.TrimSpace(mm[1])
			}
		}
		if mm := posterCityRe.FindStringSubmatch(content); len(mm) == 2 {
			if strings.TrimSpace(mm[1]) != "" {
				posterCity = strings.ToLower(strings.TrimSpace(mm[1]))
			}
		}
		if mm := posterRegionRe.FindStringSubmatch(content); len(mm) == 2 {
			if strings.TrimSpace(mm[1]) != "" {
				posterRegion = strings.ToLower(strings.TrimSpace(mm[1]))
			}
		}

		if id := extractCampaignID(content); looksLikeUUID(id) {
			campaignID = id
		}
		if vid := extractFirstInt(content); vid > 0 {
			// Best-effort: only treat as venue id if message mentions venue.
			if strings.Contains(lower, "venue") {
				venueID = vid
			}
		}

		if h := detectHostTokens(content); len(h) > 0 {
			candidate := strings.ToLower(strings.TrimSpace(h[0]))
			parts := strings.Split(strings.ReplaceAll(candidate, "_", "-"), "-")
			if len(parts) >= 3 {
				host = candidate
				// Also infer city/region from host prefix.
				if parts[0] != "" {
					city = parts[0]
				}
				if parts[1] != "" {
					region = parts[1]
				}
			}
		}

		if cty := c.detectCityCode(ctx, lower); cty != "" {
			city = cty
		}
		if reg := c.detectRegionCode(ctx, lower); reg != "" {
			region = reg
		}

		if mm := cityRe.FindStringSubmatch(content); len(mm) == 2 {
			city = strings.ToLower(strings.TrimSpace(mm[1]))
		}
		if mm := regionRe.FindStringSubmatch(content); len(mm) == 2 {
			region = strings.ToLower(strings.TrimSpace(mm[1]))
		}
	}

	if strings.TrimSpace(city) != "" {
		st.City = strings.ToLower(strings.TrimSpace(city))
	}
	if strings.TrimSpace(region) != "" {
		st.Region = strings.ToLower(strings.TrimSpace(region))
	}
	if strings.TrimSpace(host) != "" {
		st.Host = strings.ToLower(strings.TrimSpace(host))
	}
	if strings.TrimSpace(posterName) != "" {
		st.PosterName = strings.TrimSpace(posterName)
	}
	if looksLikeUUID(posterID) {
		st.PosterID = posterID
	}
	if strings.TrimSpace(posterCity) != "" {
		st.PosterCity = strings.ToLower(strings.TrimSpace(posterCity))
	}
	if strings.TrimSpace(posterRegion) != "" {
		st.PosterRegion = strings.ToLower(strings.TrimSpace(posterRegion))
	}
	if looksLikeUUID(campaignID) {
		st.CampaignID = campaignID
	}
	if venueID > 0 {
		st.VenueID = venueID
	}
	st.UpdatedAt = time.Now()
}

func (c *ChatService) updateConversationPoster(conversationID, posterName, city, region string) {
	id := strings.TrimSpace(conversationID)
	if id == "" {
		return
	}
	st := c.getConversationState(id)
	if st == nil {
		return
	}
	p := strings.TrimSpace(posterName)
	if p != "" {
		st.PosterName = p
	}
	if strings.TrimSpace(city) != "" {
		st.PosterCity = strings.ToLower(strings.TrimSpace(city))
	}
	if strings.TrimSpace(region) != "" {
		st.PosterRegion = strings.ToLower(strings.TrimSpace(region))
	}
	st.UpdatedAt = time.Now()
}

func (c *ChatService) updateConversationPosterID(conversationID, posterID string) {
	st := c.getConversationState(conversationID)
	if st == nil {
		return
	}
	if looksLikeUUID(posterID) {
		st.PosterID = posterID
		st.UpdatedAt = time.Now()
	}
}

func (c *ChatService) handlePosterPlayCount(ctx context.Context, req models.ChatRequest, onToken func(string)) (models.ChatResponse, bool, error) {
	msgLower := strings.ToLower(req.Message)
	conversationID := strings.TrimSpace(req.ConversationID)
	isKioskWise := strings.Contains(msgLower, "kiosk wise") || strings.Contains(msgLower, "kiosk-wise") || strings.Contains(msgLower, "kioskwise") || strings.Contains(msgLower, "kiosks wise") || strings.Contains(msgLower, "kiosks-wise") || strings.Contains(msgLower, "by kiosk") || strings.Contains(msgLower, "by kiosks")
	isSameFollowup := isKioskWise && (strings.Contains(msgLower, "same") || strings.Contains(msgLower, "same as") || strings.Contains(msgLower, "same kiosk"))
	isWholeKioskWiseFollowup := false
	hasPlayCount := strings.Contains(msgLower, "play count") || strings.Contains(msgLower, "plays")
	hasPosterWord := strings.Contains(msgLower, "poster")
	hasAdWord := strings.Contains(msgLower, " ad ") || strings.HasSuffix(strings.TrimSpace(msgLower), " ad") || strings.Contains(msgLower, " creative ") || strings.HasSuffix(strings.TrimSpace(msgLower), " creative")

	if isKioskWise && !hasPosterWord && !hasPlayCount {
		// Follow-up like: "get me whole kiosks wise data" after a poster-specific query.
		// Only treat it as poster analytics if conversation memory already has poster context.
		if (strings.Contains(msgLower, "whole") || strings.Contains(msgLower, "all") || strings.Contains(msgLower, "overall") || strings.Contains(msgLower, "entire") || strings.Contains(msgLower, "data")) && conversationID != "" {
			if st := c.getConversationState(conversationID); st != nil {
				if strings.TrimSpace(st.PosterID) != "" || strings.TrimSpace(st.PosterName) != "" {
					isWholeKioskWiseFollowup = true
				}
			}
		}
		if isWholeKioskWiseFollowup {
			hasPlayCount = true
		}
	}

	// Explicit query: requires poster + play count.
	// Follow-up query: allow "same kiosk wise" (or "whole kiosks wise data") to reuse poster + scope from conversation memory.
	if !hasPosterWord && !isSameFollowup && !isWholeKioskWiseFollowup && !(hasPlayCount && hasAdWord) {
		return models.ChatResponse{}, false, nil
	}
	// Allow ad/creative phrasing: "play count of <ad name> ..." (no "poster" keyword).
	if !hasPlayCount && !isSameFollowup && !isWholeKioskWiseFollowup {
		return models.ChatResponse{}, false, nil
	}
	if c.Gateway == nil {
		return models.ChatResponse{Answer: "Tool gateway is not configured."}, true, nil
	}
	if isSameFollowup {
		isKioskWise = true
	}
	if isWholeKioskWiseFollowup {
		isKioskWise = true
	}
	if !hasPosterWord {
		// If the user didn't say "poster", still treat it as a poster play-count query when
		// they used "play count" and mention an ad/creative.
		if !isSameFollowup && !isWholeKioskWiseFollowup && !(hasPlayCount && hasAdWord) {
			return models.ChatResponse{}, false, nil
		}
	}

	// Extract poster name (preserve original casing for poster_name query).
	posterName := ""
	if strings.Contains(msgLower, " posters") || strings.HasPrefix(msgLower, "posters ") || strings.Contains(msgLower, " posters ") {
		low := strings.ToLower(req.Message)
		idx := strings.Index(low, " posters")
		if idx < 0 {
			idx = strings.Index(low, " posters ")
		}
		if idx >= 0 {
			posterName = strings.TrimSpace(req.Message[:idx])
		}
	} else if hasPosterWord {
		posterName = strings.TrimSpace(extractAfterKeywordOriginal(req.Message, "poster"))
	} else {
		// Handle: "play count of <name> ...".
		posterName = strings.TrimSpace(extractAfterKeywordOriginal(req.Message, "play count of"))
		if posterName == "" {
			posterName = strings.TrimSpace(extractAfterKeywordOriginal(req.Message, "plays of"))
		}
	}
	posterName = strings.TrimSpace(strings.TrimSuffix(posterName, "kiosk wise"))
	posterName = strings.TrimSpace(strings.TrimSuffix(posterName, "kiosk-wise"))
	posterNameLower := strings.ToLower(posterName)
	if strings.Contains(posterNameLower, " from ") {
		posterName = strings.TrimSpace(strings.SplitN(posterName, " from ", 2)[0])
		posterNameLower = strings.ToLower(posterName)
	}
	if strings.Contains(posterNameLower, " in ") {
		posterName = strings.TrimSpace(strings.SplitN(posterName, " in ", 2)[0])
		posterNameLower = strings.ToLower(posterName)
	}
	if strings.Contains(posterNameLower, " for ") {
		posterName = strings.TrimSpace(strings.SplitN(posterName, " for ", 2)[0])
		posterNameLower = strings.ToLower(posterName)
	}
	// Strip a trailing "ad"/"creative" token from the extracted name.
	posterNameLower = strings.ToLower(strings.TrimSpace(posterName))
	if strings.HasSuffix(posterNameLower, " ad") {
		posterName = strings.TrimSpace(posterName[:len(posterName)-len(" ad")])
	}
	posterNameLower = strings.ToLower(strings.TrimSpace(posterName))
	if strings.HasSuffix(posterNameLower, " creative") {
		posterName = strings.TrimSpace(posterName[:len(posterName)-len(" creative")])
	}
	posterIDFromMem := ""
	if conversationID != "" {
		if st := c.getConversationState(conversationID); st != nil {
			if strings.TrimSpace(st.PosterID) != "" {
				posterIDFromMem = strings.TrimSpace(st.PosterID)
			}
			if posterName == "" && strings.TrimSpace(st.PosterName) != "" {
				posterName = strings.TrimSpace(st.PosterName)
			}
		}
	}
	if strings.Contains(strings.ToLower(posterName), "interpreted request") || strings.Contains(posterName, "poster=") {
		posterName = ""
	}
	if posterName == "" {
		if looksLikeUUID(posterIDFromMem) {
			posterName = posterIDFromMem
		} else {
			return models.ChatResponse{Answer: "Please specify a poster name (for example: play count of poster Lorla Studio)."}, true, nil
		}
	}

	city := c.detectCityCode(ctx, msgLower)
	region := c.detectRegionCode(ctx, msgLower)
	// Reuse last poster scope for follow-ups like "same kiosk wise".
	if city == "" && region == "" && conversationID != "" {
		if st := c.getConversationState(conversationID); st != nil {
			if strings.TrimSpace(st.PosterRegion) != "" {
				region = strings.ToLower(strings.TrimSpace(st.PosterRegion))
			}
			if strings.TrimSpace(st.PosterCity) != "" {
				city = strings.ToLower(strings.TrimSpace(st.PosterCity))
			}
			// Fallback to general scope memory if poster-specific scope isn't available.
			if region == "" && strings.TrimSpace(st.Region) != "" {
				region = strings.ToLower(strings.TrimSpace(st.Region))
			}
			if city == "" && strings.TrimSpace(st.City) != "" {
				city = strings.ToLower(strings.TrimSpace(st.City))
			}
		}
	}
	if city == "" && region == "" {
		return models.ChatResponse{Answer: "Please specify a city or region code (for example: moco or brt)."}, true, nil
	}
	if conversationID != "" {
		c.updateConversationPoster(conversationID, posterName, city, region)
		c.clearPending(conversationID)
	}

	// Pull POP rows filtered by poster_name (or poster_id) + scope.
	type popItem struct {
		PosterName  string    `json:"poster_name"`
		PosterID    string    `json:"poster_id"`
		HostName    string    `json:"host_name"`
		KioskName   string    `json:"kiosk_name"`
		PosterType  string    `json:"poster_type"`
		PopDatetime time.Time `json:"pop_datetime"`
		City        string    `json:"city"`
		Region      string    `json:"region"`
		PlayCount   int64     `json:"play_count"`
	}
	type popListResponse struct {
		Items    []popItem `json:"items"`
		Total    int64     `json:"total"`
		Page     int       `json:"page"`
		PageSize int       `json:"page_size"`
	}

	fromRFC, toRFC := extractDateRangeRFC3339(msgLower)
	if fromRFC == "" && toRFC == "" {
		fromRFC, toRFC = extractNaturalDateRangeRFC3339(req.Message)
	}

	page := 1
	pageSize := 200
	maxPages := 10
	steps := make([]models.Step, 0, 2)
	items := make([]popItem, 0, 64)
	for {
		posterQueryKey := "poster_name"
		if looksLikeUUID(posterName) {
			posterQueryKey = "poster_id"
		}
		basePath := fmt.Sprintf("/pop?%s=%s&page=%d&page_size=%d", posterQueryKey, urlEscape(posterName), page, pageSize)
		if strings.TrimSpace(fromRFC) != "" && strings.TrimSpace(toRFC) != "" {
			basePath += "&from=" + urlEscape(fromRFC) + "&to=" + urlEscape(toRFC)
		}
		path := basePath
		if strings.TrimSpace(region) != "" {
			path += "&region=" + urlEscape(region)
		} else if strings.TrimSpace(city) != "" {
			path += "&city=" + urlEscape(city)
		}
		status, body, err := c.Gateway.Get(path)
		if err == nil && status == 400 && strings.TrimSpace(fromRFC) != "" && strings.TrimSpace(toRFC) != "" {
			pathNoDates := fmt.Sprintf("/pop?%s=%s&page=%d&page_size=%d", posterQueryKey, urlEscape(posterName), page, pageSize)
			if strings.TrimSpace(region) != "" {
				pathNoDates += "&region=" + urlEscape(region)
			} else if strings.TrimSpace(city) != "" {
				pathNoDates += "&city=" + urlEscape(city)
			}
			status2, body2, err2 := c.Gateway.Get(pathNoDates)
			step2 := models.Step{Tool: "popList", Status: status2}
			if err2 != nil {
				step2.Error = err2.Error()
			} else {
				step2.Body = clipString(strings.TrimSpace(string(body2)), 2000)
			}
			steps = append(steps, step2)
			status, body, err = status2, body2, err2
		}
		step := models.Step{Tool: "popList", Status: status}
		if err != nil {
			step.Error = err.Error()
		} else {
			step.Body = clipString(strings.TrimSpace(string(body)), 2000)
		}
		steps = append(steps, step)
		if err != nil {
			return models.ChatResponse{Answer: "Failed to fetch POP data: " + err.Error(), Steps: steps}, true, nil
		}
		if status < 200 || status >= 300 {
			return models.ChatResponse{Answer: fmt.Sprintf("Failed to fetch POP data (status %d).", status), Steps: steps}, true, nil
		}
		var resp popListResponse
		if json.Unmarshal(body, &resp) != nil {
			return models.ChatResponse{Answer: "POP list response could not be parsed.", Steps: steps}, true, nil
		}
		if len(resp.Items) == 0 {
			break
		}
		items = append(items, resp.Items...)
		if resp.Total > 0 {
			if int64(page*pageSize) >= resp.Total {
				break
			}
		} else if len(resp.Items) < pageSize {
			break
		}
		page++
		if page > maxPages {
			break
		}
	}
	if len(items) == 0 {
		scopeLabel := ""
		if strings.TrimSpace(region) != "" {
			scopeLabel = "region '" + strings.TrimSpace(region) + "'"
		} else {
			scopeLabel = "city '" + strings.TrimSpace(city) + "'"
		}
		answer := fmt.Sprintf("No play counts found for poster '%s' in %s.", posterName, scopeLabel)
		if onToken != nil {
			onToken(answer)
		}
		return models.ChatResponse{Answer: answer, Steps: steps}, true, nil
	}

	totalPlays := int64(0)
	for _, it := range items {
		totalPlays += it.PlayCount
	}

	scopeLabel := ""
	if strings.TrimSpace(region) != "" {
		scopeLabel = "region '" + strings.TrimSpace(region) + "'"
	} else {
		scopeLabel = "city '" + strings.TrimSpace(city) + "'"
	}

	if !isKioskWise {
		answer := fmt.Sprintf("Play count for poster '%s' in %s: %d plays.", posterName, scopeLabel, totalPlays)
		if onToken != nil {
			onToken(answer)
		}
		return models.ChatResponse{Answer: answer, Steps: steps}, true, nil
	}

	// Kiosk-wise aggregation.
	type kv struct {
		Key   string
		Plays int64
	}
	byKiosk := map[string]int64{}
	for _, it := range items {
		k := strings.TrimSpace(it.KioskName)
		if k == "" {
			k = strings.TrimSpace(it.HostName)
		}
		if k == "" {
			continue
		}
		byKiosk[k] += it.PlayCount
	}
	rows := make([]kv, 0, len(byKiosk))
	for k, v := range byKiosk {
		rows = append(rows, kv{Key: k, Plays: v})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Plays > rows[j].Plays })
	if len(rows) > 10 {
		rows = rows[:10]
	}
	lines := make([]string, 0, len(rows)+2)
	lines = append(lines, fmt.Sprintf("Play count for poster '%s' in %s: %d plays", posterName, scopeLabel, totalPlays))
	lines = append(lines, "Kiosk-wise:")
	for i, r := range rows {
		lines = append(lines, fmt.Sprintf("%d. %s — %d plays", i+1, r.Key, r.Plays))
	}
	answer := strings.Join(lines, "\n")
	if onToken != nil {
		onToken(answer)
	}
	return models.ChatResponse{Answer: answer, Steps: steps}, true, nil
}

func extractFirstInt(s string) int {
	re := regexp.MustCompile(`\b(\d{1,9})\b`)
	mm := re.FindStringSubmatch(s)
	if len(mm) != 2 {
		return 0
	}
	n, err := strconv.Atoi(mm[1])
	if err != nil {
		return 0
	}
	if n <= 0 {
		return 0
	}
	return n
}

func extractExplicitDeviceID(msgLower string) int {
	s := strings.ToLower(strings.TrimSpace(msgLower))
	if s == "" {
		return 0
	}
	// Only treat numbers as device IDs when explicitly provided as "device <id>" or "device id <id>".
	re := regexp.MustCompile(`(?i)\bdevice\s+(?:id\s+)?(\d{1,9})\b`)
	mm := re.FindStringSubmatch(s)
	if len(mm) != 2 {
		return 0
	}
	n, err := strconv.Atoi(mm[1])
	if err != nil || n <= 0 {
		return 0
	}
	return n
}

func (c *ChatService) updateConversationVenueID(conversationID string, venueID int) {
	id := strings.TrimSpace(conversationID)
	if id == "" || venueID <= 0 {
		return
	}
	st := c.getConversationState(id)
	if st == nil {
		return
	}
	st.VenueID = venueID
	st.UpdatedAt = time.Now()
}

func (c *ChatService) updateConversationCampaignID(conversationID, campaignID string) {
	id := strings.TrimSpace(conversationID)
	if id == "" {
		return
	}
	st := c.getConversationState(id)
	if st == nil {
		return
	}
	cid := strings.TrimSpace(campaignID)
	if cid == "" {
		return
	}
	st.CampaignID = cid
	st.UpdatedAt = time.Now()
}

func (c *ChatService) handleCampaignImpressions(ctx context.Context, req models.ChatRequest, onToken func(string)) (models.ChatResponse, bool, error) {
	msgLower := strings.ToLower(req.Message)
	if !strings.Contains(msgLower, "impression") {
		return models.ChatResponse{}, false, nil
	}
	if c.Gateway == nil {
		return models.ChatResponse{Answer: "Tool gateway is not configured."}, true, nil
	}

	conversationID := strings.TrimSpace(req.ConversationID)
	campaignID := ""
	if id := extractCampaignID(req.Message); looksLikeUUID(id) {
		campaignID = id
	} else if conversationID != "" {
		if st := c.getConversationState(conversationID); st != nil {
			if looksLikeUUID(st.CampaignID) {
				campaignID = st.CampaignID
			}
		}
	}

	// If no UUID is present, try resolving by campaign name.
	if !looksLikeUUID(campaignID) {
		// Prefer extracting the name from "campaign <name>".
		campaignName := extractAfterKeyword(msgLower, "campaign")
		if campaignName == "" {
			campaignName = extractAfterKeyword(msgLower, "campaign:")
		}
		campaignName = strings.TrimSpace(campaignName)
		if campaignName != "" {
			// First try the search endpoint.
			statusS, bodyS, errS := c.Gateway.Get("/ads/campaigns/search?query=" + urlEscape(campaignName) + "&page=1&page_size=10")
			stepSearch := models.Step{Tool: "adsCampaignsSearch", Status: statusS}
			if errS != nil {
				stepSearch.Error = errS.Error()
			} else {
				stepSearch.Body = clipString(strings.TrimSpace(string(bodyS)), 2000)
			}
			if errS == nil && statusS >= 200 && statusS < 300 {
				var parsed map[string]any
				if json.Unmarshal(bodyS, &parsed) == nil {
					rows := extractCampaignRows(parsed)
					if len(rows) == 1 {
						if m, ok := rows[0].(map[string]any); ok {
							id, _ := m["id"].(string)
							if looksLikeUUID(id) {
								campaignID = id
							}
						}
					} else if len(rows) > 1 {
						// Ambiguous; suggest top candidates.
						sugs := formatCampaignSuggestions(rows, 5)
						answer := "Multiple campaigns matched. Please specify the campaign id. Suggestions:\n" + strings.Join(sugs, "\n")
						if onToken != nil {
							onToken(answer)
						}
						return models.ChatResponse{Answer: answer, Steps: []models.Step{stepSearch}}, true, nil
					}
				}
			}

			// Fallback: reuse existing list-based resolver.
			if !looksLikeUUID(campaignID) {
				campaignID = c.resolveCampaignID(ctx, msgLower)
			}
		}
	}

	if !looksLikeUUID(campaignID) {
		return models.ChatResponse{Answer: "Please provide a campaign id (UUID), or ask like: show impressions for campaign <name>."}, true, nil
	}

	if conversationID != "" {
		c.updateConversationCampaignID(conversationID, campaignID)
		c.clearPending(conversationID)
	}

	steps := make([]models.Step, 0, 2)

	// Lifetime impressions (POP-backed) from ADS.
	adsPath := "/ads/campaigns/" + urlEscape(campaignID) + "/impressions"
	status, body, err := c.Gateway.Get(adsPath)
	stepAds := models.Step{Tool: "adsCampaignImpressions", CampaignID: campaignID, Status: status}
	if err != nil {
		stepAds.Error = err.Error()
	} else {
		stepAds.Body = clipString(strings.TrimSpace(string(body)), 2000)
	}
	steps = append(steps, stepAds)

	// Poster breakdown from POP (optional; may not be allowed by tool catalog).
	status2 := 0
	var body2 []byte
	var err2 error
	if c.Catalog == nil || c.Catalog.IsAllowed(ctx, "GET", "/pop/impressions") {
		popPath := "/pop/impressions?campaign_id=" + urlEscape(campaignID)
		status2, body2, err2 = c.Gateway.Get(popPath)
		// Some gateway deployments return 403 {"error":"forbidden_path"} even if the OpenAPI spec lists the path.
		// Treat this as an optional enrichment and don't surface it to the user.
		if !(err2 == nil && status2 == 403 && strings.Contains(string(body2), "forbidden_path")) {
			stepPop := models.Step{Tool: "popImpressions", CampaignID: campaignID, Status: status2}
			if err2 != nil {
				stepPop.Error = err2.Error()
			} else {
				stepPop.Body = clipString(strings.TrimSpace(string(body2)), 2000)
			}
			steps = append(steps, stepPop)
		} else {
			status2 = 0
			body2 = nil
			err2 = nil
		}
	}

	// Prefer POP breakdown for the summary when available.
	var popResp struct {
		CampaignID  string `json:"campaign_id"`
		Impressions int64  `json:"impressions"`
		Posters     []struct {
			PosterID    string `json:"poster_id"`
			PosterName  string `json:"poster_name"`
			Impressions int64  `json:"impressions"`
			PlayTime    int64  `json:"play_time"`
		} `json:"posters"`
	}
	if len(body2) > 0 {
		_ = json.Unmarshal(body2, &popResp)
	}

	// ADS response is wrapped in {data:...}
	var adsResp gwCampaignImpressionsResponse
	_ = json.Unmarshal(body, &adsResp)

	answer := ""
	if err != nil || status < 200 || status >= 300 {
		// If ADS impressions fail, fall back to POP impressions if possible.
		if err2 == nil && status2 >= 200 && status2 < 300 && popResp.CampaignID != "" {
			answer = fmt.Sprintf("Campaign %s impressions (from POP): %d total.", campaignID, popResp.Impressions)
		} else {
			msg := "Failed to fetch campaign impressions."
			if err != nil {
				msg += " " + err.Error()
			}
			answer = msg
		}
	} else {
		total := int64(0)
		if adsResp.Data != nil {
			total = adsResp.Data.Impressions
		}
		if popResp.CampaignID != "" {
			// Prefer POP total when present.
			total = popResp.Impressions
		}
		answer = fmt.Sprintf("Campaign %s impressions: %d total.", campaignID, total)
		if len(popResp.Posters) > 0 {
			// Show top 5 posters by impressions.
			sort.Slice(popResp.Posters, func(i, j int) bool { return popResp.Posters[i].Impressions > popResp.Posters[j].Impressions })
			lines := make([]string, 0, 6)
			lines = append(lines, answer)
			max := 5
			if len(popResp.Posters) < max {
				max = len(popResp.Posters)
			}
			for i := 0; i < max; i++ {
				p := popResp.Posters[i]
				name := strings.TrimSpace(p.PosterName)
				if name == "" {
					name = p.PosterID
				}
				lines = append(lines, fmt.Sprintf("%d. %s — %d impressions", i+1, name, p.Impressions))
			}
			answer = strings.Join(lines, "\n")
		}
	}

	if onToken != nil {
		onToken(answer)
	}
	resp := models.ChatResponse{Answer: answer, Steps: steps}
	if popResp.CampaignID != "" {
		data := &models.ChatData{CampaignImpressions: &models.CampaignImpressions{CampaignID: popResp.CampaignID, Impressions: popResp.Impressions}}
		if len(popResp.Posters) > 0 {
			posters := make([]models.PosterImpression, 0, len(popResp.Posters))
			for _, p := range popResp.Posters {
				pt := p.PlayTime
				posters = append(posters, models.PosterImpression{PosterID: p.PosterID, PosterName: p.PosterName, Impressions: p.Impressions, PlayTime: &pt})
			}
			data.CampaignImpressions.Posters = posters
		}
		resp.Data = data
	}
	return resp, true, nil
}

func (c *ChatService) handleCampaignSearchList(ctx context.Context, req models.ChatRequest, onToken func(string)) (models.ChatResponse, bool, error) {
	msgLower := strings.ToLower(req.Message)
	if !(strings.Contains(msgLower, "campaign") || strings.Contains(msgLower, "campaigns")) {
		return models.ChatResponse{}, false, nil
	}
	// If the user asked for campaign creatives, a dedicated handler should answer.
	if strings.Contains(msgLower, "creative") {
		return models.ChatResponse{}, false, nil
	}
	// Avoid colliding with impressions handler which has its own deterministic flow.
	if strings.Contains(msgLower, "impression") {
		return models.ChatResponse{}, false, nil
	}
	if c.Gateway == nil {
		return models.ChatResponse{Answer: "Tool gateway is not configured."}, true, nil
	}

	conversationID := strings.TrimSpace(req.ConversationID)
	isSearch := strings.Contains(msgLower, "search") || strings.Contains(msgLower, "find")
	isList := strings.Contains(msgLower, "list") || strings.Contains(msgLower, "show") || strings.Contains(msgLower, "all")
	if !(isSearch || isList) {
		return models.ChatResponse{}, false, nil
	}

	statusFilter := extractStatusFilter(msgLower)
	query := ""
	if isSearch {
		// Prefer the plural keyword first so we don't match "campaign" inside "campaigns".
		query = extractAfterKeyword(msgLower, "campaigns")
		if query == "" {
			query = extractAfterKeyword(msgLower, "campaign")
		}
		query = strings.TrimSpace(query)
		if query == "" {
			return models.ChatResponse{Answer: "Please provide a campaign search query (for example: search campaigns Pepsi)."}, true, nil
		}
	}

	steps := make([]models.Step, 0, 1)
	rows := make([]any, 0)

	if isSearch {
		path := "/ads/campaigns/search?query=" + urlEscape(query) + "&page=1&page_size=20"
		status, body, err := c.Gateway.Get(path)
		step := models.Step{Tool: "adsCampaignsSearch", Status: status}
		if err != nil {
			step.Error = err.Error()
		} else {
			step.Body = clipString(strings.TrimSpace(string(body)), 2000)
		}
		steps = append(steps, step)
		if err != nil {
			return models.ChatResponse{Answer: "Failed to search campaigns: " + err.Error(), Steps: steps}, true, nil
		}
		if status < 200 || status >= 300 {
			return models.ChatResponse{Answer: fmt.Sprintf("Failed to search campaigns (status %d).", status), Steps: steps}, true, nil
		}
		var parsed map[string]any
		if json.Unmarshal(body, &parsed) != nil {
			return models.ChatResponse{Answer: "Campaign search response could not be parsed.", Steps: steps}, true, nil
		}
		rows = extractCampaignRows(parsed)
	} else {
		path := "/ads/campaigns?page=1&page_size=50"
		status, body, err := c.Gateway.Get(path)
		step := models.Step{Tool: "adsCampaigns", Status: status}
		if err != nil {
			step.Error = err.Error()
		} else {
			step.Body = clipString(strings.TrimSpace(string(body)), 2000)
		}
		steps = append(steps, step)
		if err != nil {
			return models.ChatResponse{Answer: "Failed to list campaigns: " + err.Error(), Steps: steps}, true, nil
		}
		if status < 200 || status >= 300 {
			return models.ChatResponse{Answer: fmt.Sprintf("Failed to list campaigns (status %d).", status), Steps: steps}, true, nil
		}
		var parsed map[string]any
		if json.Unmarshal(body, &parsed) != nil {
			return models.ChatResponse{Answer: "Campaign list response could not be parsed.", Steps: steps}, true, nil
		}
		rows = extractCampaignRows(parsed)
	}

	if len(rows) == 0 {
		answer := "No campaigns found."
		if isSearch {
			answer = fmt.Sprintf("No campaigns found for query '%s'.", query)
		}
		return models.ChatResponse{Answer: answer, Steps: steps}, true, nil
	}

	// Apply optional status filter when present.
	if statusFilter != "" {
		filtered := make([]any, 0, len(rows))
		for _, it := range rows {
			m, ok := it.(map[string]any)
			if !ok {
				continue
			}
			st, _ := m["status"].(string)
			if strings.EqualFold(strings.TrimSpace(st), statusFilter) {
				filtered = append(filtered, it)
			}
		}
		rows = filtered
		if len(rows) == 0 {
			return models.ChatResponse{Answer: fmt.Sprintf("No %s campaigns found.", statusFilter), Steps: steps}, true, nil
		}
	}

	// If exactly one match, remember it.
	if len(rows) == 1 && conversationID != "" {
		if m, ok := rows[0].(map[string]any); ok {
			id, _ := m["id"].(string)
			if looksLikeUUID(id) {
				c.updateConversationCampaignID(conversationID, id)
			}
		}
	}

	limit := 10
	lines := make([]string, 0, limit+1)
	if isSearch {
		lines = append(lines, fmt.Sprintf("Campaign search results for '%s':", query))
	} else if statusFilter != "" {
		lines = append(lines, fmt.Sprintf("Campaigns (%s):", statusFilter))
	} else {
		lines = append(lines, "Campaigns:")
	}
	for _, it := range rows {
		if len(lines)-1 >= limit {
			break
		}
		m, ok := it.(map[string]any)
		if !ok {
			continue
		}
		id, _ := m["id"].(string)
		name, _ := m["name"].(string)
		id = strings.TrimSpace(id)
		name = strings.TrimSpace(name)
		if id == "" || name == "" {
			continue
		}
		lines = append(lines, fmt.Sprintf("- %s (%s)", name, id))
	}
	answer := strings.Join(lines, "\n")
	if onToken != nil {
		onToken(answer)
	}
	return models.ChatResponse{Answer: answer, Steps: steps}, true, nil
}

func (c *ChatService) getConversationState(conversationID string) *conversationState {
	id := strings.TrimSpace(conversationID)
	if id == "" {
		return nil
	}
	c.convMu.Lock()
	defer c.convMu.Unlock()
	if c.convState == nil {
		c.convState = map[string]*conversationState{}
	}
	st := c.convState[id]
	if st == nil {
		st = &conversationState{}
		c.convState[id] = st
	}
	return st
}

func (c *ChatService) setPending(conversationID, handler, pendingMessage string) {
	st := c.getConversationState(conversationID)
	if st == nil {
		return
	}
	st.PendingHandler = strings.TrimSpace(handler)
	st.PendingMessage = strings.TrimSpace(pendingMessage)
	st.UpdatedAt = time.Now()
}

func (c *ChatService) clearPending(conversationID string) {
	st := c.getConversationState(conversationID)
	if st == nil {
		return
	}
	st.PendingHandler = ""
	st.PendingMessage = ""
	st.UpdatedAt = time.Now()
}

func (c *ChatService) updateConversationLocation(conversationID, city, region string) {
	st := c.getConversationState(conversationID)
	if st == nil {
		return
	}
	if strings.TrimSpace(city) != "" {
		st.City = strings.ToLower(strings.TrimSpace(city))
	}
	if strings.TrimSpace(region) != "" {
		st.Region = strings.ToLower(strings.TrimSpace(region))
	}
	st.UpdatedAt = time.Now()
}

func (c *ChatService) updateConversationHost(conversationID, host string) {
	st := c.getConversationState(conversationID)
	if st == nil {
		return
	}
	if strings.TrimSpace(host) != "" {
		st.Host = strings.ToLower(strings.TrimSpace(host))
	}
	st.UpdatedAt = time.Now()
}

func normalizeLooseText(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" {
		return ""
	}
	repl := strings.NewReplacer("_", " ", "-", " ")
	s = repl.Replace(s)
	s = strings.Join(strings.Fields(s), " ")
	return s
}

func scoreDeviceNameMatch(query string, fields ...string) int {
	q := normalizeLooseText(query)
	if q == "" {
		return 0
	}
	best := 0
	for _, f := range fields {
		ff := normalizeLooseText(f)
		if ff == "" {
			continue
		}
		s := 0
		if ff == q {
			s = 200
		} else if strings.Contains(ff, q) {
			s = 120
		} else if strings.Contains(q, ff) && len(ff) >= 3 {
			s = 100
		} else {
			qParts := strings.Fields(q)
			match := 0
			for _, p := range qParts {
				if len(p) < 3 {
					continue
				}
				if strings.Contains(ff, p) {
					match++
				}
			}
			if match > 0 {
				s = 10 * match
			}
		}
		if s > best {
			best = s
		}
	}
	return best
}

func parseRows(body []byte) []any {
	var root map[string]any
	if json.Unmarshal(body, &root) != nil {
		return nil
	}
	if v, ok := root["data"]; ok {
		if rows, ok := v.([]any); ok {
			return rows
		}
		if mm, ok := v.(map[string]any); ok {
			if rows, ok := mm["data"].([]any); ok {
				return rows
			}
			if rows, ok := mm["items"].([]any); ok {
				return rows
			}
		}
	}
	if rows, ok := root["items"].([]any); ok {
		return rows
	}
	return nil
}

func (c *ChatService) resolveVenueIDFromName(ctx context.Context, conversationID string, name string) (int, *models.Step) {
	if c.Gateway == nil {
		return 0, nil
	}

	searchQuery := urlEscape(strings.TrimSpace(name))
	// Try the new search endpoint first
	searchPath := "/ads/venues/search?query=" + searchQuery + "&page=1&page_size=50"
	status, body, err := c.Gateway.Get(searchPath)
	step := &models.Step{Tool: "adsVenuesSearch", Status: status}
	if err == nil && status >= 200 && status < 300 {
		step.Body = clipString(strings.TrimSpace(string(body)), 2000)
		rows := parseRows(body)
		if len(rows) > 0 {
			bestID := 0
			bestScore := 0
			for _, it := range rows {
				m, ok := it.(map[string]any)
				if !ok {
					continue
				}
				vID := 0
				switch v := m["id"].(type) {
				case float64:
					vID = int(v)
				case int:
					vID = v
				}
				vName, _ := m["name"].(string)
				vDesc, _ := m["description"].(string)

				score := scoreDeviceNameMatch(name, vName, vDesc)
				if score > bestScore {
					bestScore = score
					bestID = vID
				}
			}
			if bestScore >= 30 {
				return bestID, step
			}
		}
	}

	// Fallback to listing all venues if search fails or finds nothing strong
	path := "/ads/venues?page=1&page_size=200"
	status, body, err = c.Gateway.Get(path)
	step = &models.Step{Tool: "adsVenues", Status: status}
	if err != nil {
		step.Error = err.Error()
		return 0, step
	}
	step.Body = clipString(strings.TrimSpace(string(body)), 2000)

	if status < 200 || status >= 300 {
		return 0, step
	}

	rows := parseRows(body)
	if len(rows) == 0 {
		return 0, step
	}

	bestID := 0
	bestScore := 0
	for _, it := range rows {
		m, ok := it.(map[string]any)
		if !ok {
			continue
		}
		vID := 0
		switch v := m["id"].(type) {
		case float64:
			vID = int(v)
		case int:
			vID = v
		}
		vName, _ := m["name"].(string)
		vDesc, _ := m["description"].(string)

		score := scoreDeviceNameMatch(name, vName, vDesc)
		if score > bestScore {
			bestScore = score
			bestID = vID
		}
	}

	if bestScore >= 30 {
		return bestID, step
	}

	return 0, step
}

func (c *ChatService) resolveHostFromDeviceName(ctx context.Context, conversationID string, name string) (string, *models.Step) {
	if c.Gateway == nil {
		return "", nil
	}
	st := c.getConversationState(conversationID)
	city := ""
	region := ""
	if st != nil {
		city = strings.ToLower(strings.TrimSpace(st.City))
		region = strings.ToLower(strings.TrimSpace(st.Region))
	}

	// Require a reasonably strong match so we don't accidentally resolve to an unrelated device.
	queryNorm := normalizeLooseText(name)
	queryTokens := make([]string, 0)
	stop := map[string]struct{}{
		"show": {}, "get": {}, "give": {}, "tell": {}, "please": {}, "me": {}, "the": {}, "a": {}, "an": {},
		"device": {}, "devices": {}, "kiosk": {}, "kiosks": {}, "server": {}, "info": {}, "detail": {}, "details": {},
		"status": {}, "health": {}, "metrics": {}, "telemetry": {},
	}
	for _, t := range strings.Fields(queryNorm) {
		if _, ok := stop[t]; ok {
			continue
		}
		if len(t) >= 4 {
			queryTokens = append(queryTokens, t)
		}
	}
	if len(queryTokens) == 0 {
		// Fall back to old behavior (but still with a higher threshold).
		for _, t := range strings.Fields(queryNorm) {
			if _, ok := stop[t]; ok {
				continue
			}
			queryTokens = append(queryTokens, t)
		}
	}
	countTokenMatches := func(candidate string) int {
		cand := normalizeLooseText(candidate)
		if cand == "" {
			return 0
		}
		m := 0
		for _, qt := range queryTokens {
			if len(qt) < 3 {
				continue
			}
			if strings.Contains(cand, qt) {
				m++
			}
		}
		return m
	}

	bestHost := ""
	bestScore := 0
	var bestStep *models.Step

	// Prefer the search endpoint when available, but different deployments may use different param names.
	searchQuery := urlEscape(strings.TrimSpace(name))
	baseCandidates := []struct {
		tool string
		path string
	}{
		{tool: "adsDevicesSearch", path: "/ads/devices/search?query=" + searchQuery + "&page=1&page_size=50"},
		{tool: "adsDevicesSearch", path: "/ads/devices/search?q=" + searchQuery + "&page=1&page_size=50"},
		{tool: "adsDevicesSearch", path: "/ads/devices/search?search=" + searchQuery + "&page=1&page_size=50"},
		{tool: "adsDevices", path: "/ads/devices?query=" + searchQuery + "&page=1&page_size=200"},
		{tool: "adsDevices", path: "/ads/devices?search=" + searchQuery + "&page=1&page_size=200"},
	}
	makeCandidates := func(withCity bool) []struct{ tool, path string } {
		out := make([]struct{ tool, path string }, 0, len(baseCandidates))
		for _, cnd := range baseCandidates {
			out = append(out, struct{ tool, path string }{tool: cnd.tool, path: cnd.path})
		}
		if withCity && city != "" {
			for i := range out {
				out[i].path += "&city=" + urlEscape(city)
			}
		}
		return out
	}

	// First try city-scoped search (more precise). If it yields no strong match, retry without city.
	runCandidates := func(cands []struct{ tool, path string }) {
		for _, cand := range cands {
			status, body, err := c.Gateway.Get(cand.path)
			searchStep := &models.Step{Tool: cand.tool, Status: status}
			if err != nil {
				searchStep.Error = err.Error()
				bestStep = searchStep
				continue
			}
			searchStep.Body = clipString(strings.TrimSpace(string(body)), 2000)
			bestStep = searchStep
			if status == 400 {
				// Likely wrong parameter name / endpoint shape; try the next candidate.
				continue
			}
			if status < 200 || status >= 300 {
				continue
			}
			rows := parseRows(body)
			if len(rows) == 0 {
				continue
			}
			for _, it := range rows {
				m, ok := it.(map[string]any)
				if !ok {
					continue
				}
				rowCity, _ := m["city"].(string)
				rowRegion, _ := m["region"].(string)
				if region != "" && strings.ToLower(strings.TrimSpace(rowRegion)) != region {
					continue
				}
				description, _ := m["description"].(string)
				displayName, _ := m["display_name"].(string)
				if strings.TrimSpace(displayName) == "" {
					displayName, _ = m["displayName"].(string)
				}
				kioskName, _ := m["kiosk_name"].(string)
				if strings.TrimSpace(kioskName) == "" {
					kioskName, _ = m["kioskName"].(string)
				}
				deviceName, _ := m["name"].(string)
				facing := ""
				stopName := ""
				if dc, ok := m["device_config"].(map[string]any); ok {
					facing, _ = dc["facing"].(string)
					if stops, ok := dc["stops"].([]any); ok && len(stops) > 0 {
						if sm, ok := stops[0].(map[string]any); ok {
							stopName, _ = sm["stop_name"].(string)
							if strings.TrimSpace(stopName) == "" {
								stopName, _ = sm["stopName"].(string)
							}
						}
					}
					if gtfs, ok := dc["gtfs"].(map[string]any); ok {
						if strings.TrimSpace(stopName) == "" {
							stopName, _ = gtfs["stopName"].(string)
						}
					}
				}
				hostName, _ := m["host_name"].(string)
				if strings.TrimSpace(hostName) == "" {
					hostName, _ = m["host"].(string)
				}
				if strings.TrimSpace(hostName) == "" {
					hostName, _ = m["hostName"].(string)
				}
				serverID, _ := m["server_id"].(string)
				if strings.TrimSpace(serverID) == "" {
					serverID, _ = m["serverId"].(string)
				}
				if strings.TrimSpace(serverID) == "" {
					serverID, _ = m["device_key"].(string)
				}
				if strings.TrimSpace(serverID) == "" {
					serverID, _ = m["deviceKey"].(string)
				}
				candidateHost := strings.TrimSpace(serverID)
				if candidateHost == "" {
					candidateHost = strings.TrimSpace(hostName)
				}
				if candidateHost == "" {
					continue
				}
				nameHits := countTokenMatches(kioskName) + countTokenMatches(deviceName) + countTokenMatches(displayName) + countTokenMatches(description) + countTokenMatches(stopName) + countTokenMatches(facing)
				if nameHits < 2 {
					continue
				}
				score := scoreDeviceNameMatch(name, kioskName, deviceName, displayName, description, stopName, facing, hostName, serverID, rowCity, rowRegion)
				if score > bestScore {
					bestScore = score
					bestHost = candidateHost
				}
			}
		}
	}

	// First try city-scoped search (more precise). If it yields no strong match, retry without city.
	runCandidates(makeCandidates(true))
	if bestScore < 30 {
		runCandidates(makeCandidates(false))
	}
	if bestScore >= 30 {
		return strings.ToLower(strings.TrimSpace(bestHost)), bestStep
	}

	page := 1
	pageSize := 200
	maxPages := 10
	for {
		path := fmt.Sprintf("/ads/devices?page=%d&page_size=%d", page, pageSize)
		if city != "" {
			path += "&city=" + urlEscape(city)
		}
		status, body, err := c.Gateway.Get(path)
		step := &models.Step{Tool: "adsDevices", Status: status}
		if err != nil {
			step.Error = err.Error()
			// Return the last step on error.
			return "", step
		}
		step.Body = clipString(strings.TrimSpace(string(body)), 2000)
		bestStep = step
		if status < 200 || status >= 300 {
			return "", step
		}

		var root map[string]any
		if json.Unmarshal(body, &root) != nil {
			return "", step
		}
		rows, _ := root["data"].([]any)
		pagination, _ := root["pagination"].(map[string]any)
		hasMore := false
		if pagination != nil {
			if v, ok := pagination["has_more"].(bool); ok {
				hasMore = v
			}
		}
		// Some deployments don't include pagination.has_more; fall back to row count.
		if !hasMore {
			if len(rows) == pageSize {
				hasMore = true
			}
		}

		for _, it := range rows {
			m, ok := it.(map[string]any)
			if !ok {
				continue
			}

			rowCity, _ := m["city"].(string)
			rowRegion, _ := m["region"].(string)
			if region != "" && strings.ToLower(strings.TrimSpace(rowRegion)) != region {
				continue
			}
			description, _ := m["description"].(string)
			displayName, _ := m["display_name"].(string)
			if strings.TrimSpace(displayName) == "" {
				displayName, _ = m["displayName"].(string)
			}

			kioskName, _ := m["kiosk_name"].(string)
			if strings.TrimSpace(kioskName) == "" {
				kioskName, _ = m["kioskName"].(string)
			}
			deviceName, _ := m["name"].(string)
			facing := ""
			stopName := ""
			if dc, ok := m["device_config"].(map[string]any); ok {
				facing, _ = dc["facing"].(string)
				if stops, ok := dc["stops"].([]any); ok && len(stops) > 0 {
					if sm, ok := stops[0].(map[string]any); ok {
						stopName, _ = sm["stop_name"].(string)
						if strings.TrimSpace(stopName) == "" {
							stopName, _ = sm["stopName"].(string)
						}
					}
				}
				if gtfs, ok := dc["gtfs"].(map[string]any); ok {
					if strings.TrimSpace(stopName) == "" {
						stopName, _ = gtfs["stopName"].(string)
					}
				}
			}
			hostName, _ := m["host_name"].(string)
			if strings.TrimSpace(hostName) == "" {
				hostName, _ = m["host"].(string)
			}
			if strings.TrimSpace(hostName) == "" {
				hostName, _ = m["hostName"].(string)
			}
			serverID, _ := m["server_id"].(string)
			if strings.TrimSpace(serverID) == "" {
				serverID, _ = m["serverId"].(string)
			}
			if strings.TrimSpace(serverID) == "" {
				serverID, _ = m["device_key"].(string)
			}
			if strings.TrimSpace(serverID) == "" {
				serverID, _ = m["deviceKey"].(string)
			}

			// Choose the best candidate identifier to use as server_id for metrics.
			candidateHost := strings.TrimSpace(serverID)
			if candidateHost == "" {
				candidateHost = strings.TrimSpace(hostName)
			}
			if candidateHost == "" {
				continue
			}

			// Require multiple token hits on the human-readable name to avoid random matches.
			nameHits := countTokenMatches(kioskName) + countTokenMatches(deviceName) + countTokenMatches(displayName) + countTokenMatches(description) + countTokenMatches(stopName) + countTokenMatches(facing)
			if nameHits < 2 {
				continue
			}

			score := scoreDeviceNameMatch(name, kioskName, deviceName, displayName, description, stopName, facing, hostName, serverID, rowCity, rowRegion)
			if score > bestScore {
				bestScore = score
				bestHost = candidateHost
			}
		}

		// If we already have a strong match, stop early.
		if bestScore >= 80 {
			break
		}
		if !hasMore {
			break
		}
		page++
		if page > maxPages {
			break
		}
	}
	if bestScore < 30 {
		return "", bestStep
	}
	return strings.ToLower(strings.TrimSpace(bestHost)), bestStep
}

type projectLookup struct {
	city        string
	name        string
	description string
	words       []string
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
			if !cityCandidateAllowed(city, s) {
				continue
			}
			return city
		}
	}
	if projectCity := c.detectCityFromProjects(ctx, s); projectCity != "" {
		return projectCity
	}
	return ""
}

func (c *ChatService) detectCityFromProjects(ctx context.Context, msgLower string) string {
	s := strings.ToLower(strings.TrimSpace(msgLower))
	if s == "" {
		return ""
	}

	// If the user provided a region code (e.g. "brt", "kc") and did not explicitly ask for a city,
	// do not infer a city from project description/keywords. This avoids cases like "show brt stats"
	// incorrectly mapping to a city whose description mentions BRT.
	regionCode := c.detectRegionCode(ctx, s)
	regionOnly := regionCode != "" && !strings.Contains(s, "city")

	c.projectMu.Lock()
	defer c.projectMu.Unlock()

	c.ensureProjectLookupsLocked(ctx)
	if len(c.projectLookups) == 0 {
		return ""
	}

	msgWords := tokenizeWords(s)
	wordSet := make(map[string]struct{}, len(msgWords))
	for _, w := range msgWords {
		if len(w) < 2 {
			continue
		}
		wordSet[w] = struct{}{}
	}

	bestCity := ""
	bestScore := 0

	for _, lookup := range c.projectLookups {
		if lookup.city == "" {
			continue
		}
		score := 0
		if len(lookup.city) <= 2 {
			if containsWord(s, lookup.city) {
				score = len(lookup.city)
			}
		} else if strings.Contains(s, lookup.city) {
			score = len(lookup.city)
		} else {
			for _, w := range msgWords {
				if len(w) < 3 {
					continue
				}
				if strings.Contains(lookup.city, w) && len(w) > score {
					score = len(w)
				}
			}
		}

		if score == 0 && lookup.name != "" && lookup.name != lookup.city && strings.Contains(s, lookup.name) {
			score = len(lookup.name)
		}

		if score == 0 && lookup.description != "" && strings.Contains(s, lookup.description) {
			score = len(lookup.description)
		}

		if score == 0 && len(lookup.words) > 0 && !regionOnly {
			for _, alias := range lookup.words {
				if len(alias) < 3 {
					continue
				}
				if strings.Contains(s, alias) || strings.Contains(alias, s) {
					if len(alias) > score {
						score = len(alias)
					}
					continue
				}
				if _, ok := wordSet[alias]; ok && len(alias) > score {
					score = len(alias)
				}
			}
		}

		if score > bestScore {
			bestScore = score
			bestCity = lookup.city
		}
	}

	return bestCity
}

func (c *ChatService) ensureProjectLookupsLocked(ctx context.Context) {
	if c.projectCacheTTL <= 0 {
		c.projectCacheTTL = 10 * time.Minute
	}
	if c.projectLookups != nil && !c.projectCityCacheAt.IsZero() && time.Since(c.projectCityCacheAt) < c.projectCacheTTL {
		return
	}
	if c.Gateway == nil {
		return
	}

	status, body, err := c.Gateway.Get("/ads/projects?page=1&page_size=200")
	if err != nil || status < 200 || status >= 300 {
		return
	}

	var resp struct {
		Data []struct {
			Name        string  `json:"name"`
			Description *string `json:"description"`
		} `json:"data"`
	}
	if json.Unmarshal(body, &resp) != nil {
		return
	}

	lookups := make([]projectLookup, 0, len(resp.Data))
	citySet := make(map[string]struct{})
	for _, item := range resp.Data {
		nameLower := strings.ToLower(strings.TrimSpace(item.Name))
		if nameLower == "" {
			continue
		}
		descLower := ""
		if item.Description != nil {
			descLower = strings.ToLower(strings.TrimSpace(*item.Description))
		}
		words := collectProjectWords(nameLower, descLower)
		lookups = append(lookups, projectLookup{
			city:        nameLower,
			name:        nameLower,
			description: descLower,
			words:       words,
		})
		citySet[nameLower] = struct{}{}
	}

	if len(lookups) == 0 {
		return
	}

	c.projectLookups = lookups
	c.projectCityCache = citySet
	c.projectCityCacheAt = time.Now()
}

func tokenizeWords(input string) []string {
	if strings.TrimSpace(input) == "" {
		return nil
	}
	lower := strings.ToLower(input)
	var words []string
	var b strings.Builder
	flush := func() {
		if b.Len() == 0 {
			return
		}
		words = append(words, b.String())
		b.Reset()
	}
	for _, r := range lower {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
		} else {
			flush()
		}
	}
	flush()
	return words
}

func collectProjectWords(values ...string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{})
	result := make([]string, 0)
	for _, val := range values {
		for _, w := range tokenizeWords(val) {
			if len(w) < 2 {
				continue
			}
			if _, ok := seen[w]; ok {
				continue
			}
			seen[w] = struct{}{}
			result = append(result, w)
		}
	}
	return result
}

func containsWord(text, word string) bool {
	if strings.TrimSpace(text) == "" || strings.TrimSpace(word) == "" {
		return false
	}
	target := strings.ToLower(strings.TrimSpace(word))
	for _, w := range tokenizeWords(text) {
		if strings.EqualFold(w, target) {
			return true
		}
	}
	return false
}

func cityCandidateAllowed(city, msgLower string) bool {
	if strings.TrimSpace(city) == "" || strings.TrimSpace(msgLower) == "" {
		return false
	}
	if len(city) >= 3 {
		return true
	}
	return strings.Contains(msgLower, "city") || strings.Contains(msgLower, "cities")
}

func (c *ChatService) handleKioskCountFromCity(ctx context.Context, req models.ChatRequest, onToken func(string)) (models.ChatResponse, bool, error) {
	msgLower := strings.ToLower(req.Message)
	containsDeviceWord := strings.Contains(msgLower, "kiosk") || strings.Contains(msgLower, "kiosks") || strings.Contains(msgLower, "device") || strings.Contains(msgLower, "devices")
	containsDataWord := strings.Contains(msgLower, " data") || strings.Contains(msgLower, "data ")

	// Host-specific "data usage" questions (e.g. "how much internet data <host> is using") should be
	// handled by the per-host telemetry handler, not city/region status.
	if containsDataWord {
		hostTokens := detectHostTokens(req.Message)
		if len(hostTokens) > 0 {
			if strings.Contains(msgLower, "internet") || strings.Contains(msgLower, "bandwidth") || strings.Contains(msgLower, "traffic") ||
				strings.Contains(msgLower, "throughput") || strings.Contains(msgLower, "usage") || strings.Contains(msgLower, "using") ||
				strings.Contains(msgLower, "consumed") || strings.Contains(msgLower, "consumption") {
				return models.ChatResponse{}, false, nil
			}
		}
	}

	cityRaw := c.detectCityCode(ctx, msgLower)
	region := c.detectRegionCode(ctx, msgLower)
	city, _ := normalizeCitySelection(cityRaw, region, msgLower)
	hasLocation := city != "" || region != ""

	if !(containsDeviceWord || (containsDataWord && hasLocation)) {
		return models.ChatResponse{}, false, nil
	}

	hasCountKeyword := strings.Contains(msgLower, "how many") || strings.Contains(msgLower, "count") || strings.Contains(msgLower, "number of")
	// "data" is ambiguous; only treat it as a status request when the user also mentions kiosks/devices.
	hasStatusKeyword := strings.Contains(msgLower, "offline") || strings.Contains(msgLower, "online") || strings.Contains(msgLower, "status") || strings.Contains(msgLower, "down") || (containsDataWord && hasLocation && containsDeviceWord)
	if !(hasCountKeyword || hasStatusKeyword) {
		return models.ChatResponse{}, false, nil
	}

	if hasStatusKeyword {
		queryCity, _ := normalizeCitySelection(city, region, msgLower)
		queryRegion := region
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

func (c *ChatService) handleMetricsTodayByLocation(ctx context.Context, req models.ChatRequest, onToken func(string)) (models.ChatResponse, bool, error) {
	msgLower := strings.ToLower(req.Message)
	if !(strings.Contains(msgLower, "metric") || strings.Contains(msgLower, "metrics")) {
		return models.ChatResponse{}, false, nil
	}
	if !(strings.Contains(msgLower, "today") || strings.Contains(msgLower, "today's") || strings.Contains(msgLower, "todays")) {
		return models.ChatResponse{}, false, nil
	}
	// If the user specified a host, the per-host telemetry handler should handle it.
	if len(detectHostTokens(req.Message)) > 0 {
		return models.ChatResponse{}, false, nil
	}

	conversationID := strings.TrimSpace(req.ConversationID)
	cityRaw := c.detectCityCode(ctx, msgLower)
	region := c.detectRegionCode(ctx, msgLower)
	city, _ := normalizeCitySelection(cityRaw, region, msgLower)
	if city == "" && region == "" && conversationID != "" {
		// Reuse last location context for follow-ups like "show today's metrics".
		if st := c.getConversationState(conversationID); st != nil {
			if strings.TrimSpace(st.City) != "" {
				city = strings.ToLower(strings.TrimSpace(st.City))
			}
			if strings.TrimSpace(st.Region) != "" {
				region = strings.ToLower(strings.TrimSpace(st.Region))
			}
		}
	}
	if city == "" && region == "" {
		// Don't fall through into host telemetry prompts for "today's metrics".
		return models.ChatResponse{Answer: "Please specify a city and/or region (for example: city moco, region brt)."}, true, nil
	}
	if strings.TrimSpace(req.ConversationID) != "" {
		c.updateConversationLocation(req.ConversationID, city, region)
		if strings.TrimSpace(region) != "" {
			c.updateConversationLocation(req.ConversationID, "", region)
		}
		c.clearPending(req.ConversationID)
	}
	if c.Gateway == nil {
		return models.ChatResponse{Answer: "Tool gateway is not configured."}, true, nil
	}

	filterCity := strings.ToLower(strings.TrimSpace(city))
	filterRegion := strings.ToLower(strings.TrimSpace(region))

	count := 0
	online := 0
	var cpuSum, memSum, tempSum float64
	var dailyRx, dailyTx, monthlyRx, monthlyTx int64
	latest := time.Time{}

	steps := make([]models.Step, 0, 3)
	page := 1
	pageSize := 200
	maxPages := 5
	for {
		path := fmt.Sprintf("/metrics/latest?page=%d&page_size=%d&include_totals=false", page, pageSize)
		status, body, err := c.Gateway.Get(path)
		step := models.Step{Tool: "metricsLatest", Status: status}
		if err != nil {
			step.Error = err.Error()
		} else {
			step.Body = clipString(strings.TrimSpace(string(body)), 2000)
		}
		steps = append(steps, step)

		if err != nil {
			return models.ChatResponse{Answer: "Failed to fetch latest metrics: " + err.Error(), Steps: steps}, true, nil
		}
		if status < 200 || status >= 300 {
			return models.ChatResponse{Answer: fmt.Sprintf("Failed to fetch latest metrics (status %d).", status), Steps: steps}, true, nil
		}

		var payload struct {
			Data []struct {
				Time              time.Time `json:"time"`
				CPU               float64   `json:"cpu"`
				Memory            float64   `json:"memory"`
				Temperature       float64   `json:"temperature"`
				NetDailyRxBytes   int64     `json:"net_daily_rx_bytes"`
				NetDailyTxBytes   int64     `json:"net_daily_tx_bytes"`
				NetMonthlyRxBytes int64     `json:"net_monthly_rx_bytes"`
				NetMonthlyTxBytes int64     `json:"net_monthly_tx_bytes"`
				PowerOnline       bool      `json:"power_online"`
				City              string    `json:"city"`
				Region            string    `json:"region"`
			} `json:"data"`
			Pagination struct {
				HasMore bool `json:"has_more"`
			} `json:"pagination"`
		}
		if json.Unmarshal(body, &payload) != nil {
			return models.ChatResponse{Answer: "Latest metrics response could not be parsed.", Steps: steps}, true, nil
		}

		for _, row := range payload.Data {
			rowCity := strings.ToLower(strings.TrimSpace(row.City))
			rowRegion := strings.ToLower(strings.TrimSpace(row.Region))
			if filterCity != "" && rowCity != filterCity {
				continue
			}
			if filterRegion != "" && rowRegion != filterRegion {
				continue
			}
			count++
			if row.PowerOnline {
				online++
			}
			cpuSum += row.CPU
			memSum += row.Memory
			tempSum += row.Temperature
			dailyRx += row.NetDailyRxBytes
			dailyTx += row.NetDailyTxBytes
			monthlyRx += row.NetMonthlyRxBytes
			monthlyTx += row.NetMonthlyTxBytes
			if row.Time.After(latest) {
				latest = row.Time
			}
		}

		if !payload.Pagination.HasMore {
			break
		}
		page++
		if page > maxPages {
			break
		}
	}

	scopeParts := make([]string, 0, 2)
	if filterCity != "" {
		scopeParts = append(scopeParts, fmt.Sprintf("city '%s'", filterCity))
	}
	if filterRegion != "" {
		scopeParts = append(scopeParts, fmt.Sprintf("region '%s'", filterRegion))
	}
	scopeLabel := strings.Join(scopeParts, ", ")
	if scopeLabel == "" {
		scopeLabel = "the requested scope"
	}

	if count == 0 {
		answer := fmt.Sprintf("No latest metrics were found for %s.", scopeLabel)
		if onToken != nil {
			onToken(answer)
		}
		return models.ChatResponse{Answer: answer, Steps: steps}, true, nil
	}

	avgCPU := cpuSum / float64(count)
	avgMem := memSum / float64(count)
	avgTemp := tempSum / float64(count)

	answer := fmt.Sprintf(
		"Today's metrics for %s: %d devices (%d online). Network daily RX %.1f MB, TX %.1f MB | monthly RX %.1f MB, TX %.1f MB. Avg CPU %.1f%%, memory %.1f%%, temp %.1f°C. (latest %s UTC).",
		scopeLabel,
		count,
		online,
		bytesToMiB(dailyRx),
		bytesToMiB(dailyTx),
		bytesToMiB(monthlyRx),
		bytesToMiB(monthlyTx),
		avgCPU,
		avgMem,
		avgTemp,
		latest.Format(time.RFC3339),
	)
	if onToken != nil {
		onToken(answer)
	}
	return models.ChatResponse{Answer: answer, Steps: steps}, true, nil
}

func extractTopN(msgLower string) int {
	s := strings.ToLower(strings.TrimSpace(msgLower))
	if s == "" {
		return 0
	}
	// Simple patterns: "top 5", "top 10".
	for _, tok := range strings.Fields(s) {
		if tok == "top" {
			continue
		}
	}
	parts := strings.Fields(s)
	for i := 0; i < len(parts)-1; i++ {
		if parts[i] == "top" {
			if n, err := strconv.Atoi(strings.TrimSpace(parts[i+1])); err == nil {
				if n > 0 {
					if n > 200 {
						return 200
					}
					return n
				}
			}
		}
	}
	return 0
}

func (c *ChatService) handlePopStatsGeneric(ctx context.Context, req models.ChatRequest, onToken func(string)) (models.ChatResponse, bool, error) {
	msgLower := strings.ToLower(req.Message)
	if !(strings.Contains(msgLower, "stat") || strings.Contains(msgLower, "pop") || strings.Contains(msgLower, "analytic")) {
		return models.ChatResponse{}, false, nil
	}

	// When the user asks for "top X" analytics, allow this generic stats handler to answer.
	// Specific top-* handlers run earlier and will still take precedence.
	if !strings.Contains(msgLower, "analytic") && (strings.Contains(msgLower, "top poster") || strings.Contains(msgLower, "top device") || strings.Contains(msgLower, "top kiosk")) {
		// Covered by specific handlers that run earlier.
		return models.ChatResponse{}, false, nil
	}

	conversationID := strings.TrimSpace(req.ConversationID)
	city := c.detectCityCode(ctx, msgLower)
	region := c.detectRegionCode(ctx, msgLower)
	// If the user explicitly mentioned a city or region in the message, prioritize that.
	// Otherwise, fallback to the conversation state.
	if city == "" && region == "" && conversationID != "" {
		if st := c.getConversationState(conversationID); st != nil {
			city = strings.ToLower(strings.TrimSpace(st.City))
			region = strings.ToLower(strings.TrimSpace(st.Region))
		}
	}
	if city == "" && region == "" {
		return models.ChatResponse{Answer: "Please specify a city or region code (for example: kcmo or brt)."}, true, nil
	}
	if conversationID != "" {
		c.updateConversationLocation(conversationID, city, region)
		c.clearPending(conversationID)
	}
	if c.Gateway == nil {
		return models.ChatResponse{Answer: "Tool gateway is not configured."}, true, nil
	}

	groupBy := ""
	switch {
	case strings.Contains(msgLower, "poster"):
		groupBy = "poster"
	case strings.Contains(msgLower, "device"):
		groupBy = "device"
	case strings.Contains(msgLower, "kiosk"):
		groupBy = "kiosk"
	}
	if groupBy == "" {
		groupBy = "poster"
	}

	metric := "clicks"
	if strings.Contains(msgLower, "play") {
		metric = "plays"
	} else if strings.Contains(msgLower, "count") {
		metric = "count"
	}

	limit := 10
	basePath := fmt.Sprintf("/pop/stats?group_by=%s&metric=%s&order=top&limit=%d", groupBy, metric, limit)
	path := basePath
	scopeLabel := ""
	if region != "" {
		path += "&region=" + urlEscape(region)
		scopeLabel = fmt.Sprintf("region '%s'", region)
	} else if city != "" {
		path += "&city=" + urlEscape(city)
		scopeLabel = fmt.Sprintf("city '%s'", city)
	}

	status, body, err := c.Gateway.Get(path)
	step := models.Step{Tool: "popStats", Status: status}
	if err != nil {
		step.Error = err.Error()
	} else {
		step.Body = clipString(strings.TrimSpace(string(body)), 2000)
	}
	steps := []models.Step{step}

	if err != nil {
		return models.ChatResponse{Answer: "Failed to fetch POP stats: " + err.Error(), Steps: steps}, true, nil
	}
	if status < 200 || status >= 300 {
		answer := fmt.Sprintf("Failed to fetch POP stats (status %d).", status)
		return models.ChatResponse{Answer: answer, Steps: []models.Step{step}}, true, nil
	}

	type popStatsResponse struct {
		Items []map[string]any `json:"items"`
	}
	var parsed popStatsResponse
	if json.Unmarshal(body, &parsed) != nil {
		return models.ChatResponse{Answer: fmt.Sprintf("POP stats response for %s could not be parsed.", scopeLabel), Steps: []models.Step{step}}, true, nil
	}

	if len(parsed.Items) == 0 {
		answer := fmt.Sprintf("No POP %s stats found for %s.", metric, scopeLabel)
		return models.ChatResponse{Answer: answer, Steps: steps}, true, nil
	}

	lines := make([]string, 0, len(parsed.Items))
	for idx, row := range parsed.Items {
		if idx >= 10 {
			break
		}
		name := ""
		if groupBy == "poster" {
			if s, ok := row["PosterName"].(string); ok && strings.TrimSpace(s) != "" {
				name = s
			}
		}
		if name == "" {
			if s, ok := row["Key"].(string); ok {
				name = s
			}
		}
		if strings.TrimSpace(name) == "" {
			continue
		}
		value := 0.0
		switch v := row["Metric"].(type) {
		case float64:
			value = v
		case int:
			value = float64(v)
		case json.Number:
			if f, e := v.Float64(); e == nil {
				value = f
			}
		}
		lines = append(lines, fmt.Sprintf("%d. %s — %.0f %s", len(lines)+1, name, value, metric))
	}

	if len(lines) == 0 {
		answer := fmt.Sprintf("No POP %s stats found for %s.", metric, scopeLabel)
		return models.ChatResponse{Answer: answer, Steps: []models.Step{step}}, true, nil
	}

	prefix := "Top"
	switch groupBy {
	case "poster":
		prefix = "Top posters"
	case "device":
		prefix = "Top devices"
	case "kiosk":
		prefix = "Top kiosks"
	}
	answer := fmt.Sprintf("%s in %s by %s:\n%s", prefix, scopeLabel, metric, strings.Join(lines, "\n"))
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
	// Avoid misrouting analytics/POP/stats queries to telemetry just because they mention "kiosk" or "device".
	// Telemetry intent should be explicit (telemetry/health/status/metrics).
	if strings.Contains(msgLower, "analytic") || strings.Contains(msgLower, "pop") || strings.Contains(msgLower, "stats") {
		if !(contains("telemetry", "health", "device status", "metrics", "temperature", "battery", "uptime", "disk", "storage", "cpu", "ram", "volume", "mute", "network", "bandwidth", "data usage")) {
			return models.ChatResponse{}, false, nil
		}
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
	// Network usage is often phrased as "how much data <host> is using".
	wantsNetwork := contains("network", "bandwidth", "traffic", "throughput", "internet", "data usage", "data-use", "consumed", "consumption", "usage", "using")
	wantsProcesses := contains("process", "service", "app", "apps", "kiosk")
	wantsInputDevices := contains("input", "usb", "peripheral")
	wantsUptime := contains("uptime")
	wantsTelemetry := contains("telemetry", "status", "health", "metrics", "device status")
	if !wantsNetwork {
		// Heuristic: "how much data" implies usage even without the explicit token "usage".
		if strings.Contains(msgLower, "how much") && strings.Contains(msgLower, "data") {
			wantsNetwork = true
		}
	}
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
		conversationID := strings.TrimSpace(req.ConversationID)
		if conversationID != "" {
			// If we already resolved a host earlier in this conversation, reuse it.
			if st := c.getConversationState(conversationID); st != nil {
				lastHost := strings.TrimSpace(st.Host)
				if lastHost != "" {
					req2 := req
					req2.Message = strings.TrimSpace(req.Message) + " " + lastHost
					return c.handleDeviceTelemetry(ctx, req2, onToken)
				}
			}
			c.setPending(conversationID, "deviceTelemetry", req.Message)
		}
		return models.ChatResponse{Answer: "Please specify the device or server name (for example: dart2)."}, true, nil
	}

	host := strings.ToLower(strings.TrimSpace(hostTokens[0]))
	if host == "" {
		conversationID := strings.TrimSpace(req.ConversationID)
		if conversationID != "" {
			c.setPending(conversationID, "deviceTelemetry", req.Message)
		}
		return models.ChatResponse{Answer: "Please specify the device or server name (for example: dart2)."}, true, nil
	}
	if strings.TrimSpace(req.ConversationID) != "" {
		c.updateConversationHost(req.ConversationID, host)
		// Derive city/region from host prefix when possible (e.g. moco-brt-...).
		parts := strings.Split(strings.ReplaceAll(host, "_", "-"), "-")
		if len(parts) >= 2 {
			c.updateConversationLocation(req.ConversationID, parts[0], parts[1])
		}
		c.clearPending(req.ConversationID)
	}

	includeTotals := "false"
	if wantsNetwork {
		includeTotals = "true"
	}
	path := "/metrics/history?page=1&page_size=1&include_totals=" + includeTotals + "&server_id=" + urlEscape(host)
	status, body, err := c.Gateway.Get(path)
	step := models.Step{Tool: "metricsHistory", Status: status}
	if err != nil {
		step.Error = err.Error()
	} else {
		step.Body = clipString(strings.TrimSpace(string(body)), 2000)
	}

	answer := ""
	if err != nil {
		answer = "Failed to fetch telemetry data: " + err.Error()
	} else if status < 200 || status >= 300 {
		answer = fmt.Sprintf("Failed to fetch telemetry data (status %d).", status)
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
				NetMonthlyRxBytes    int64     `json:"net_monthly_rx_bytes"`
				NetMonthlyTxBytes    int64     `json:"net_monthly_tx_bytes"`
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
			Totals map[string]any `json:"totals"`
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
				monthlyRx := int64(0)
				monthlyTx := int64(0)
				monthlyFromTotals := false
				if payload.Totals != nil {
					if v, ok := payload.Totals["net_monthly_rx_bytes"]; ok {
						switch vv := v.(type) {
						case float64:
							monthlyRx = int64(vv)
							monthlyFromTotals = true
						case int64:
							monthlyRx = vv
							monthlyFromTotals = true
						case json.Number:
							if n, e := vv.Int64(); e == nil {
								monthlyRx = n
								monthlyFromTotals = true
							}
						}
					}
					if v, ok := payload.Totals["net_monthly_tx_bytes"]; ok {
						switch vv := v.(type) {
						case float64:
							monthlyTx = int64(vv)
							monthlyFromTotals = true
						case int64:
							monthlyTx = vv
							monthlyFromTotals = true
						case json.Number:
							if n, e := vv.Int64(); e == nil {
								monthlyTx = n
								monthlyFromTotals = true
							}
						}
					}
				}
				if !monthlyFromTotals {
					// Some metrics deployments return monthly totals on the per-record entry.
					monthlyRx = entry.NetMonthlyRxBytes
					monthlyTx = entry.NetMonthlyTxBytes
				}
				netLine := fmt.Sprintf(
					"Network current RX %.1f MB, TX %.1f MB | daily RX %.1f MB, TX %.1f MB",
					bytesToMiB(entry.NetBytesRecv),
					bytesToMiB(entry.NetBytesSent),
					bytesToMiB(entry.NetDailyRxBytes),
					bytesToMiB(entry.NetDailyTxBytes),
				)
				netLine += fmt.Sprintf(" | monthly RX %.1f MB, TX %.1f MB", bytesToMiB(monthlyRx), bytesToMiB(monthlyTx))
				sections = append(sections, netLine+".")
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

	// Prefer explicit "<code> region" or "region <code>" patterns to avoid mis-detecting city tokens
	// as region tokens when both are present (e.g. "moco city brt region").
	if strings.Contains(s, "region") {
		words := tokenizeWords(s)
		for i := 0; i < len(words); i++ {
			if words[i] != "region" {
				continue
			}
			// region <code>
			if i+1 < len(words) {
				cand := strings.ToLower(strings.TrimSpace(words[i+1]))
				if cand != "" && c.isKnownRegionCode(ctx, cand) {
					// If the user also mentioned "city" and this token looks like a city code,
					// do not treat it as a region.
					if strings.Contains(s, "city") && c.isKnownProjectCityCode(ctx, cand) {
						// continue searching; this is likely the city token (e.g. "moco city")
					} else {
					return cand
					}
				}
			}
			// <code> region
			if i-1 >= 0 {
				cand := strings.ToLower(strings.TrimSpace(words[i-1]))
				if cand != "" && c.isKnownRegionCode(ctx, cand) {
					if strings.Contains(s, "city") && c.isKnownProjectCityCode(ctx, cand) {
						// continue searching
					} else {
					return cand
					}
				}
			}
		}
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
		// If a token is both a region code and a known project city code (e.g. "kcmo"),
		// treat it as a city unless the user explicitly asked for a region.
		if !strings.Contains(s, "region") && len(r) > 3 && c.isKnownProjectCityCode(ctx, r) {
			continue
		}
		// Avoid false positives for very short codes like "da" matching inside words like "today".
		if len(r) <= 2 {
			if containsWord(s, r) {
				return r
			}
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

func (c *ChatService) buildInterpretationHeader(req models.ChatRequest, conversationID string) string {
	msg := strings.TrimSpace(req.Message)
	if msg == "" {
		return ""
	}
	msgLower := strings.ToLower(msg)

	var st *conversationState
	if strings.TrimSpace(conversationID) != "" {
		st = c.getConversationState(strings.TrimSpace(conversationID))
	}

	isPopDomain := strings.Contains(msgLower, "pop") || strings.Contains(msgLower, "stat") || strings.Contains(msgLower, "analytic")
	isDeviceHealth := strings.Contains(msgLower, "telemetry") || strings.Contains(msgLower, "health") || strings.Contains(msgLower, "metrics") || strings.Contains(msgLower, "cpu") || strings.Contains(msgLower, "ram") || strings.Contains(msgLower, "memory") || strings.Contains(msgLower, "disk") || strings.Contains(msgLower, "storage") || strings.Contains(msgLower, "uptime") || strings.Contains(msgLower, "battery") || strings.Contains(msgLower, "network") || strings.Contains(msgLower, "bandwidth") || strings.Contains(msgLower, "data usage") || strings.Contains(msgLower, "volume") || strings.Contains(msgLower, "mute")

	// Time window
	timeWindow := ""
	if strings.Contains(msgLower, "yesterday") || strings.Contains(msgLower, "yesterday's") || strings.Contains(msgLower, "yesterdays") {
		timeWindow = "Yesterday"
	} else if strings.Contains(msgLower, "today") || strings.Contains(msgLower, "today's") || strings.Contains(msgLower, "todays") || strings.Contains(msgLower, "current_day") {
		timeWindow = "Today"
	} else if strings.Contains(msgLower, "month") {
		timeWindow = "Monthly"
	} else if strings.Contains(msgLower, "since") {
		timeWindow = "Since"
	}

	// Unit
	unit := ""
	if strings.Contains(msgLower, "minute") {
		unit = "in minutes"
	}

	// Scope/entity
	subject := ""
	if isDeviceHealth {
		subject = "Device health"
	} else if strings.Contains(msgLower, "device") || strings.Contains(msgLower, "kiosk") || strings.Contains(msgLower, "server") {
		if strings.Contains(msgLower, "detail") || strings.Contains(msgLower, "info") || strings.Contains(msgLower, "show") || strings.Contains(msgLower, "get") {
			subject = "Device info"
		}
	} else if strings.Contains(msgLower, "venue") {
		if strings.Contains(msgLower, "device") || strings.Contains(msgLower, "devices") {
			subject = "Venue devices"
		} else {
			subject = "Venue"
		}
	} else if strings.Contains(msgLower, "poster") || strings.Contains(msgLower, "creative") || strings.Contains(msgLower, "ad") {
		if isPopDomain {
			subject = "Poster POP"
		} else if strings.Contains(msgLower, "play") || strings.Contains(msgLower, "count") {
			subject = "Poster play count"
		} else {
			subject = "Poster"
		}
	} else if isPopDomain {
		subject = "POP"
	}
	if subject == "" && st != nil {
		if strings.Contains(msgLower, "same") {
			if strings.TrimSpace(st.PosterID) != "" || strings.TrimSpace(st.PosterName) != "" {
				subject = "Poster play count"
			}
		}
	}

	// Grouping/shape
	shape := ""
	if strings.Contains(msgLower, "kiosk wise") || strings.Contains(msgLower, "kiosk-wise") || strings.Contains(msgLower, "kiosks wise") || strings.Contains(msgLower, "kiosks-wise") {
		shape = "kiosk-wise"
	} else if strings.Contains(msgLower, "top") && (strings.Contains(msgLower, "kiosk") || strings.Contains(msgLower, "kiosks")) {
		shape = "top kiosks"
	} else if strings.Contains(msgLower, "top") && (strings.Contains(msgLower, "device") || strings.Contains(msgLower, "devices")) {
		shape = "top devices"
	} else if strings.Contains(msgLower, "top") && (strings.Contains(msgLower, "poster") || strings.Contains(msgLower, "posters")) {
		shape = "top posters"
	} else if strings.Contains(msgLower, "top") {
		shape = "top"
	}

	// Geo scope (best-effort)
	scope := ""
	getPrevToken := func(tokens []string, idx int) string {
		if idx <= 0 || idx >= len(tokens) {
			return ""
		}
		cand := strings.TrimSpace(tokens[idx-1])
		cand = strings.Trim(cand, "\"'.,;:()[]{}")
		candLower := strings.ToLower(cand)
		switch candLower {
		case "from", "in", "the", "a", "an", "of", "for", "on", "at", "to", "with":
			return ""
		}
		return cand
	}
	getNextToken := func(tokens []string, idx int) string {
		if idx < 0 || idx+1 >= len(tokens) {
			return ""
		}
		cand := strings.TrimSpace(tokens[idx+1])
		cand = strings.Trim(cand, "\"'.,;:()[]{}")
		candLower := strings.ToLower(cand)
		switch candLower {
		case "from", "in", "the", "a", "an", "of", "for", "on", "at", "to", "with":
			return ""
		}
		return cand
	}
	words := strings.Fields(msgLower)
	region := ""
	city := ""
	for i, w := range words {
		wl := strings.ToLower(strings.Trim(w, "\"'.,;:()[]{}"))
		if region == "" && wl == "region" {
			region = getPrevToken(words, i)
			if region == "" {
				region = getNextToken(words, i)
			}
		}
		if city == "" && wl == "city" {
			city = getPrevToken(words, i)
			if city == "" {
				city = getNextToken(words, i)
			}
		}
	}
	if region == "" {
		region2 := strings.TrimSpace(extractAfterKeyword(msgLower, "region"))
		if region2 != "" {
			if f := strings.Fields(region2); len(f) > 0 {
				region = strings.Trim(f[0], "\"'.,;:()[]{}")
			}
		}
	}
	if city == "" {
		city2 := strings.TrimSpace(extractAfterKeyword(msgLower, "city"))
		if city2 != "" {
			if f := strings.Fields(city2); len(f) > 0 {
				city = strings.Trim(f[0], "\"'.,;:()[]{}")
			}
		}
	}
	if region != "" {
		scope = "region=" + region
	}
	if city != "" {
		if scope != "" {
			scope += ","
		}
		scope += "city=" + city
	}
	if scope == "" && st != nil {
		memRegion := strings.ToLower(strings.TrimSpace(st.Region))
		memCity := strings.ToLower(strings.TrimSpace(st.City))
		if memRegion != "" {
			scope = "region=" + memRegion
		}
		if memCity != "" {
			if scope != "" {
				scope += ","
			}
			scope += "city=" + memCity
		}
	}

	// Target (host token if present)
	target := ""
	userProvidedHost := false
	isLikelyHost := func(s string) bool {
		cand := strings.ToLower(strings.TrimSpace(s))
		cand = strings.ReplaceAll(cand, "_", "-")
		if cand == "" {
			return false
		}
		parts := strings.Split(cand, "-")
		good := 0
		for _, p := range parts {
			if strings.TrimSpace(p) != "" {
				good++
			}
		}
		return good >= 3
	}
	if tokens := detectHostTokens(msg); len(tokens) > 0 {
		cand := strings.ToLower(strings.TrimSpace(tokens[0]))
		if isLikelyHost(cand) {
			target = cand
			userProvidedHost = true
		}
	}
	if target == "" && st != nil {
		// Only show a remembered host when it's clearly relevant (device/host-oriented queries).
		// For poster-specific POP/play-count flows (including kiosk-wise follow-ups), avoid implying a host target.
		if strings.TrimSpace(st.Host) != "" {
			if isDeviceHealth {
				target = strings.ToLower(strings.TrimSpace(st.Host))
			} else if subject == "Device info" {
				target = strings.ToLower(strings.TrimSpace(st.Host))
			} else if subject != "" && strings.Contains(strings.ToLower(subject), "poster") {
				// no-op
			} else if strings.Contains(msgLower, "poster") || strings.Contains(msgLower, "creative") || strings.Contains(msgLower, "ad") {
				// no-op
			} else if userProvidedHost {
				target = strings.ToLower(strings.TrimSpace(st.Host))
			} else {
				// POP-by-host / device details / similar follow-ups can reuse host.
				if strings.Contains(msgLower, "pop") || strings.Contains(msgLower, "device") || strings.Contains(msgLower, "kiosk") || strings.Contains(msgLower, "server") {
					target = strings.ToLower(strings.TrimSpace(st.Host))
				}
			}
		}
	}

	posterRef := ""
	if st != nil && subject != "Device info" && !isDeviceHealth {
		if strings.TrimSpace(st.PosterID) != "" {
			posterRef = strings.TrimSpace(st.PosterID)
		} else if strings.TrimSpace(st.PosterName) != "" {
			posterRef = strings.TrimSpace(st.PosterName)
		}
	}
	if p := extractAfterKeyword(msgLower, "poster"); strings.TrimSpace(p) != "" {
		p = strings.TrimSpace(p)
		if f := strings.Fields(p); len(f) > 0 {
			if looksLikeUUID(f[0]) {
				posterRef = f[0]
			} else if strings.TrimSpace(posterRef) == "" {
				posterRef = p
			}
		}
	}
	for _, tk := range strings.Fields(msgLower) {
		if looksLikeUUID(tk) {
			posterRef = tk
			break
		}
	}
	if strings.TrimSpace(posterRef) != "" {
		posterRef = clipString(strings.TrimSpace(posterRef), 60)
	}
	// If we have poster context and the user is asking for kiosk-wise breakdown, avoid implying a host target
	// unless the user explicitly provided a host token.
	if !userProvidedHost && !isDeviceHealth && strings.TrimSpace(posterRef) != "" && shape == "kiosk-wise" {
		target = ""
	}

	venueRef := ""
	venueID := 0
	if st != nil && st.VenueID > 0 {
		venueID = st.VenueID
	}
	if strings.Contains(msgLower, "venue") {
		v := strings.TrimSpace(extractAfterKeyword(msgLower, "venue"))
		if v != "" {
			if f := strings.Fields(v); len(f) > 0 {
				if id, err := strconv.Atoi(strings.TrimSpace(f[0])); err == nil {
					venueID = id
				} else {
					venueRef = v
				}
			}
		}
	}
	if venueRef != "" {
		venueRef = clipString(strings.TrimSpace(venueRef), 60)
	}

	parts := make([]string, 0, 6)
	if timeWindow != "" {
		parts = append(parts, timeWindow)
	}
	if subject != "" {
		parts = append(parts, subject)
	}
	if posterRef != "" {
		parts = append(parts, "poster="+posterRef)
	}
	if venueID > 0 {
		parts = append(parts, fmt.Sprintf("venue_id=%d", venueID))
	} else if venueRef != "" {
		parts = append(parts, "venue="+venueRef)
	}
	if target != "" {
		parts = append(parts, "for "+target)
	}
	if shape != "" {
		parts = append(parts, "("+shape+")")
	}
	if scope != "" {
		parts = append(parts, "["+scope+"]")
	}
	if unit != "" {
		parts = append(parts, unit)
	}
	if len(parts) == 0 {
		return ""
	}
	return "Interpreted request: " + strings.Join(parts, " ")
}

func prefixIfNeeded(header, answer string) string {
	h := strings.TrimSpace(header)
	a := strings.TrimSpace(answer)
	if h == "" {
		return answer
	}
	if a == "" {
		return header
	}
	if strings.HasPrefix(a, h) {
		return answer
	}
	return h + "\n" + answer
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
	conversationID := strings.TrimSpace(req.ConversationID)
	city := c.detectCityCode(ctx, msgLower)
	region := ""
	if city == "" {
		region = c.detectRegionCode(ctx, msgLower)
		// Memory fallback: "show top posters" should reuse last scope.
		if region == "" && conversationID != "" {
			if st := c.getConversationState(conversationID); st != nil {
				if strings.TrimSpace(st.Region) != "" {
					region = strings.ToLower(strings.TrimSpace(st.Region))
				} else if strings.TrimSpace(st.City) != "" {
					city = strings.ToLower(strings.TrimSpace(st.City))
				}
			}
		}
		if region == "" && city == "" {
			return models.ChatResponse{Answer: "Please specify a city code (for example: kcmo, kc, brt, dart)."}, true, nil
		}
	}
	if conversationID != "" {
		c.updateConversationLocation(conversationID, city, region)
		c.clearPending(conversationID)
	}
	if c.Gateway == nil {
		return models.ChatResponse{Answer: "Tool gateway is not configured."}, true, nil
	}
	metric := "clicks"
	if strings.Contains(msgLower, "play") {
		metric = "plays"
	}
	limit := 10
	if n := extractTopN(msgLower); n > 0 {
		limit = n
	}
	path := "/pop/stats?group_by=poster&metric=" + metric + "&order=top&limit=" + fmt.Sprintf("%d", limit)
	scopeLabel := ""
	if region != "" {
		path += "&region=" + urlEscape(region)
		scopeLabel = region
	} else {
		path += "&city=" + urlEscape(city)
		scopeLabel = city
	}
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
			if region != "" {
				answer = fmt.Sprintf("No poster %s stats found for region '%s'.", metric, scopeLabel)
			} else {
				answer = fmt.Sprintf("No poster %s stats found for city '%s'.", metric, scopeLabel)
			}
		} else {
			lines := make([]string, 0, len(itemsAny))
			for i, it := range itemsAny {
				if i >= limit {
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
			if region != "" {
				answer = fmt.Sprintf("Top posters in %s by %s:\n%s", scopeLabel, metric, strings.Join(lines, "\n"))
			} else {
				answer = fmt.Sprintf("Top posters in %s by %s:\n%s", scopeLabel, metric, strings.Join(lines, "\n"))
			}
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
	conversationID := strings.TrimSpace(req.ConversationID)
	city := c.detectCityCode(ctx, msgLower)
	region := ""
	if city == "" {
		region = c.detectRegionCode(ctx, msgLower)
		// Memory fallback: "show top devices" should reuse last scope.
		if region == "" && conversationID != "" {
			if st := c.getConversationState(conversationID); st != nil {
				if strings.TrimSpace(st.Region) != "" {
					region = strings.ToLower(strings.TrimSpace(st.Region))
				} else if strings.TrimSpace(st.City) != "" {
					city = strings.ToLower(strings.TrimSpace(st.City))
				}
			}
		}
		if region == "" && city == "" {
			return models.ChatResponse{Answer: "Please specify a city code (for example: kcmo, kc, brt, dart)."}, true, nil
		}
	}
	if conversationID != "" {
		c.updateConversationLocation(conversationID, city, region)
		c.clearPending(conversationID)
	}
	if c.Gateway == nil {
		return models.ChatResponse{Answer: "Tool gateway is not configured."}, true, nil
	}
	metric := "clicks"
	if strings.Contains(msgLower, "play") {
		metric = "plays"
	}
	limit := 10
	if n := extractTopN(msgLower); n > 0 {
		limit = n
	}
	path := "/pop/stats?group_by=device&metric=" + metric + "&order=top&limit=" + fmt.Sprintf("%d", limit)
	scopeLabel := ""
	if region != "" {
		path += "&region=" + urlEscape(region)
		scopeLabel = region
	} else {
		path += "&city=" + urlEscape(city)
		scopeLabel = city
	}
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
			if region != "" {
				answer = fmt.Sprintf("No device %s stats found for region '%s'.", metric, scopeLabel)
			} else {
				answer = fmt.Sprintf("No device %s stats found for city '%s'.", metric, scopeLabel)
			}
		} else {
			lines := make([]string, 0, len(itemsAny))
			for i, it := range itemsAny {
				if i >= limit {
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
			answer = fmt.Sprintf("Top devices in %s by %s:\n%s", scopeLabel, metric, strings.Join(lines, "\n"))
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
	rest := msgLower[idx+len(keyword):]
	return strings.TrimSpace(rest)
}

func extractAfterKeywordOriginal(msg string, keyword string) string {
	msgTrim := strings.TrimSpace(msg)
	if msgTrim == "" {
		return ""
	}
	key := strings.ToLower(strings.TrimSpace(keyword))
	if key == "" {
		return ""
	}
	low := strings.ToLower(msgTrim)
	idx := strings.Index(low, key)
	if idx < 0 {
		return ""
	}
	start := idx + len(key)
	if start < 0 || start > len(msgTrim) {
		return ""
	}
	return strings.TrimSpace(msgTrim[start:])
}

func urlEscape(s string) string {
	// url.QueryEscape is appropriate for encoding query parameter values.
	return url.QueryEscape(strings.TrimSpace(s))
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

	rawCityCode := c.detectCityCode(ctx, msgLower)
	regionCodeForQuery := c.detectRegionCode(ctx, msgLower)
	cityCodeForQuery, cityExplicit := normalizeCitySelection(rawCityCode, regionCodeForQuery, msgLower)
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
	cityCodeForQuery, cityExplicit = normalizeCitySelection(c.detectCityCode(ctx, msgLower), regionCodeForQuery, msgLower)
	hasCityCode := cityCodeForQuery != ""
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
				// Prefer region when no city detected, user explicitly says region, or city/region tokens are identical (e.g. "brt").
				if strings.Contains(msgLower, "region") || !cityExplicit || strings.EqualFold(regionCodeForQuery, cityCodeForQuery) {
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
						// Clear any stale empty markers once we have data.
						if toolData != nil {
							delete(toolData, "empty_data")
						}
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
	// Keep a single source of truth for routing/tool-calling: ChatStream.
	// ChatStream will handle conversation hydration + message persistence.
	return c.ChatStream(ctx, ownerKey, req, nil)
}

func (c *ChatService) ChatStream(ctx context.Context, ownerKey string, req models.ChatRequest, onToken func(string)) (models.ChatResponse, error) {
	conversationID := strings.TrimSpace(req.ConversationID)
	header := c.buildInterpretationHeader(req, conversationID)
	streamedHeader := false
	onTokenWrapped := onToken
	if onToken != nil && strings.TrimSpace(header) != "" {
		onTokenWrapped = func(tok string) {
			if !streamedHeader {
				streamedHeader = true
				onToken(header + "\n" + tok)
				return
			}
			onToken(tok)
		}
	}
	if conversationID != "" {
		c.ensureConversationStateHydrated(ctx, ownerKey, conversationID)
		_ = c.Store.AppendMessage(ctx, ownerKey, conversationID, "user", req.Message)
	}
	if conversationID != "" {
		st := c.getConversationState(conversationID)
		if st != nil && st.PendingHandler == "deviceTelemetry" {
			// Pending telemetry should not hijack unrelated analytical queries.
			msgLower := strings.ToLower(req.Message)
			if strings.Contains(msgLower, "analytic") || strings.Contains(msgLower, "pop") || strings.Contains(msgLower, "stat") || strings.Contains(msgLower, "poster") {
				c.clearPending(conversationID)
			} else {
			hostTokens := detectHostTokens(req.Message)
			shouldResolve := false
			if len(hostTokens) == 0 {
				shouldResolve = true
			} else {
				msgHasSpaces := strings.Contains(strings.TrimSpace(req.Message), " ")
				candidate := strings.ToLower(strings.TrimSpace(hostTokens[0]))
				parts := strings.Split(strings.ReplaceAll(candidate, "_", "-"), "-")
				if msgHasSpaces || len(parts) < 3 {
					shouldResolve = true
				}
			}
			if shouldResolve {
				if host, step := c.resolveHostFromDeviceName(ctx, conversationID, req.Message); strings.TrimSpace(host) != "" {
					msg := strings.TrimSpace(st.PendingMessage)
					if msg == "" {
						msg = "show telemetry"
					}
					c.clearPending(conversationID)
					req2 := req
					req2.Message = msg + " " + host
					resp, handled, err := c.handleDeviceTelemetry(ctx, req2, onToken)
					if handled {
						if step != nil {
							resp.Steps = append([]models.Step{*step}, resp.Steps...)
						}
						return resp, err
					}
				}
				c.clearPending(conversationID)
				answer := "I couldn't find a device matching that name. Please reply with the host/server id (for example: moco-brt-briggs-001)."
				answer = prefixIfNeeded(header, answer)
				if onTokenWrapped != nil {
					onTokenWrapped(answer)
				}
				return models.ChatResponse{Answer: answer}, nil
			}
			}
		}
	}

	if resp, handled, err := c.handleTopPostersFromCity(ctx, req, onTokenWrapped); handled {
		resp.Answer = prefixIfNeeded(header, resp.Answer)
		if conversationID != "" {
			_ = c.Store.AppendMessage(ctx, ownerKey, conversationID, "assistant", resp.Answer)
		}
		return resp, err
	}
	if resp, handled, err := c.handleTopDevicesFromCity(ctx, req, onTokenWrapped); handled {
		resp.Answer = prefixIfNeeded(header, resp.Answer)
		if conversationID != "" {
			_ = c.Store.AppendMessage(ctx, ownerKey, conversationID, "assistant", resp.Answer)
		}
		return resp, err
	}
	if resp, handled, err := c.handlePosterAnalyticsByID(ctx, req, onTokenWrapped); handled {
		resp.Answer = prefixIfNeeded(header, resp.Answer)
		if conversationID != "" {
			_ = c.Store.AppendMessage(ctx, ownerKey, conversationID, "assistant", resp.Answer)
		}
		return resp, err
	}
	if resp, handled, err := c.handlePosterMonthData(ctx, req, onTokenWrapped); handled {
		resp.Answer = prefixIfNeeded(header, resp.Answer)
		if conversationID != "" {
			_ = c.Store.AppendMessage(ctx, ownerKey, conversationID, "assistant", resp.Answer)
		}
		return resp, err
	}
	if resp, handled, err := c.handlePosterPlayCount(ctx, req, onTokenWrapped); handled {
		resp.Answer = prefixIfNeeded(header, resp.Answer)
		if conversationID != "" {
			_ = c.Store.AppendMessage(ctx, ownerKey, conversationID, "assistant", resp.Answer)
		}
		return resp, err
	}
	if resp, handled, err := c.handlePopForPosterID(ctx, req, onTokenWrapped); handled {
		resp.Answer = prefixIfNeeded(header, resp.Answer)
		if conversationID != "" {
			_ = c.Store.AppendMessage(ctx, ownerKey, conversationID, "assistant", resp.Answer)
		}
		return resp, err
	}
	if resp, handled, err := c.handleKioskPosterPlayCount(ctx, req, onTokenWrapped); handled {
		resp.Answer = prefixIfNeeded(header, resp.Answer)
		if conversationID != "" {
			_ = c.Store.AppendMessage(ctx, ownerKey, conversationID, "assistant", resp.Answer)
		}
		return resp, err
	}
	if resp, handled, err := c.handleKioskCountFromCity(ctx, req, onTokenWrapped); handled {
		resp.Answer = prefixIfNeeded(header, resp.Answer)
		if conversationID != "" {
			_ = c.Store.AppendMessage(ctx, ownerKey, conversationID, "assistant", resp.Answer)
		}
		return resp, err
	}
	if resp, handled, err := c.handlePopYesterdayByHost(ctx, req, onTokenWrapped); handled {
		resp.Answer = prefixIfNeeded(header, resp.Answer)
		if conversationID != "" {
			_ = c.Store.AppendMessage(ctx, ownerKey, conversationID, "assistant", resp.Answer)
		}
		return resp, err
	}
	if resp, handled, err := c.handlePopTodayByHost(ctx, req, onTokenWrapped); handled {
		resp.Answer = prefixIfNeeded(header, resp.Answer)
		if conversationID != "" {
			_ = c.Store.AppendMessage(ctx, ownerKey, conversationID, "assistant", resp.Answer)
		}
		return resp, err
	}
	if resp, handled, err := c.handlePopStatsGeneric(ctx, req, onTokenWrapped); handled {
		resp.Answer = prefixIfNeeded(header, resp.Answer)
		if conversationID != "" {
			_ = c.Store.AppendMessage(ctx, ownerKey, conversationID, "assistant", resp.Answer)
		}
		return resp, err
	}
	if resp, handled, err := c.handleVenueDevices(ctx, req, onTokenWrapped); handled {
		resp.Answer = prefixIfNeeded(header, resp.Answer)
		if conversationID != "" {
			_ = c.Store.AppendMessage(ctx, ownerKey, conversationID, "assistant", resp.Answer)
		}
		return resp, err
	}
	if resp, handled, err := c.handleDeviceVenues(ctx, req, onTokenWrapped); handled {
		resp.Answer = prefixIfNeeded(header, resp.Answer)
		if conversationID != "" {
			_ = c.Store.AppendMessage(ctx, ownerKey, conversationID, "assistant", resp.Answer)
		}
		return resp, err
	}
	if resp, handled, err := c.handleVenueSearchList(ctx, req, onTokenWrapped); handled {
		resp.Answer = prefixIfNeeded(header, resp.Answer)
		if conversationID != "" {
			_ = c.Store.AppendMessage(ctx, ownerKey, conversationID, "assistant", resp.Answer)
		}
		return resp, err
	}
	if resp, handled, err := c.handleLowUptimeDevices(ctx, req, onTokenWrapped); handled {
		resp.Answer = prefixIfNeeded(header, resp.Answer)
		if conversationID != "" {
			_ = c.Store.AppendMessage(ctx, ownerKey, conversationID, "assistant", resp.Answer)
		}
		return resp, err
	}
	if resp, handled, err := c.handleDeviceDetails(ctx, req, onTokenWrapped); handled {
		resp.Answer = prefixIfNeeded(header, resp.Answer)
		if conversationID != "" {
			_ = c.Store.AppendMessage(ctx, ownerKey, conversationID, "assistant", resp.Answer)
		}
		return resp, err
	}
	if resp, handled, err := c.handleDeviceTelemetry(ctx, req, onTokenWrapped); handled {
		resp.Answer = prefixIfNeeded(header, resp.Answer)
		if conversationID != "" {
			_ = c.Store.AppendMessage(ctx, ownerKey, conversationID, "assistant", resp.Answer)
		}
		return resp, err
	}
	if resp, handled, err := c.handleCampaignCreatives(ctx, req, onTokenWrapped); handled {
		resp.Answer = prefixIfNeeded(header, resp.Answer)
		if conversationID != "" {
			_ = c.Store.AppendMessage(ctx, ownerKey, conversationID, "assistant", resp.Answer)
		}
		return resp, err
	}
	if resp, handled, err := c.handleCreativeUpload(ctx, ownerKey, req); handled {
		resp.Answer = prefixIfNeeded(header, resp.Answer)
		if conversationID != "" {
			_ = c.Store.AppendMessage(ctx, ownerKey, conversationID, "assistant", resp.Answer)
		}
		return resp, err
	}
	if resp, handled, err := c.handlePosterDetails(ctx, req, onTokenWrapped); handled {
		resp.Answer = prefixIfNeeded(header, resp.Answer)
		if conversationID != "" {
			_ = c.Store.AppendMessage(ctx, ownerKey, conversationID, "assistant", resp.Answer)
		}
		return resp, err
	}

	data, steps, toolData := c.prefetchImpressions(ctx, req.Message)

	if c.MockMode {
		mockText := "(mock) I am running without OpenAI."
		if toolData != nil {
			mockText += " I fetched impressions data."
		}
		mockText = prefixIfNeeded(header, mockText)
		for i := 0; i < len(mockText); i += 20 {
			end := i + 20
			if end > len(mockText) {
				end = len(mockText)
			}
			if onTokenWrapped != nil {
				onTokenWrapped(mockText[i:end])
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
	full = prefixIfNeeded(header, full)
	for i := 0; i < len(full); i += 20 {
		end := i + 20
		if end > len(full) {
			end = len(full)
		}
		if onTokenWrapped != nil {
			onTokenWrapped(full[i:end])
		}
	}
	if conversationID != "" {
		_ = c.Store.AppendMessage(ctx, ownerKey, conversationID, "assistant", full)
	}

	return models.ChatResponse{Answer: full, Data: data, Steps: steps}, nil
}
