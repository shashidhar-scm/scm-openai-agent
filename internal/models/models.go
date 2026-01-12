package models

import "time"

type ChatRequest struct {
	Message        string `json:"message"`
	ConversationID string `json:"conversation_id"`
	Attachments    []ChatAttachment `json:"attachments,omitempty"`
}

type ChatAttachment struct {
	FileName    string `json:"file_name"`
	ContentType string `json:"content_type"`
	Base64      string `json:"base64"`
}

type ChatResponse struct {
	Answer string    `json:"answer"`
	Data   *ChatData `json:"data,omitempty"`
	Steps  []Step    `json:"steps,omitempty"`
}

type ChatData struct {
	CampaignImpressions *CampaignImpressions `json:"campaign_impressions,omitempty"`
}

type CampaignImpressions struct {
	CampaignID  string            `json:"campaign_id"`
	Impressions int64             `json:"impressions"`
	Posters     []PosterImpression `json:"posters,omitempty"`
}

type PosterImpression struct {
	PosterID    string `json:"poster_id"`
	PosterName  string `json:"poster_name"`
	Impressions int64  `json:"impressions"`
	PlayTime    *int64 `json:"play_time,omitempty"`
}

type Step struct {
	Tool       string `json:"tool"`
	CampaignID string `json:"campaign_id,omitempty"`
	Status     int    `json:"status"`
	Error      string `json:"error,omitempty"`
	Body       string `json:"body,omitempty"`
}

type Conversation struct {
	ConversationID string    `json:"conversation_id"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type Message struct {
	ID             int64     `json:"id"`
	ConversationID string    `json:"conversation_id"`
	Role           string    `json:"role"`
	Content        string    `json:"content"`
	CreatedAt      time.Time `json:"created_at"`
}

type ConversationsResponse struct {
	Data Conversation `json:"data"`
}

type MessagesResponse struct {
	Data []Message `json:"data"`
}
