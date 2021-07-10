package reddit

import "fmt"

type Message struct {
	ID        string  `json:"id"`
	Kind      string  `json:"kind"`
	Type      string  `json:"type"`
	Author    string  `json:"author"`
	Subject   string  `json:"subject"`
	Body      string  `json:"body"`
	CreatedAt float64 `json:"created_utc"`
	Context   string  `json:"context"`
	ParentID  string  `json:"parent_id"`
	LinkTitle string  `json:"link_title"`
}

type MessageData struct {
	Message `json:"data"`
	Kind    string `json:"kind"`
}

func (md MessageData) FullName() string {
	return fmt.Sprintf("%s_%s", md.Kind, md.ID)
}

type MessageListing struct {
	Messages []MessageData `json:"children"`
}

type MessageListingResponse struct {
	MessageListing MessageListing `json:"data"`
}

type RefreshTokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
}
