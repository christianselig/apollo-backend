package reddit

import (
	"fmt"
	"strings"

	"github.com/valyala/fastjson"
)

type Error struct {
	Message string `json:"message"`
	Code    int    `json:"error"`
}

func (err *Error) Error() string {
	return fmt.Sprintf("%s (%d)", err.Message, err.Code)
}

type Message struct {
	ID          string  `json:"id"`
	Kind        string  `json:"kind"`
	Type        string  `json:"type"`
	Author      string  `json:"author"`
	Subject     string  `json:"subject"`
	Body        string  `json:"body"`
	CreatedAt   float64 `json:"created_utc"`
	Context     string  `json:"context"`
	ParentID    string  `json:"parent_id"`
	LinkTitle   string  `json:"link_title"`
	Destination string  `json:"dest"`
	Subreddit   string  `json:"subreddit"`
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

type MeResponse struct {
	ID   string `json:"id"`
	Name string
}

func (mr *MeResponse) NormalizedUsername() string {
	return strings.ToLower(mr.Name)
}

func NewMeResponse(val *fastjson.Value) *MeResponse {
	mr := &MeResponse{}

	mr.ID = string(val.GetStringBytes("id"))
	mr.Name = string(val.GetStringBytes("name"))

	return mr
}

type Thing struct {
	Kind        string  `json:"kind"`
	ID          string  `json:"id"`
	Type        string  `json:"type"`
	Author      string  `json:"author"`
	Subject     string  `json:"subject"`
	Body        string  `json:"body"`
	CreatedAt   float64 `json:"created_utc"`
	Context     string  `json:"context"`
	ParentID    string  `json:"parent_id"`
	LinkTitle   string  `json:"link_title"`
	Destination string  `json:"dest"`
	Subreddit   string  `json:"subreddit"`
}

func NewThing(val *fastjson.Value) *Thing {
	t := &Thing{}

	t.Kind = string(val.GetStringBytes("kind"))

	data := val.Get("data")

	t.ID = string(data.GetStringBytes("id"))
	t.Type = string(data.GetStringBytes("type"))
	t.Author = string(data.GetStringBytes("author"))
	t.Subject = string(data.GetStringBytes("subject"))
	t.Body = string(data.GetStringBytes("body"))
	t.CreatedAt = data.GetFloat64("created_utc")
	t.Context = string(data.GetStringBytes("context"))
	t.ParentID = string(data.GetStringBytes("parent_id"))
	t.LinkTitle = string(data.GetStringBytes("link_title"))
	t.Destination = string(data.GetStringBytes("dest"))
	t.Subreddit = string(data.GetStringBytes("subreddit"))

	return t
}

type ListingResponse struct {
	Count    int
	Children []*Thing
	After    string
	Before   string
}

func NewListingResponse(val *fastjson.Value) *ListingResponse {
	lr := &ListingResponse{}

	data := val.Get("data")
	lr.After = string(data.GetStringBytes("after"))
	lr.Before = string(data.GetStringBytes("before"))
	lr.Count = data.GetInt("dist")
	lr.Children = make([]*Thing, lr.Count)

	children := data.GetArray("children")
	for i := 0; i < lr.Count; i++ {
		t := NewThing(children[i])
		lr.Children[i] = t
	}

	return lr
}
