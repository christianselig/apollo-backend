package reddit

import (
	"fmt"
	"strings"
	"time"

	"github.com/valyala/fastjson"
)

type ResponseHandler func(*fastjson.Value) interface{}

type Error struct {
	Message    string `json:"message"`
	Code       int    `json:"error"`
	StatusCode int
}

func (err *Error) Error() string {
	return fmt.Sprintf("%s (%d)", err.Message, err.Code)
}

func NewError(val *fastjson.Value, status int) *Error {
	err := &Error{}

	err.Message = string(val.GetStringBytes("message"))
	err.Code = val.GetInt("error")

	return err
}

type RefreshTokenResponse struct {
	AccessToken  string        `json:"access_token"`
	RefreshToken string        `json:"refresh_token"`
	Expiry       time.Duration `json:"expires_in"`
}

func NewRefreshTokenResponse(val *fastjson.Value) interface{} {
	rtr := &RefreshTokenResponse{}

	rtr.AccessToken = string(val.GetStringBytes("access_token"))
	rtr.RefreshToken = string(val.GetStringBytes("refresh_token"))
	rtr.Expiry = time.Duration(val.GetInt("expires_in")) * time.Second

	return rtr
}

type MeResponse struct {
	ID   string `json:"id"`
	Name string
}

func (mr *MeResponse) NormalizedUsername() string {
	return strings.ToLower(mr.Name)
}

func NewMeResponse(val *fastjson.Value) interface{} {
	mr := &MeResponse{}

	mr.ID = string(val.GetStringBytes("id"))
	mr.Name = string(val.GetStringBytes("name"))

	return mr
}

type Thing struct {
	Kind          string    `json:"kind"`
	ID            string    `json:"id"`
	Type          string    `json:"type"`
	Author        string    `json:"author"`
	Subject       string    `json:"subject"`
	Body          string    `json:"body"`
	CreatedAt     time.Time `json:"created_utc"`
	Context       string    `json:"context"`
	ParentID      string    `json:"parent_id"`
	LinkTitle     string    `json:"link_title"`
	Destination   string    `json:"dest"`
	Subreddit     string    `json:"subreddit"`
	SubredditType string    `json:"subreddit_type"`
	Score         int64     `json:"score"`
	SelfText      string    `json:"selftext"`
	Title         string    `json:"title"`
	URL           string    `json:"url"`
	Flair         string    `json:"flair"`
	Thumbnail     string    `json:"thumbnail"`
	Over18        bool      `json:"over_18"`
}

func (t *Thing) FullName() string {
	return fmt.Sprintf("%s_%s", t.Kind, t.ID)
}

func (t *Thing) IsDeleted() bool {
	return t.Author == "[deleted]"
}

func NewThing(val *fastjson.Value) *Thing {
	t := &Thing{}

	t.Kind = string(val.GetStringBytes("kind"))

	data := val.Get("data")
	unix := int64(data.GetFloat64("created_utc"))

	t.ID = string(data.GetStringBytes("id"))
	t.Type = string(data.GetStringBytes("type"))
	t.Author = string(data.GetStringBytes("author"))
	t.Subject = string(data.GetStringBytes("subject"))
	t.Body = string(data.GetStringBytes("body"))
	t.CreatedAt = time.Unix(unix, 0).UTC()
	t.Context = string(data.GetStringBytes("context"))
	t.ParentID = string(data.GetStringBytes("parent_id"))
	t.LinkTitle = string(data.GetStringBytes("link_title"))
	t.Destination = string(data.GetStringBytes("dest"))
	t.Subreddit = string(data.GetStringBytes("subreddit"))
	t.SubredditType = string(data.GetStringBytes("subreddit_type"))

	t.Score = data.GetInt64("score")
	t.Title = string(data.GetStringBytes("title"))
	t.SelfText = string(data.GetStringBytes("selftext"))
	t.URL = string(data.GetStringBytes("url"))
	t.Flair = string(data.GetStringBytes("link_flair_text"))
	t.Thumbnail = string(data.GetStringBytes("thumbnail"))
	t.Over18 = data.GetBool("over_18")

	return t
}

type ListingResponse struct {
	Count    int
	Children []*Thing
	After    string
	Before   string
}

func NewListingResponse(val *fastjson.Value) interface{} {
	lr := &ListingResponse{}

	data := val.Get("data")
	children := data.GetArray("children")

	lr.After = string(data.GetStringBytes("after"))
	lr.Before = string(data.GetStringBytes("before"))
	lr.Count = len(children)

	if lr.Count == 0 {
		return lr
	}

	lr.Children = make([]*Thing, lr.Count)
	for i := 0; i < lr.Count; i++ {
		lr.Children[i] = NewThing(children[i])
	}

	return lr
}

type SubredditResponse struct {
	Thing

	Name        string
	Quarantined bool
}

func NewSubredditResponse(val *fastjson.Value) interface{} {
	sr := &SubredditResponse{}

	sr.Kind = string(val.GetStringBytes("kind"))

	data := val.Get("data")
	sr.ID = string(data.GetStringBytes("id"))
	sr.Name = string(data.GetStringBytes("display_name"))
	sr.Quarantined = data.GetBool("quarantined")

	return sr
}

type UserResponse struct {
	Thing

	AcceptFollowers bool
	Name            string
}

func NewUserResponse(val *fastjson.Value) interface{} {
	ur := &UserResponse{}
	ur.Kind = string(val.GetStringBytes("kind"))

	data := val.Get("data")
	ur.ID = string(data.GetStringBytes("id"))
	ur.Name = string(data.GetStringBytes("name"))
	ur.AcceptFollowers = data.GetBool("accept_followers")

	return ur
}

var EmptyListingResponse = &ListingResponse{}
