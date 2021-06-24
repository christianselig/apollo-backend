package reddit

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

const (
	tokenURL = "https://www.reddit.com/api/v1/access_token"
)

type Client struct {
	id     string
	secret string
}

func NewClient(id, secret string) *Client {
	return &Client{id, secret}
}

type AuthenticatedClient struct {
	*Client

	refreshToken string
	accessToken  string
	expiry       *time.Time
}

func (rc *Client) NewAuthenticatedClient(refreshToken, accessToken string) *AuthenticatedClient {
	return &AuthenticatedClient{rc, refreshToken, accessToken, nil}
}

func (rac *AuthenticatedClient) request(r *Request) ([]byte, error) {
	tr := &http.Transport{}
	client := &http.Client{Transport: tr}

	req, err := r.HTTPRequest()
	if err != nil {
		return nil, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	return ioutil.ReadAll(resp.Body)
}

func (rac *AuthenticatedClient) RefreshTokens() (*RefreshTokenResponse, error) {
	req := NewRequest(
		WithMethod("POST"),
		WithURL(tokenURL),
		WithBody("grant_type", "refresh_token"),
		WithBody("refresh_token", rac.refreshToken),
		WithBasicAuth(rac.id, rac.secret),
	)

	body, err := rac.request(req)

	if err != nil {
		return nil, err
	}

	rtr := &RefreshTokenResponse{}
	json.Unmarshal([]byte(body), rtr)
	return rtr, nil
}

func (rac *AuthenticatedClient) MessageInbox(from string) (*MessageListingResponse, error) {
	req := NewRequest(
		WithMethod("GET"),
		WithToken(rac.accessToken),
		WithURL("https://oauth.reddit.com/message/inbox.json"),
		WithQuery("before", from),
	)

	body, err := rac.request(req)

	if err != nil {
		return nil, err
	}

	mlr := &MessageListingResponse{}
	json.Unmarshal([]byte(body), mlr)
	return mlr, nil
}

type MeResponse struct {
	Name string
}

func (mr *MeResponse) NormalizedUsername() string {
	return strings.ToLower(mr.Name)
}

func (rac *AuthenticatedClient) Me() (*MeResponse, error) {
	req := NewRequest(
		WithMethod("GET"),
		WithToken(rac.accessToken),
		WithURL("https://oauth.reddit.com/api/v1/me"),
	)

	body, err := rac.request(req)

	if err != nil {
		return nil, err
	}

	mr := &MeResponse{}
	err = json.Unmarshal(body, mr)

	return mr, err
}
