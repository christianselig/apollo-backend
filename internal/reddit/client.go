package reddit

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptrace"
	"strings"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/valyala/fastjson"
)

const (
	tokenURL = "https://www.reddit.com/api/v1/access_token"
)

type Client struct {
	id     string
	secret string
	client *http.Client
	tracer *httptrace.ClientTrace
	parser *fastjson.Parser
	statsd *statsd.Client
}

func NewClient(id, secret string, statsd *statsd.Client) *Client {
	tracer := &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			if info.Reused {
				statsd.Incr("reddit.api.connections.reused", []string{}, 0.1)
				if info.WasIdle {
					idleTime := float64(int64(info.IdleTime) / int64(time.Millisecond))
					statsd.Histogram("reddit.api.connections.idle_time", idleTime, []string{}, 0.1)
				}
			} else {
				statsd.Incr("reddit.api.connections.created", []string{}, 0.1)
			}
		},
	}

	client := &http.Client{}

	parser := &fastjson.Parser{}

	return &Client{
		id,
		secret,
		client,
		tracer,
		parser,
		statsd,
	}
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
	req, err := r.HTTPRequest()
	if err != nil {
		return nil, err
	}

	req = req.WithContext(httptrace.WithClientTrace(req.Context(), rac.tracer))

	start := time.Now()
	resp, err := rac.client.Do(req)
	rac.statsd.Incr("reddit.api.calls", r.tags, 0.1)
	rac.statsd.Histogram("reddit.api.latency", float64(time.Now().Sub(start).Milliseconds()), r.tags, 0.1)

	if err != nil {
		rac.statsd.Incr("reddit.api.errors", r.tags, 0.1)
		return nil, err
	}
	defer resp.Body.Close()

	return ioutil.ReadAll(resp.Body)
}

func (rac *AuthenticatedClient) RefreshTokens() (*RefreshTokenResponse, error) {
	req := NewRequest(
		WithTags([]string{"url:/api/v1/access_token"}),
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
		WithTags([]string{"url:/api/v1/message/inbox"}),
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
		WithTags([]string{"url:/api/v1/me"}),
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
