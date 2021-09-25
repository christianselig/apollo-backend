package reddit

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptrace"
	"regexp"
	"strings"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/valyala/fastjson"
)

type Client struct {
	id     string
	secret string
	client *http.Client
	tracer *httptrace.ClientTrace
	pool   *fastjson.ParserPool
	statsd statsd.ClientInterface
}

func SplitID(id string) (string, string) {
	if parts := strings.Split(id, "_"); len(parts) == 2 {
		return parts[0], parts[1]
	}

	return "", ""
}

func PostIDFromContext(context string) string {
	exps := []*regexp.Regexp{
		regexp.MustCompile(`\/r\/[^\/]*\/comments\/([^\/]*)\/.*`),
	}

	for _, exp := range exps {
		matches := exp.FindStringSubmatch(context)
		if len(matches) != 2 {
			continue
		}
		return matches[1]
	}
	return ""
}

func NewClient(id, secret string, statsd statsd.ClientInterface, connLimit int) *Client {
	tracer := &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			if info.Reused {
				_ = statsd.Incr("reddit.api.connections.reused", []string{}, 0.1)
				if info.WasIdle {
					idleTime := float64(int64(info.IdleTime) / int64(time.Millisecond))
					_ = statsd.Histogram("reddit.api.connections.idle_time", idleTime, []string{}, 0.1)
				}
			} else {
				_ = statsd.Incr("reddit.api.connections.created", []string{}, 0.1)
			}
		},
	}

	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = connLimit / 4 / 100
	t.MaxConnsPerHost = connLimit / 100
	t.MaxIdleConnsPerHost = connLimit / 4 / 100
	t.IdleConnTimeout = 60 * time.Second
	t.ResponseHeaderTimeout = 5 * time.Second

	client := &http.Client{Transport: t}

	pool := &fastjson.ParserPool{}

	return &Client{
		id,
		secret,
		client,
		tracer,
		pool,
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

func (rac *AuthenticatedClient) request(r *Request, rh ResponseHandler, empty interface{}) (interface{}, error) {
	req, err := r.HTTPRequest()
	if err != nil {
		return nil, err
	}

	req = req.WithContext(httptrace.WithClientTrace(req.Context(), rac.tracer))

	start := time.Now()
	resp, err := rac.client.Do(req)
	_ = rac.statsd.Incr("reddit.api.calls", r.tags, 0.1)
	_ = rac.statsd.Histogram("reddit.api.latency", float64(time.Since(start).Milliseconds()), r.tags, 0.1)

	if err != nil {
		_ = rac.statsd.Incr("reddit.api.errors", r.tags, 0.1)
		if strings.Contains(err.Error(), "http2: timeout awaiting response headers") {
			return nil, ErrTimeout
		}
		return nil, err
	}
	defer resp.Body.Close()

	bb, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		_ = rac.statsd.Incr("reddit.api.errors", r.tags, 0.1)
		return nil, err
	}

	if resp.StatusCode != 200 {
		_ = rac.statsd.Incr("reddit.api.errors", r.tags, 0.1)

		// Try to parse a json error. Otherwise we generate a generic one
		parser := rac.pool.Get()
		defer rac.pool.Put(parser)

		val, jerr := parser.ParseBytes(bb)
		if jerr != nil {
			return nil, ServerError{resp.StatusCode}
		}
		return nil, NewError(val, resp.StatusCode)
	}

	if r.emptyResponseBytes > 0 && len(bb) == r.emptyResponseBytes {
		return empty, nil
	}

	parser := rac.pool.Get()
	defer rac.pool.Put(parser)

	val, err := parser.ParseBytes(bb)
	if err != nil {
		return nil, err
	}

	return rh(val), nil
}

func (rac *AuthenticatedClient) RefreshTokens() (*RefreshTokenResponse, error) {
	req := NewRequest(
		WithTags([]string{"url:/api/v1/access_token"}),
		WithMethod("POST"),
		WithURL("https://www.reddit.com/api/v1/access_token"),
		WithBody("grant_type", "refresh_token"),
		WithBody("refresh_token", rac.refreshToken),
		WithBasicAuth(rac.id, rac.secret),
	)

	rtr, err := rac.request(req, NewRefreshTokenResponse, nil)
	if err != nil {
		switch rerr := err.(type) {
		case ServerError:
			if rerr.StatusCode == 400 {
				return nil, ErrOauthRevoked
			}
		}

		return nil, err
	}

	ret := rtr.(*RefreshTokenResponse)
	if ret.RefreshToken == "" {
		ret.RefreshToken = rac.refreshToken
	}

	return ret, nil
}

func (rac *AuthenticatedClient) SubredditAbout(subreddit string, opts ...RequestOption) (*SubredditResponse, error) {
	url := fmt.Sprintf("https://oauth.reddit.com/r/%s/about.json", subreddit)
	opts = append([]RequestOption{
		WithMethod("GET"),
		WithToken(rac.accessToken),
		WithURL(url),
	}, opts...)
	req := NewRequest(opts...)
	sr, err := rac.request(req, NewSubredditResponse, nil)

	if err != nil {
		return nil, err
	}

	return sr.(*SubredditResponse), nil
}

func (rac *AuthenticatedClient) subredditPosts(subreddit string, sort string, opts ...RequestOption) (*ListingResponse, error) {
	url := fmt.Sprintf("https://oauth.reddit.com/r/%s/%s.json", subreddit, sort)
	opts = append([]RequestOption{
		WithMethod("GET"),
		WithToken(rac.accessToken),
		WithURL(url),
	}, opts...)
	req := NewRequest(opts...)

	lr, err := rac.request(req, NewListingResponse, EmptyListingResponse)
	if err != nil {
		return nil, err
	}

	return lr.(*ListingResponse), nil
}

func (rac *AuthenticatedClient) SubredditHot(subreddit string, opts ...RequestOption) (*ListingResponse, error) {
	return rac.subredditPosts(subreddit, "hot", opts...)
}

func (rac *AuthenticatedClient) SubredditNew(subreddit string, opts ...RequestOption) (*ListingResponse, error) {
	return rac.subredditPosts(subreddit, "new", opts...)
}

func (rac *AuthenticatedClient) MessageInbox(opts ...RequestOption) (*ListingResponse, error) {
	opts = append([]RequestOption{
		WithTags([]string{"url:/api/v1/message/inbox"}),
		WithMethod("GET"),
		WithToken(rac.accessToken),
		WithURL("https://oauth.reddit.com/message/inbox.json"),
		WithEmptyResponseBytes(122),
	}, opts...)
	req := NewRequest(opts...)

	lr, err := rac.request(req, NewListingResponse, EmptyListingResponse)
	if err != nil {
		switch rerr := err.(type) {
		case ServerError:
			if rerr.StatusCode == 403 {
				return nil, ErrOauthRevoked
			}
		}

		return nil, err
	}
	return lr.(*ListingResponse), nil
}

func (rac *AuthenticatedClient) MessageUnread(opts ...RequestOption) (*ListingResponse, error) {
	opts = append([]RequestOption{
		WithTags([]string{"url:/api/v1/message/unread"}),
		WithMethod("GET"),
		WithToken(rac.accessToken),
		WithURL("https://oauth.reddit.com/message/unread.json"),
		WithEmptyResponseBytes(122),
	}, opts...)

	req := NewRequest(opts...)

	lr, err := rac.request(req, NewListingResponse, EmptyListingResponse)
	if err != nil {
		switch rerr := err.(type) {
		case ServerError:
			if rerr.StatusCode == 403 {
				return nil, ErrOauthRevoked
			}
		}

		return nil, err
	}
	return lr.(*ListingResponse), nil
}

func (rac *AuthenticatedClient) Me() (*MeResponse, error) {
	req := NewRequest(
		WithTags([]string{"url:/api/v1/me"}),
		WithMethod("GET"),
		WithToken(rac.accessToken),
		WithURL("https://oauth.reddit.com/api/v1/me"),
	)

	mr, err := rac.request(req, NewMeResponse, nil)
	if err != nil {
		switch rerr := err.(type) {
		case ServerError:
			if rerr.StatusCode == 403 {
				return nil, ErrOauthRevoked
			}
		}

		return nil, err
	}
	return mr.(*MeResponse), nil
}
