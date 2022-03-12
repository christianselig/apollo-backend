package reddit

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptrace"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/go-redis/redis/v8"
	"github.com/valyala/fastjson"
)

const (
	SkipRateLimiting       = "<SKIP_RATE_LIMITING>"
	RequestRemainingBuffer = 50

	RateLimitRemainingHeader = "X-Ratelimit-Remaining"
	RateLimitResetHeader     = "X-Ratelimit-Reset"
)

type Client struct {
	id     string
	secret string
	client *http.Client
	tracer *httptrace.ClientTrace
	pool   *fastjson.ParserPool
	statsd statsd.ClientInterface
	redis  *redis.Client
}

type RateLimitingInfo struct {
	Remaining float64
	Reset     int
	Present   bool
}

var backoffSchedule = []time.Duration{
	4 * time.Second,
	8 * time.Second,
	16 * time.Second,
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

func NewClient(id, secret string, statsd statsd.ClientInterface, redis *redis.Client, connLimit int) *Client {
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
		redis,
	}
}

type AuthenticatedClient struct {
	*Client

	redditId     string
	refreshToken string
	accessToken  string
}

func (rc *Client) NewAuthenticatedClient(redditId, refreshToken, accessToken string) *AuthenticatedClient {
	if redditId == "" {
		panic("requires a redditId")
	}

	return &AuthenticatedClient{rc, redditId, refreshToken, accessToken}
}

func (rc *Client) doRequest(r *Request) ([]byte, *RateLimitingInfo, error) {
	req, err := r.HTTPRequest()
	if err != nil {
		return nil, nil, err
	}

	req = req.WithContext(httptrace.WithClientTrace(req.Context(), rc.tracer))

	start := time.Now()

	resp, err := rc.client.Do(req)

	_ = rc.statsd.Incr("reddit.api.calls", r.tags, 0.1)
	_ = rc.statsd.Histogram("reddit.api.latency", float64(time.Since(start).Milliseconds()), r.tags, 0.1)

	if err != nil {
		_ = rc.statsd.Incr("reddit.api.errors", r.tags, 0.1)
		if strings.Contains(err.Error(), "http2: timeout awaiting response headers") {
			return nil, nil, ErrTimeout
		}
		return nil, nil, err
	}
	defer resp.Body.Close()

	rli := &RateLimitingInfo{Present: false}
	if _, ok := resp.Header[RateLimitRemainingHeader]; ok {
		rli.Present = true
		rli.Remaining, _ = strconv.ParseFloat(resp.Header.Get(RateLimitRemainingHeader), 64)
		rli.Reset, _ = strconv.Atoi(resp.Header.Get(RateLimitResetHeader))
	}

	if resp.StatusCode != 200 {
		_ = rc.statsd.Incr("reddit.api.errors", r.tags, 0.1)
		return nil, rli, ServerError{resp.StatusCode}
	}

	bb, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		_ = rc.statsd.Incr("reddit.api.errors", r.tags, 0.1)
		return nil, rli, err
	}
	return bb, rli, nil
}

func (rac *AuthenticatedClient) request(r *Request, rh ResponseHandler, empty interface{}) (interface{}, error) {
	if rl, err := rac.isRateLimited(); rl || err != nil {
		return nil, ErrRateLimited
	}

	bb, rli, err := rac.doRequest(r)

	if err != nil && r.retry {
		for _, backoff := range backoffSchedule {
			done := make(chan struct{})

			time.AfterFunc(backoff, func() {
				_ = rac.statsd.Incr("reddit.api.retries", r.tags, 0.1)
				bb, rli, err = rac.doRequest(r)
				done <- struct{}{}
			})

			<-done

			if err == nil {
				break
			}
		}
	}

	if err == nil && rli.Present && rli.Remaining <= RequestRemainingBuffer {
		_ = rac.statsd.Incr("reddit.api.ratelimit", r.tags, 0.1)
		rac.markRateLimited(rli.Remaining, time.Duration(rli.Reset)*time.Second)
	}

	if err != nil {
		_ = rac.statsd.Incr("reddit.api.errors", r.tags, 0.1)
		if strings.Contains(err.Error(), "http2: timeout awaiting response headers") {
			return nil, ErrTimeout
		}
		return nil, err
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

func (rac *AuthenticatedClient) isRateLimited() (bool, error) {
	if rac.redditId == SkipRateLimiting {
		return false, nil
	}

	key := fmt.Sprintf("reddit:%s:ratelimited", rac.redditId)
	_, err := rac.redis.Get(context.Background(), key).Result()

	if err == redis.Nil {
		return false, nil
	} else if err == nil {
		return true, nil
	} else {
		return false, err
	}
}

func (rac *AuthenticatedClient) markRateLimited(remaining float64, duration time.Duration) error {
	if rac.redditId == SkipRateLimiting {
		return ErrRequiresRedditId
	}

	key := fmt.Sprintf("reddit:%s:ratelimited", rac.redditId)
	_, err := rac.redis.SetEX(context.Background(), key, remaining, duration).Result()
	return err
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

func (rac *AuthenticatedClient) AboutInfo(fullname string, opts ...RequestOption) (*ListingResponse, error) {
	opts = append([]RequestOption{
		WithMethod("GET"),
		WithToken(rac.accessToken),
		WithURL("https://oauth.reddit.com/api/info"),
		WithQuery("id", fullname),
	}, opts...)
	req := NewRequest(opts...)

	lr, err := rac.request(req, NewListingResponse, nil)
	if err != nil {
		return nil, err
	}

	return lr.(*ListingResponse), nil
}

func (rac *AuthenticatedClient) UserPosts(user string, opts ...RequestOption) (*ListingResponse, error) {
	url := fmt.Sprintf("https://oauth.reddit.com/u/%s/submitted.json", user)
	opts = append([]RequestOption{
		WithMethod("GET"),
		WithToken(rac.accessToken),
		WithURL(url),
	}, opts...)
	req := NewRequest(opts...)

	lr, err := rac.request(req, NewListingResponse, nil)
	if err != nil {
		return nil, err
	}

	return lr.(*ListingResponse), nil
}

func (rac *AuthenticatedClient) UserAbout(user string, opts ...RequestOption) (*UserResponse, error) {
	url := fmt.Sprintf("https://oauth.reddit.com/u/%s/about.json", user)
	opts = append([]RequestOption{
		WithMethod("GET"),
		WithToken(rac.accessToken),
		WithURL(url),
	}, opts...)
	req := NewRequest(opts...)
	ur, err := rac.request(req, NewUserResponse, nil)

	if err != nil {
		return nil, err
	}

	return ur.(*UserResponse), nil

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

	lr, err := rac.request(req, NewListingResponse, nil)
	if err != nil {
		return nil, err
	}

	return lr.(*ListingResponse), nil
}

func (rac *AuthenticatedClient) SubredditHot(subreddit string, opts ...RequestOption) (*ListingResponse, error) {
	return rac.subredditPosts(subreddit, "hot", opts...)
}

func (rac *AuthenticatedClient) SubredditTop(subreddit string, opts ...RequestOption) (*ListingResponse, error) {
	return rac.subredditPosts(subreddit, "top", opts...)
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
