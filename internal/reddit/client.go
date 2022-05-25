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

	RateLimitRemainingHeader = "x-ratelimit-remaining"
	RateLimitUsedHeader      = "x-ratelimit-used"
	RateLimitResetHeader     = "x-ratelimit-reset"
)

type Client struct {
	id          string
	secret      string
	client      *http.Client
	tracer      *httptrace.ClientTrace
	pool        *fastjson.ParserPool
	statsd      statsd.ClientInterface
	redis       *redis.Client
	defaultOpts []RequestOption
}

type RateLimitingInfo struct {
	Remaining float64
	Used      int
	Reset     int
	Present   bool
	Timestamp string
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

func NewClient(id, secret string, statsd statsd.ClientInterface, redis *redis.Client, connLimit int, opts ...RequestOption) *Client {
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
		opts,
	}
}

type AuthenticatedClient struct {
	client *Client

	redditId     string
	refreshToken string
	accessToken  string
}

func (rc *Client) NewAuthenticatedClient(redditId, refreshToken, accessToken string) *AuthenticatedClient {
	if redditId == "" {
		panic("requires a redditId")
	}

	if accessToken == "" {
		panic("requires an access token")
	}

	return &AuthenticatedClient{rc, redditId, refreshToken, accessToken}
}

func (rc *Client) doRequest(ctx context.Context, r *Request) ([]byte, *RateLimitingInfo, error) {
	req, err := r.HTTPRequest(ctx)
	if err != nil {
		return nil, nil, err
	}

	req = req.WithContext(httptrace.WithClientTrace(ctx, rc.tracer))

	start := time.Now()

	client := r.client
	if client == nil {
		client = rc.client
	}

	resp, err := client.Do(req)

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
	if resp.Header.Get(RateLimitRemainingHeader) != "" {
		rli.Present = true
		rli.Remaining, _ = strconv.ParseFloat(resp.Header.Get(RateLimitRemainingHeader), 64)
		rli.Used, _ = strconv.Atoi(resp.Header.Get(RateLimitUsedHeader))
		rli.Reset, _ = strconv.Atoi(resp.Header.Get(RateLimitResetHeader))
		rli.Timestamp = time.Now().String()
	}

	bb, err := ioutil.ReadAll(resp.Body)

	switch resp.StatusCode {
	case 200:
		return bb, rli, err
	case 401:
		return nil, rli, ErrInvalidBasicAuth
	case 403:
		return nil, rli, ErrOauthRevoked
	default:
		_ = rc.statsd.Incr("reddit.api.errors", r.tags, 0.1)
		return nil, rli, ServerError{string(bb), resp.StatusCode}
	}
}

func (rac *AuthenticatedClient) request(ctx context.Context, r *Request, rh ResponseHandler, empty interface{}) (interface{}, error) {
	if rac.isRateLimited() {
		return nil, ErrRateLimited
	}

	if err := rac.logRequest(); err != nil {
		return nil, err
	}

	bb, rli, err := rac.client.doRequest(ctx, r)

	if err != nil && err != ErrOauthRevoked && r.retry {
		for _, backoff := range backoffSchedule {
			done := make(chan struct{})

			time.AfterFunc(backoff, func() {
				_ = rac.client.statsd.Incr("reddit.api.retries", r.tags, 0.1)

				if err = rac.logRequest(); err != nil {
					done <- struct{}{}
					return
				}

				bb, rli, err = rac.client.doRequest(ctx, r)
				done <- struct{}{}
			})

			<-done

			if err == nil {
				break
			}
		}
	}

	if err != nil {
		_ = rac.client.statsd.Incr("reddit.api.errors", r.tags, 0.1)
		if strings.Contains(err.Error(), "http2: timeout awaiting response headers") {
			return nil, ErrTimeout
		}
		return nil, err
	} else {
		_ = rac.markRateLimited(rli)
	}

	if r.emptyResponseBytes > 0 && len(bb) == r.emptyResponseBytes {
		return empty, nil
	}

	parser := rac.client.pool.Get()
	defer rac.client.pool.Put(parser)

	val, err := parser.ParseBytes(bb)
	if err != nil {
		return nil, err
	}

	return rh(val), nil
}

func (rac *AuthenticatedClient) logRequest() error {
	if rac.redditId == SkipRateLimiting {
		return nil
	}

	return rac.client.redis.HIncrBy(context.Background(), "reddit:requests", rac.redditId, 1).Err()
}

func (rac *AuthenticatedClient) isRateLimited() bool {
	if rac.redditId == SkipRateLimiting {
		return false
	}

	key := fmt.Sprintf("reddit:%s:ratelimited", rac.redditId)
	_, err := rac.client.redis.Get(context.Background(), key).Result()
	return err != redis.Nil
}

func (rac *AuthenticatedClient) markRateLimited(rli *RateLimitingInfo) error {
	if rac.redditId == SkipRateLimiting {
		return ErrRequiresRedditId
	}

	if !rli.Present {
		return nil
	}

	if rli.Remaining > RequestRemainingBuffer {
		return nil
	}

	_ = rac.client.statsd.Incr("reddit.api.ratelimit", nil, 1.0)

	key := fmt.Sprintf("reddit:%s:ratelimited", rac.redditId)
	duration := time.Duration(rli.Reset) * time.Second
	info := fmt.Sprintf("%+v", *rli)

	if rli.Used > 2000 {
		_, err := rac.client.redis.HSet(context.Background(), "reddit:ratelimited:crazy", rac.redditId, info).Result()
		if err != nil {
			return err
		}
	}

	_, err := rac.client.redis.SetEX(context.Background(), key, info, duration).Result()
	return err
}

func (rac *AuthenticatedClient) RefreshTokens(ctx context.Context, opts ...RequestOption) (*RefreshTokenResponse, error) {
	opts = append(rac.client.defaultOpts, opts...)
	opts = append(opts, []RequestOption{
		WithTags([]string{"url:/api/v1/access_token"}),
		WithMethod("POST"),
		WithURL("https://www.reddit.com/api/v1/access_token"),
		WithBody("grant_type", "refresh_token"),
		WithBody("refresh_token", rac.refreshToken),
		WithBasicAuth(rac.client.id, rac.client.secret),
	}...)
	req := NewRequest(opts...)

	rtr, err := rac.request(ctx, req, NewRefreshTokenResponse, nil)
	if err != nil {
		return nil, err
	}

	ret := rtr.(*RefreshTokenResponse)
	if ret.RefreshToken == "" {
		ret.RefreshToken = rac.refreshToken
	}

	return ret, nil
}

func (rac *AuthenticatedClient) AboutInfo(ctx context.Context, fullname string, opts ...RequestOption) (*ListingResponse, error) {
	opts = append(rac.client.defaultOpts, opts...)
	opts = append(opts, []RequestOption{
		WithMethod("GET"),
		WithToken(rac.accessToken),
		WithURL("https://oauth.reddit.com/api/info"),
		WithQuery("id", fullname),
	}...)
	req := NewRequest(opts...)

	lr, err := rac.request(ctx, req, NewListingResponse, nil)
	if err != nil {
		return nil, err
	}

	return lr.(*ListingResponse), nil
}

func (rac *AuthenticatedClient) UserPosts(ctx context.Context, user string, opts ...RequestOption) (*ListingResponse, error) {
	url := fmt.Sprintf("https://oauth.reddit.com/u/%s/submitted", user)
	opts = append(rac.client.defaultOpts, opts...)
	opts = append(opts, []RequestOption{
		WithMethod("GET"),
		WithToken(rac.accessToken),
		WithURL(url),
	}...)
	req := NewRequest(opts...)

	lr, err := rac.request(ctx, req, NewListingResponse, nil)
	if err != nil {
		return nil, err
	}

	return lr.(*ListingResponse), nil
}

func (rac *AuthenticatedClient) UserAbout(ctx context.Context, user string, opts ...RequestOption) (*UserResponse, error) {
	url := fmt.Sprintf("https://oauth.reddit.com/u/%s/about", user)
	opts = append(rac.client.defaultOpts, opts...)
	opts = append(opts, []RequestOption{
		WithMethod("GET"),
		WithToken(rac.accessToken),
		WithURL(url),
	}...)
	req := NewRequest(opts...)
	ur, err := rac.request(ctx, req, NewUserResponse, nil)

	if err != nil {
		return nil, err
	}

	return ur.(*UserResponse), nil

}

func (rac *AuthenticatedClient) SubredditAbout(ctx context.Context, subreddit string, opts ...RequestOption) (*SubredditResponse, error) {
	url := fmt.Sprintf("https://oauth.reddit.com/r/%s/about", subreddit)
	opts = append(rac.client.defaultOpts, opts...)
	opts = append(opts, []RequestOption{
		WithMethod("GET"),
		WithToken(rac.accessToken),
		WithURL(url),
	}...)
	req := NewRequest(opts...)
	sr, err := rac.request(ctx, req, NewSubredditResponse, nil)

	if err != nil {
		return nil, err
	}

	return sr.(*SubredditResponse), nil
}

func (rac *AuthenticatedClient) subredditPosts(ctx context.Context, subreddit string, sort string, opts ...RequestOption) (*ListingResponse, error) {
	url := fmt.Sprintf("https://oauth.reddit.com/r/%s/%s", subreddit, sort)
	opts = append(rac.client.defaultOpts, opts...)
	opts = append(opts, []RequestOption{
		WithMethod("GET"),
		WithToken(rac.accessToken),
		WithURL(url),
	}...)
	req := NewRequest(opts...)

	lr, err := rac.request(ctx, req, NewListingResponse, nil)
	if err != nil {
		return nil, err
	}

	return lr.(*ListingResponse), nil
}

func (rac *AuthenticatedClient) SubredditHot(ctx context.Context, subreddit string, opts ...RequestOption) (*ListingResponse, error) {
	return rac.subredditPosts(ctx, subreddit, "hot", opts...)
}

func (rac *AuthenticatedClient) SubredditTop(ctx context.Context, subreddit string, opts ...RequestOption) (*ListingResponse, error) {
	return rac.subredditPosts(ctx, subreddit, "top", opts...)
}

func (rac *AuthenticatedClient) SubredditNew(ctx context.Context, subreddit string, opts ...RequestOption) (*ListingResponse, error) {
	return rac.subredditPosts(ctx, subreddit, "new", opts...)
}

func (rac *AuthenticatedClient) MessageInbox(ctx context.Context, opts ...RequestOption) (*ListingResponse, error) {
	opts = append(rac.client.defaultOpts, opts...)
	opts = append(opts, []RequestOption{
		WithTags([]string{"url:/api/v1/message/inbox"}),
		WithMethod("GET"),
		WithToken(rac.accessToken),
		WithURL("https://oauth.reddit.com/message/inbox"),
		WithEmptyResponseBytes(122),
	}...)
	req := NewRequest(opts...)

	lr, err := rac.request(ctx, req, NewListingResponse, EmptyListingResponse)
	if err != nil {
		return nil, err
	}
	return lr.(*ListingResponse), nil
}

func (rac *AuthenticatedClient) MessageUnread(ctx context.Context, opts ...RequestOption) (*ListingResponse, error) {
	opts = append(rac.client.defaultOpts, opts...)
	opts = append(opts, []RequestOption{
		WithTags([]string{"url:/api/v1/message/unread"}),
		WithMethod("GET"),
		WithToken(rac.accessToken),
		WithURL("https://oauth.reddit.com/message/unread"),
		WithEmptyResponseBytes(122),
	}...)

	req := NewRequest(opts...)

	lr, err := rac.request(ctx, req, NewListingResponse, EmptyListingResponse)
	if err != nil {
		return nil, err
	}
	return lr.(*ListingResponse), nil
}

func (rac *AuthenticatedClient) Me(ctx context.Context, opts ...RequestOption) (*MeResponse, error) {
	opts = append(rac.client.defaultOpts, opts...)
	opts = append(opts, []RequestOption{
		WithTags([]string{"url:/api/v1/me"}),
		WithMethod("GET"),
		WithToken(rac.accessToken),
		WithURL("https://oauth.reddit.com/api/v1/me"),
	}...)

	req := NewRequest(opts...)
	mr, err := rac.request(ctx, req, NewMeResponse, nil)
	if err != nil {
		return nil, err
	}
	return mr.(*MeResponse), nil
}
