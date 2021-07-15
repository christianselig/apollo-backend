package reddit

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

const userAgent = "server:apollo-backend:v1.0 (by /u/iamthatis)"

type Request struct {
	body               url.Values
	query              url.Values
	method             string
	token              string
	url                string
	auth               string
	tags               []string
	emptyResponseBytes int
}

type RequestOption func(*Request)

func NewRequest(opts ...RequestOption) *Request {
	req := &Request{url.Values{}, url.Values{}, "GET", "", "", "", nil, 0}
	for _, opt := range opts {
		opt(req)
	}

	return req
}

func (r *Request) HTTPRequest() (*http.Request, error) {
	req, err := http.NewRequest(r.method, r.url, strings.NewReader(r.body.Encode()))
	req.URL.RawQuery = r.query.Encode()

	req.Header.Add("User-Agent", userAgent)

	if r.token != "" {
		req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", r.token))
	}

	if r.auth != "" {
		req.Header.Add("Authorization", fmt.Sprintf("Basic %s", r.auth))
	}

	return req, err
}

func WithTags(tags []string) RequestOption {
	return func(req *Request) {
		req.tags = tags
	}
}

func WithMethod(method string) RequestOption {
	return func(req *Request) {
		req.method = method
	}
}

func WithURL(url string) RequestOption {
	return func(req *Request) {
		req.url = url
	}
}

func WithBasicAuth(user, password string) RequestOption {
	return func(req *Request) {
		encoded := base64.StdEncoding.EncodeToString([]byte(user + ":" + password))
		req.auth = encoded
	}
}

func WithToken(token string) RequestOption {
	return func(req *Request) {
		req.token = token
	}
}

func WithBody(key, val string) RequestOption {
	return func(req *Request) {
		req.body.Set(key, val)
	}
}

func WithQuery(key, val string) RequestOption {
	return func(req *Request) {
		req.query.Set(key, val)
	}
}

func WithEmptyResponseBytes(bytes int) RequestOption {
	return func(req *Request) {
		req.emptyResponseBytes = bytes
	}
}
