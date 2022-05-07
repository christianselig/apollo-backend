package reddit_test

import (
	"io/ioutil"
	"testing"
	"time"

	"github.com/christianselig/apollo-backend/internal/reddit"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/fastjson"
)

var pool = &fastjson.ParserPool{}

func NewTestParser(t *testing.T) *fastjson.Parser {
	t.Helper()

	parser := pool.Get()

	t.Cleanup(func() {
		pool.Put(parser)
	})

	return parser
}

func TestMeResponseParsing(t *testing.T) {
	t.Parallel()

	bb, err := ioutil.ReadFile("testdata/me.json")
	assert.NoError(t, err)

	parser := NewTestParser(t)
	val, err := parser.ParseBytes(bb)
	assert.NoError(t, err)

	ret := reddit.NewMeResponse(val)
	me := ret.(*reddit.MeResponse)
	assert.NotNil(t, me)

	assert.Equal(t, "xgeee", me.ID)
	assert.Equal(t, "hugocat", me.Name)
}

func TestRefreshTokenResponseParsing(t *testing.T) {
	t.Parallel()

	bb, err := ioutil.ReadFile("testdata/refresh_token.json")
	assert.NoError(t, err)

	parser := NewTestParser(t)
	val, err := parser.ParseBytes(bb)
	assert.NoError(t, err)

	ret := reddit.NewRefreshTokenResponse(val)
	rtr := ret.(*reddit.RefreshTokenResponse)
	assert.NotNil(t, rtr)

	assert.Equal(t, "***REMOVED***", rtr.AccessToken)
	assert.Equal(t, "***REMOVED***", rtr.RefreshToken)
	assert.Equal(t, 1*time.Hour, rtr.Expiry)
}

func TestListingResponseParsing(t *testing.T) {
	t.Parallel()

	// Message list
	bb, err := ioutil.ReadFile("testdata/message_inbox.json")
	assert.NoError(t, err)

	parser := NewTestParser(t)
	val, err := parser.ParseBytes(bb)
	assert.NoError(t, err)

	ret := reddit.NewListingResponse(val)
	l := ret.(*reddit.ListingResponse)
	assert.NotNil(t, l)

	assert.Equal(t, 25, l.Count)
	assert.Equal(t, 25, len(l.Children))
	assert.Equal(t, "t1_h470gjv", l.After)
	assert.Equal(t, "", l.Before)

	thing := l.Children[0]
	created := time.Date(2021, time.July, 14, 17, 56, 35, 0, time.UTC)
	assert.Equal(t, "t4", thing.Kind)
	assert.Equal(t, "138z6ke", thing.ID)
	assert.Equal(t, "unknown", thing.Type)
	assert.Equal(t, "iamthatis", thing.Author)
	assert.Equal(t, "how goes it", thing.Subject)
	assert.Equal(t, "how are you today", thing.Body)
	assert.Equal(t, created, thing.CreatedAt)
	assert.Equal(t, "hugocat", thing.Destination)
	assert.Equal(t, "t4_138z6ke", thing.FullName())

	thing = l.Children[6]
	assert.Equal(t, "/r/calicosummer/comments/ngcapc/hello_i_am_a_cat/h4q5j98/?context=3", thing.Context)
	assert.Equal(t, "t1_h46tec3", thing.ParentID)
	assert.Equal(t, "hello i am a cat", thing.LinkTitle)
	assert.Equal(t, "calicosummer", thing.Subreddit)

	// Post list
	bb, err = ioutil.ReadFile("testdata/subreddit_new.json")
	assert.NoError(t, err)

	val, err = parser.ParseBytes(bb)
	assert.NoError(t, err)

	ret = reddit.NewListingResponse(val)
	l = ret.(*reddit.ListingResponse)
	assert.NotNil(t, l)

	assert.Equal(t, 100, l.Count)

	thing = l.Children[1]
	assert.Equal(t, "Riven boss", thing.Title)
	assert.Equal(t, "Question", thing.Flair)
	assert.Contains(t, thing.SelfText, "never done riven")
	assert.Equal(t, int64(1), thing.Score)
}

func TestSubredditResponseParsing(t *testing.T) {
	t.Parallel()

	bb, err := ioutil.ReadFile("testdata/subreddit_about.json")
	assert.NoError(t, err)

	parser := NewTestParser(t)
	val, err := parser.ParseBytes(bb)
	assert.NoError(t, err)

	ret := reddit.NewSubredditResponse(val)
	s := ret.(*reddit.SubredditResponse)
	assert.NotNil(t, s)

	assert.Equal(t, "t5", s.Kind)
	assert.Equal(t, "2vq0w", s.ID)
	assert.Equal(t, "DestinyTheGame", s.Name)
}

func TestUserResponseParsing(t *testing.T) {
	t.Parallel()

	bb, err := ioutil.ReadFile("testdata/user_about.json")
	assert.NoError(t, err)

	parser := NewTestParser(t)
	val, err := parser.ParseBytes(bb)
	assert.NoError(t, err)

	ret := reddit.NewUserResponse(val)
	u := ret.(*reddit.UserResponse)
	assert.NotNil(t, u)

	assert.Equal(t, "t2", u.Kind)
	assert.Equal(t, "1ia22", u.ID)
	assert.Equal(t, "changelog", u.Name)
	assert.Equal(t, true, u.AcceptFollowers)
}

func TestUserPostsParsing(t *testing.T) {
	t.Parallel()

	bb, err := ioutil.ReadFile("testdata/user_posts.json")
	assert.NoError(t, err)

	parser := NewTestParser(t)
	val, err := parser.ParseBytes(bb)
	assert.NoError(t, err)

	ret := reddit.NewListingResponse(val)
	ps := ret.(*reddit.ListingResponse)
	assert.NotNil(t, ps)

	post := ps.Children[0]

	assert.Equal(t, "public", post.SubredditType)
}
