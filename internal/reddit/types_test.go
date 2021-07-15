package reddit

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/valyala/fastjson"
)

var (
	parser = &fastjson.Parser{}
)

func TestMeResponseParsing(t *testing.T) {
	bb, err := ioutil.ReadFile("testdata/me.json")
	assert.NoError(t, err)

	val, err := parser.ParseBytes(bb)
	assert.NoError(t, err)

	me := NewMeResponse(val)
	assert.NotNil(t, me)

	assert.Equal(t, "xgeee", me.ID)
	assert.Equal(t, "hugocat", me.Name)
}

func TestListingResponseParsing(t *testing.T) {
	bb, err := ioutil.ReadFile("testdata/message_inbox.json")
	assert.NoError(t, err)

	val, err := parser.ParseBytes(bb)
	assert.NoError(t, err)

	l := NewListingResponse(val)
	assert.NotNil(t, l)

	assert.Equal(t, 25, l.Count)
	assert.Equal(t, 25, len(l.Children))
	assert.Equal(t, "t1_h470gjv", l.After)
	assert.Equal(t, "", l.Before)

	thing := l.Children[0]
	assert.Equal(t, "t4", thing.Kind)
	assert.Equal(t, "138z6ke", thing.ID)
	assert.Equal(t, "unknown", thing.Type)
	assert.Equal(t, "iamthatis", thing.Author)
	assert.Equal(t, "how goes it", thing.Subject)
	assert.Equal(t, "how are you today", thing.Body)
	assert.Equal(t, 1626285395.0, thing.CreatedAt)
	assert.Equal(t, "hugocat", thing.Destination)

	thing = l.Children[6]
	assert.Equal(t, "/r/calicosummer/comments/ngcapc/hello_i_am_a_cat/h4q5j98/?context=3", thing.Context)
	assert.Equal(t, "t1_h46tec3", thing.ParentID)
	assert.Equal(t, "hello i am a cat", thing.LinkTitle)
	assert.Equal(t, "calicosummer", thing.Subreddit)
}
