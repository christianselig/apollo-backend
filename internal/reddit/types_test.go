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

	assert.Equal(t, "1ia22", me.ID)
	assert.Equal(t, "changelog", me.Name)
}
