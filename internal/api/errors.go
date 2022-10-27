package api

import (
	"errors"
	"net/http"
)

var ErrDuplicateAPNSToken = errors.New("duplicate apns token")

func (a *api) errorResponse(w http.ResponseWriter, _ *http.Request, status int, err error) {
	w.Header().Set("X-Apollo-Error", err.Error())
	http.Error(w, err.Error(), status)
}
