package api

import "net/http"

func (a *api) errorResponse(w http.ResponseWriter, _ *http.Request, status int, message string) {
	w.Header().Set("X-Apollo-Error", message)
	http.Error(w, message, status)
}
