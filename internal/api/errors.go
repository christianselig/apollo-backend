package api

import "net/http"

func (a *api) errorResponse(w http.ResponseWriter, r *http.Request, status int, message string) {
	w.WriteHeader(status)
	w.Write([]byte(message))
}
