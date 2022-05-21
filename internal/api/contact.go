package api

import (
	"encoding/json"
	"net/http"

	"github.com/smtp2go-oss/smtp2go-go"
)

type sendMessageRequest struct {
	Title string `json:"title"`
	Body  string `json:"body"`
}

func (a *api) contactHandler(w http.ResponseWriter, r *http.Request) {
	smr := &sendMessageRequest{}
	if err := json.NewDecoder(r.Body).Decode(smr); err != nil {
		a.errorResponse(w, r, 500, err)
		return
	}

	msg := &smtp2go.Email{
		From:     "🤖 Apollo API <robot@apollonotifications.com>",
		To:       []string{"ultrasurvey@apolloapp.io"},
		Subject:  smr.Title,
		TextBody: smr.Body,
	}

	if _, err := smtp2go.Send(msg); err != nil {
		a.errorResponse(w, r, 500, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}
