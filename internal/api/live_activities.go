package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/christianselig/apollo-backend/internal/domain"
)

func (a *api) createLiveActivityHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	la := &domain.LiveActivity{}
	if err := json.NewDecoder(r.Body).Decode(la); err != nil {
		a.errorResponse(w, r, 400, err)
		return
	}

	if _, err := a.liveActivityRepo.Get(ctx, la.APNSToken); err == nil {
		a.errorResponse(w, r, 400, ErrDuplicateAPNSToken)
		return
	}

	ac := a.reddit.NewAuthenticatedClient(la.RedditAccountID, la.RefreshToken, la.AccessToken)
	rtr, err := ac.RefreshTokens(ctx)
	if err != nil {
		a.errorResponse(w, r, 500, err)
		return
	}

	la.RefreshToken = rtr.RefreshToken
	la.TokenExpiresAt = time.Now().Add(1 * time.Hour)

	if err := a.liveActivityRepo.Create(ctx, la); err != nil {
		a.errorResponse(w, r, 500, err)
		return
	}

	w.WriteHeader(http.StatusOK)
}
