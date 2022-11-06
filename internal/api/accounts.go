package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"go.uber.org/zap"

	"github.com/christianselig/apollo-backend/internal/domain"
	"github.com/christianselig/apollo-backend/internal/reddit"
)

type accountNotificationsRequest struct {
	InboxNotifications   bool `json:"inbox_notifications"`
	WatcherNotifications bool `json:"watcher_notifications"`
	GlobalMute           bool `json:"global_mute"`
}

func (a *api) notificationsAccountHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	anr := &accountNotificationsRequest{}
	if err := json.NewDecoder(r.Body).Decode(anr); err != nil {
		a.errorResponse(w, r, 500, err)
		return
	}

	vars := mux.Vars(r)
	apns := vars["apns"]
	rid := vars["redditID"]

	dev, err := a.deviceRepo.GetByAPNSToken(ctx, apns)
	if err != nil {
		a.errorResponse(w, r, 500, err)
		return
	}

	acct, err := a.accountRepo.GetByRedditID(ctx, rid)
	if err != nil {
		a.errorResponse(w, r, 500, err)
		return
	}

	if err := a.deviceRepo.SetNotifiable(ctx, &dev, &acct, anr.InboxNotifications, anr.WatcherNotifications, anr.GlobalMute); err != nil {
		a.errorResponse(w, r, 500, err)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (a *api) getNotificationsAccountHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	vars := mux.Vars(r)
	apns := vars["apns"]
	rid := vars["redditID"]

	dev, err := a.deviceRepo.GetByAPNSToken(ctx, apns)
	if err != nil {
		a.errorResponse(w, r, 500, err)
		return
	}

	acct, err := a.accountRepo.GetByRedditID(ctx, rid)
	if err != nil {
		a.errorResponse(w, r, 500, err)
		return
	}

	inbox, watchers, global, err := a.deviceRepo.GetNotifiable(ctx, &dev, &acct)
	if err != nil {
		a.errorResponse(w, r, 500, err)
		return
	}

	w.WriteHeader(http.StatusOK)

	an := &accountNotificationsRequest{InboxNotifications: inbox, WatcherNotifications: watchers, GlobalMute: global}
	_ = json.NewEncoder(w).Encode(an)
}

func (a *api) disassociateAccountHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	vars := mux.Vars(r)
	apns := vars["apns"]
	rid := vars["redditID"]

	dev, err := a.deviceRepo.GetByAPNSToken(ctx, apns)
	if err != nil {
		a.errorResponse(w, r, 500, err)
		return
	}

	acct, err := a.accountRepo.GetByRedditID(ctx, rid)
	if err != nil {
		a.errorResponse(w, r, 500, err)
		return
	}

	if err := a.accountRepo.Disassociate(ctx, &acct, &dev); err != nil {
		a.errorResponse(w, r, 500, err)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (a *api) upsertAccountsHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	vars := mux.Vars(r)
	apns := vars["apns"]

	dev, err := a.deviceRepo.GetByAPNSToken(ctx, apns)
	if err != nil {
		a.errorResponse(w, r, 422, err)
		return
	}

	laccs, err := a.accountRepo.GetByAPNSToken(ctx, apns)
	if err != nil {
		a.errorResponse(w, r, 422, err)
		return
	}

	accsMap := map[string]domain.Account{}
	for _, acc := range laccs {
		accsMap[acc.NormalizedUsername()] = acc
	}

	var raccs []domain.Account
	if err := json.NewDecoder(r.Body).Decode(&raccs); err != nil {
		a.errorResponse(w, r, 422, err)
		return
	}
	for _, acc := range raccs {
		delete(accsMap, acc.NormalizedUsername())

		rac := a.reddit.NewAuthenticatedClient(reddit.SkipRateLimiting, acc.RefreshToken, acc.AccessToken)
		tokens, err := rac.RefreshTokens(ctx)
		if err != nil {
			err := fmt.Errorf("failed to refresh tokens: %w", err)
			a.errorResponse(w, r, 422, err)
			return
		}

		// Reset expiration timer
		acc.TokenExpiresAt = time.Now().Add(tokens.Expiry)
		acc.RefreshToken = tokens.RefreshToken
		acc.AccessToken = tokens.AccessToken

		rac = a.reddit.NewAuthenticatedClient(reddit.SkipRateLimiting, tokens.RefreshToken, tokens.AccessToken)
		me, err := rac.Me(ctx)

		if err != nil {
			err := fmt.Errorf("failed to fetch user info: %w", err)
			a.errorResponse(w, r, 422, err)
			return
		}

		if me.NormalizedUsername() != acc.NormalizedUsername() {
			err := fmt.Errorf("wrong user: expected %s, got %s", me.NormalizedUsername(), acc.NormalizedUsername())
			a.errorResponse(w, r, 401, err)
			return
		}

		// Set account ID from Reddit
		acc.AccountID = me.ID

		mi, err := rac.MessageInbox(ctx, reddit.WithQuery("limit", "1"))
		if err != nil {
			a.errorResponse(w, r, 500, err)
			return
		}

		if mi.Count > 0 {
			acc.LastMessageID = mi.Children[0].FullName()
			acc.CheckCount = 1
		}

		if err := a.accountRepo.CreateOrUpdate(ctx, &acc); err != nil {
			a.errorResponse(w, r, 422, err)
			return
		}

		if err := a.accountRepo.Associate(ctx, &acc, &dev); err != nil {
			a.errorResponse(w, r, 422, err)
			return
		}
	}

	for _, acc := range accsMap {
		_ = a.accountRepo.Disassociate(ctx, &acc, &dev)
	}

	w.WriteHeader(http.StatusOK)
}

func (a *api) upsertAccountHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	vars := mux.Vars(r)

	var acct domain.Account

	if err := json.NewDecoder(r.Body).Decode(&acct); err != nil {
		a.logger.Error("failed to parse request json", zap.Error(err))
		a.errorResponse(w, r, 422, err)
		return
	}

	// Here we check whether the account is supplied with a valid token.
	rac := a.reddit.NewAuthenticatedClient(reddit.SkipRateLimiting, acct.RefreshToken, acct.AccessToken)
	tokens, err := rac.RefreshTokens(ctx)
	if err != nil {
		a.logger.Error("failed to refresh token", zap.Error(err))
		a.errorResponse(w, r, 422, err)
		return
	}

	// Reset expiration timer
	acct.TokenExpiresAt = time.Now().Add(tokens.Expiry)
	acct.RefreshToken = tokens.RefreshToken
	acct.AccessToken = tokens.AccessToken

	rac = a.reddit.NewAuthenticatedClient(reddit.SkipRateLimiting, acct.RefreshToken, acct.AccessToken)
	me, err := rac.Me(ctx)

	if err != nil {
		a.logger.Error("failed to grab user details from reddit", zap.Error(err))
		a.errorResponse(w, r, 500, err)
		return
	}

	if me.NormalizedUsername() != acct.NormalizedUsername() {
		err := fmt.Errorf("wrong user: expected %s, got %s", me.NormalizedUsername(), acct.NormalizedUsername())
		a.logger.Warn("user is not who they say they are", zap.Error(err))
		a.errorResponse(w, r, 401, err)
		return
	}

	// Set account ID from Reddit
	acct.AccountID = me.ID

	mi, err := rac.MessageInbox(ctx, reddit.WithQuery("limit", "1"))
	if err != nil {
		a.errorResponse(w, r, 500, err)
		return
	}

	if mi.Count > 0 {
		acct.LastMessageID = mi.Children[0].FullName()
		acct.CheckCount = 1
	}

	// Associate
	dev, err := a.deviceRepo.GetByAPNSToken(ctx, vars["apns"])
	if err != nil {
		a.logger.Error("failed to fetch device from database", zap.Error(err))
		a.errorResponse(w, r, 500, err)
		return
	}

	// Upsert account
	if err := a.accountRepo.CreateOrUpdate(ctx, &acct); err != nil {
		a.logger.Error("failed to update account", zap.Error(err))
		a.errorResponse(w, r, 500, err)
		return
	}

	if err := a.accountRepo.Associate(ctx, &acct, &dev); err != nil {
		a.logger.Error("failed to associate account with device", zap.Error(err))
		a.errorResponse(w, r, 500, err)
		return
	}

	w.WriteHeader(http.StatusOK)
}
