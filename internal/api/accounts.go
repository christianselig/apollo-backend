package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"

	"github.com/christianselig/apollo-backend/internal/domain"
	"github.com/christianselig/apollo-backend/internal/reddit"
)

type accountNotificationsRequest struct {
	InboxNotifications   bool `json:"inbox_notifications"`
	WatcherNotifications bool `json:"watcher_notifications"`
	GlobalMute           bool `json:"global_mute"`
}

func (a *api) notificationsAccountHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

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
	ctx := r.Context()

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
	ctx := r.Context()

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
	ctx := r.Context()

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

		ac := a.reddit.NewAuthenticatedClient(reddit.SkipRateLimiting, acc.RefreshToken, acc.AccessToken)
		tokens, err := ac.RefreshTokens(ctx)
		if err != nil {
			a.errorResponse(w, r, 422, err)
			return
		}

		// Reset expiration timer
		acc.TokenExpiresAt = time.Now().Add(tokens.Expiry)
		acc.RefreshToken = tokens.RefreshToken
		acc.AccessToken = tokens.AccessToken

		ac = a.reddit.NewAuthenticatedClient(reddit.SkipRateLimiting, acc.RefreshToken, acc.AccessToken)
		me, err := ac.Me(ctx)

		if err != nil {
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
		fmt.Println(acc.NormalizedUsername())
		_ = a.accountRepo.Disassociate(ctx, &acc, &dev)
	}

	body := fmt.Sprintf(`{"apns_token": "%s"}`, apns)
	req, err := http.NewRequestWithContext(ctx, "POST", "https://apollopushserver.xyz/api/new-server-addition", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer 98g5j89aurqwfcsp9khlnvgd38fa15")

	if err != nil {
		a.logger.WithFields(logrus.Fields{
			"apns": apns,
		}).Error(err)
		return
	}

	w.WriteHeader(http.StatusOK)

	resp, _ := a.httpClient.Do(req)
	if err != nil {
		a.logger.WithFields(logrus.Fields{"err": err}).Error("failed to remove old client")
		return
	}
	resp.Body.Close()
}

func (a *api) upsertAccountHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	vars := mux.Vars(r)

	var acct domain.Account

	if err := json.NewDecoder(r.Body).Decode(&acct); err != nil {
		a.logger.WithFields(logrus.Fields{
			"err": err,
		}).Info("failed to parse request json")
		a.errorResponse(w, r, 422, err)
		return
	}

	// Here we check whether the account is supplied with a valid token.
	ac := a.reddit.NewAuthenticatedClient(reddit.SkipRateLimiting, acct.RefreshToken, acct.AccessToken)
	tokens, err := ac.RefreshTokens(ctx)
	if err != nil {
		a.logger.WithFields(logrus.Fields{
			"err": err,
		}).Info("failed to refresh token")
		a.errorResponse(w, r, 422, err)
		return
	}

	// Reset expiration timer
	acct.TokenExpiresAt = time.Now().Add(tokens.Expiry)
	acct.RefreshToken = tokens.RefreshToken
	acct.AccessToken = tokens.AccessToken

	ac = a.reddit.NewAuthenticatedClient(reddit.SkipRateLimiting, acct.RefreshToken, acct.AccessToken)
	me, err := ac.Me(ctx)

	if err != nil {
		a.logger.WithFields(logrus.Fields{
			"err": err,
		}).Info("failed to grab user details from Reddit")
		a.errorResponse(w, r, 500, err)
		return
	}

	if me.NormalizedUsername() != acct.NormalizedUsername() {
		err := fmt.Errorf("wrong user: expected %s, got %s", me.NormalizedUsername(), acct.NormalizedUsername())
		a.logger.WithFields(logrus.Fields{"err": err}).Warn("user is not who they say they are")
		a.errorResponse(w, r, 401, err)
		return
	}

	// Set account ID from Reddit
	acct.AccountID = me.ID

	// Associate
	dev, err := a.deviceRepo.GetByAPNSToken(ctx, vars["apns"])
	if err != nil {
		a.logger.WithFields(logrus.Fields{
			"err": err,
		}).Info("failed fetching device from database")
		a.errorResponse(w, r, 500, err)
		return
	}

	// Upsert account
	if err := a.accountRepo.CreateOrUpdate(ctx, &acct); err != nil {
		a.logger.WithFields(logrus.Fields{
			"err": err,
		}).Info("failed updating account in database")
		a.errorResponse(w, r, 500, err)
		return
	}

	if err := a.accountRepo.Associate(ctx, &acct, &dev); err != nil {
		a.logger.WithFields(logrus.Fields{
			"err": err,
		}).Info("failed associating account with device")
		a.errorResponse(w, r, 500, err)
		return
	}

	w.WriteHeader(http.StatusOK)
}
