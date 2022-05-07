package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
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
	anr := &accountNotificationsRequest{}
	if err := json.NewDecoder(r.Body).Decode(anr); err != nil {
		a.errorResponse(w, r, 500, err.Error())
		return
	}

	vars := mux.Vars(r)
	apns := vars["apns"]
	rid := vars["redditID"]

	ctx := context.Background()

	dev, err := a.deviceRepo.GetByAPNSToken(ctx, apns)
	if err != nil {
		a.errorResponse(w, r, 500, err.Error())
		return
	}

	acct, err := a.accountRepo.GetByRedditID(ctx, rid)
	if err != nil {
		a.errorResponse(w, r, 500, err.Error())
		return
	}

	if err := a.deviceRepo.SetNotifiable(ctx, &dev, &acct, anr.InboxNotifications, anr.WatcherNotifications, anr.GlobalMute); err != nil {
		a.errorResponse(w, r, 500, err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (a *api) getNotificationsAccountHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	apns := vars["apns"]
	rid := vars["redditID"]

	ctx := context.Background()

	dev, err := a.deviceRepo.GetByAPNSToken(ctx, apns)
	if err != nil {
		a.errorResponse(w, r, 500, err.Error())
		return
	}

	acct, err := a.accountRepo.GetByRedditID(ctx, rid)
	if err != nil {
		a.errorResponse(w, r, 500, err.Error())
		return
	}

	inbox, watchers, global, err := a.deviceRepo.GetNotifiable(ctx, &dev, &acct)
	if err != nil {
		a.errorResponse(w, r, 500, err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)

	an := &accountNotificationsRequest{InboxNotifications: inbox, WatcherNotifications: watchers, GlobalMute: global}
	_ = json.NewEncoder(w).Encode(an)
}

func (a *api) disassociateAccountHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	apns := vars["apns"]
	rid := vars["redditID"]

	ctx := context.Background()

	dev, err := a.deviceRepo.GetByAPNSToken(ctx, apns)
	if err != nil {
		a.errorResponse(w, r, 500, err.Error())
		return
	}

	acct, err := a.accountRepo.GetByRedditID(ctx, rid)
	if err != nil {
		a.errorResponse(w, r, 500, err.Error())
		return
	}

	if err := a.accountRepo.Disassociate(ctx, &acct, &dev); err != nil {
		a.errorResponse(w, r, 500, err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (a *api) upsertAccountsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	apns := vars["apns"]

	ctx := context.Background()

	dev, err := a.deviceRepo.GetByAPNSToken(ctx, apns)
	if err != nil {
		a.errorResponse(w, r, 422, err.Error())
		return
	}

	laccs, err := a.accountRepo.GetByAPNSToken(ctx, apns)
	if err != nil {
		a.errorResponse(w, r, 422, err.Error())
		return
	}

	accsMap := map[string]domain.Account{}
	for _, acc := range laccs {
		accsMap[acc.NormalizedUsername()] = acc
	}

	var raccs []domain.Account
	if err := json.NewDecoder(r.Body).Decode(&raccs); err != nil {
		a.errorResponse(w, r, 422, err.Error())
		return
	}
	for _, acc := range raccs {
		delete(accsMap, acc.NormalizedUsername())

		ac := a.reddit.NewAuthenticatedClient(reddit.SkipRateLimiting, acc.RefreshToken, acc.AccessToken)
		tokens, err := ac.RefreshTokens()
		if err != nil {
			a.errorResponse(w, r, 422, err.Error())
			return
		}

		// Reset expiration timer
		acc.ExpiresAt = time.Now().Unix() + 3540
		acc.RefreshToken = tokens.RefreshToken
		acc.AccessToken = tokens.AccessToken

		ac = a.reddit.NewAuthenticatedClient(reddit.SkipRateLimiting, acc.RefreshToken, acc.AccessToken)
		me, err := ac.Me()

		if err != nil {
			a.errorResponse(w, r, 422, err.Error())
			return
		}

		if me.NormalizedUsername() != acc.NormalizedUsername() {
			a.errorResponse(w, r, 422, "nice try")
			return
		}

		// Set account ID from Reddit
		acc.AccountID = me.ID

		if err := a.accountRepo.CreateOrUpdate(ctx, &acc); err != nil {
			a.errorResponse(w, r, 422, err.Error())
			return
		}

		_ = a.accountRepo.Associate(ctx, &acc, &dev)
	}

	for _, acc := range accsMap {
		fmt.Println(acc.NormalizedUsername())
		_ = a.accountRepo.Disassociate(ctx, &acc, &dev)
	}

	go func(apns string) {
		url := fmt.Sprintf("https://apollopushserver.xyz/api/new-server-addition?apns_token=%s", apns)
		req, err := http.NewRequest("POST", url, nil)
		req.Header.Set("Authentication", "Bearer 98g5j89aurqwfcsp9khlnvgd38fa15")

		if err != nil {
			a.logger.WithFields(logrus.Fields{
				"apns": apns,
			}).Error(err)
			return
		}

		resp, _ := a.httpClient.Do(req)
		resp.Body.Close()
	}(apns)

	w.WriteHeader(http.StatusOK)
}

func (a *api) upsertAccountHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	ctx := context.Background()
	var acct domain.Account

	if err := json.NewDecoder(r.Body).Decode(&acct); err != nil {
		a.logger.WithFields(logrus.Fields{
			"err": err,
		}).Info("failed to parse request json")
		a.errorResponse(w, r, 422, err.Error())
		return
	}

	// Here we check whether the account is supplied with a valid token.
	ac := a.reddit.NewAuthenticatedClient(reddit.SkipRateLimiting, acct.RefreshToken, acct.AccessToken)
	tokens, err := ac.RefreshTokens()
	if err != nil {
		a.logger.WithFields(logrus.Fields{
			"err": err,
		}).Info("failed to refresh token")
		a.errorResponse(w, r, 422, err.Error())
		return
	}

	// Reset expiration timer
	acct.ExpiresAt = time.Now().Unix() + 3540
	acct.RefreshToken = tokens.RefreshToken
	acct.AccessToken = tokens.AccessToken

	ac = a.reddit.NewAuthenticatedClient(reddit.SkipRateLimiting, acct.RefreshToken, acct.AccessToken)
	me, err := ac.Me()

	if err != nil {
		a.logger.WithFields(logrus.Fields{
			"err": err,
		}).Info("failed to grab user details from Reddit")
		a.errorResponse(w, r, 500, err.Error())
		return
	}

	if me.NormalizedUsername() != acct.NormalizedUsername() {
		a.logger.WithFields(logrus.Fields{
			"err": err,
		}).Info("user is not who they say they are")
		a.errorResponse(w, r, 422, "nice try")
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
		a.errorResponse(w, r, 500, err.Error())
		return
	}

	// Upsert account
	if err := a.accountRepo.CreateOrUpdate(ctx, &acct); err != nil {
		a.logger.WithFields(logrus.Fields{
			"err": err,
		}).Info("failed updating account in database")
		a.errorResponse(w, r, 500, err.Error())
		return
	}

	if err := a.accountRepo.Associate(ctx, &acct, &dev); err != nil {
		a.logger.WithFields(logrus.Fields{
			"err": err,
		}).Info("failed associating account with device")
		a.errorResponse(w, r, 500, err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
}
