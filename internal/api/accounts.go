package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/christianselig/apollo-backend/internal/domain"
	"github.com/julienschmidt/httprouter"
	"github.com/sirupsen/logrus"
)

func (a *api) upsertAccountHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	ctx := context.Background()
	var acct domain.Account

	if err := json.NewDecoder(r.Body).Decode(acct); err != nil {
		a.logger.WithFields(logrus.Fields{
			"err": err,
		}).Info("failed to parse request json")
		a.errorResponse(w, r, 422, err.Error())
		return
	}

	// Here we check whether the account is supplied with a valid token.
	ac := a.reddit.NewAuthenticatedClient(acct.RefreshToken, acct.AccessToken)
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

	ac = a.reddit.NewAuthenticatedClient(acct.RefreshToken, acct.AccessToken)
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
	dev, err := a.deviceRepo.GetByAPNSToken(ctx, ps.ByName("apns"))
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

	if err := a.models.DevicesAccounts.Associate(acct.ID, d.ID); err != nil {
		a.logger.WithFields(logrus.Fields{
			"err": err,
		}).Info("failed associating account with device")
		a.errorResponse(w, r, 500, err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
}
