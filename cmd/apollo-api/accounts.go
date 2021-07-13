package main

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/sirupsen/logrus"

	"github.com/christianselig/apollo-backend/internal/data"
)

func (app *application) upsertAccountHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	a := &data.Account{}
	if err := json.NewDecoder(r.Body).Decode(a); err != nil {
		app.logger.WithFields(logrus.Fields{
			"err": err,
		}).Info("failed to parse request json")
		app.errorResponse(w, r, 422, err.Error())
		return
	}

	// Here we check whether the account is supplied with a valid token.
	ac := app.client.NewAuthenticatedClient(a.RefreshToken, a.AccessToken)
	tokens, err := ac.RefreshTokens()
	if err != nil {
		app.logger.WithFields(logrus.Fields{
			"err": err,
		}).Info("failed to refresh token")
		app.errorResponse(w, r, 422, err.Error())
		return
	}

	// Reset expiration timer
	a.ExpiresAt = time.Now().Unix() + 3540
	a.RefreshToken = tokens.RefreshToken
	a.AccessToken = tokens.AccessToken

	ac = app.client.NewAuthenticatedClient(a.RefreshToken, a.AccessToken)
	me, err := ac.Me()

	if err != nil {
		app.logger.WithFields(logrus.Fields{
			"err": err,
		}).Info("failed to grab user details from Reddit")
		app.errorResponse(w, r, 500, err.Error())
		return
	}

	if me.NormalizedUsername() != a.NormalizedUsername() {
		app.logger.WithFields(logrus.Fields{
			"err": err,
		}).Info("user is not who they say they are")
		app.errorResponse(w, r, 422, "nice try")
		return
	}

	// Set account ID from Reddit
	a.AccountID = me.ID

	// Associate
	d, err := app.models.Devices.GetByAPNSToken(ps.ByName("apns"))
	if err != nil {
		app.logger.WithFields(logrus.Fields{
			"err": err,
		}).Info("failed fetching device from database")
		app.errorResponse(w, r, 500, err.Error())
		return
	}

	// Upsert account
	if err := app.models.Accounts.Upsert(a); err != nil {
		app.logger.WithFields(logrus.Fields{
			"err": err,
		}).Info("failed updating account in database")
		app.errorResponse(w, r, 500, err.Error())
		return
	}

	if err := app.models.DevicesAccounts.Associate(a.ID, d.ID); err != nil {
		app.logger.WithFields(logrus.Fields{
			"err": err,
		}).Info("failed associating account with device")
		app.errorResponse(w, r, 500, err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
}
