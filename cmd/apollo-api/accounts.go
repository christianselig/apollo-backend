package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/julienschmidt/httprouter"

	"github.com/andremedeiros/apollo/internal/data"
)

func (app *application) upsertAccountHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	a := &data.Account{}
	if err := json.NewDecoder(r.Body).Decode(a); err != nil {
		fmt.Println("failing on decoding json")
		app.errorResponse(w, r, 500, err.Error())
		return
	}

	a.ExpiresAt = time.Now().Unix() + 3300

	// Here we check whether the account is supplied with a valid token.
	ac := app.client.NewAuthenticatedClient(a.RefreshToken, a.AccessToken)
	me, err := ac.Me()

	if err != nil {
		fmt.Println("failing on fetching remote user")
		app.errorResponse(w, r, 500, err.Error())
		return
	}

	if me.NormalizedUsername() != a.NormalizedUsername() {
		fmt.Println("failing on account username comparison")
		app.errorResponse(w, r, 500, "nice try")
		return
	}

	// Upsert account
	if err := app.models.Accounts.Upsert(a); err != nil {
		fmt.Println("failing on account upsert")
		app.errorResponse(w, r, 500, err.Error())
		return
	}

	// Associate
	d, err := app.models.Devices.GetByAPNSToken(ps.ByName("apns"))
	if err != nil {
		fmt.Println("failing on apns")
		app.errorResponse(w, r, 500, err.Error())
		return
	}

	if err := app.models.DevicesAccounts.Associate(a.ID, d.ID); err != nil {
		fmt.Println("failing on associate")
		app.errorResponse(w, r, 500, err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
}
