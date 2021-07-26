package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/dustin/go-humanize/english"
	"github.com/julienschmidt/httprouter"
	"github.com/sideshow/apns2"
	"github.com/sideshow/apns2/payload"
	"github.com/sirupsen/logrus"

	"github.com/christianselig/apollo-backend/internal/data"
)

func (a *api) upsertDeviceHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	d := &data.Device{}
	if err := json.NewDecoder(r.Body).Decode(d); err != nil {
		a.errorResponse(w, r, 500, err.Error())
		return
	}

	if err := a.models.Devices.Upsert(d); err != nil {
		a.errorResponse(w, r, 500, err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (a *api) testDeviceHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	ctx := context.Background()
	tok := ps.ByName("apns")

	d, err := a.models.Devices.GetByAPNSToken(tok)
	if err != nil {
		a.logger.WithFields(logrus.Fields{
			"err": err,
		}).Info("failed fetching device from database")
		a.errorResponse(w, r, 500, err.Error())
		return
	}

	stmt := `
			SELECT username
			FROM accounts
			INNER JOIN devices_accounts ON devices_accounts.account_id = accounts.id
			WHERE devices_accounts.device_id = $1`
	rows, err := a.db.Query(ctx, stmt, d.ID)
	if err != nil {
		a.logger.WithFields(logrus.Fields{
			"apns": tok,
			"err":  err,
		}).Error("failed to fetch device accounts")
		return
	}
	defer rows.Close()

	var users []string
	for rows.Next() {
		var user string
		rows.Scan(&user)
		users = append(users, user)
	}

	body := fmt.Sprintf("Active usernames are: %s", english.OxfordWordSeries(users, "and"))
	notification := &apns2.Notification{}
	notification.Topic = "com.christianselig.Apollo"
	notification.Payload = payload.
		NewPayload().
		Category("test-notification").
		AlertBody(body)

	client := apns2.NewTokenClient(a.apns)
	if !d.Sandbox {
		client = client.Production()
	}

	if _, err := client.Push(notification); err != nil {
		a.logger.WithFields(logrus.Fields{
			"err": err,
		}).Info("failed to send test notification")
		a.errorResponse(w, r, 500, err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
}
