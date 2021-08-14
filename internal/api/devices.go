package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/christianselig/apollo-backend/internal/domain"
	"github.com/dustin/go-humanize/english"
	"github.com/gorilla/mux"
	"github.com/sideshow/apns2"
	"github.com/sideshow/apns2/payload"
	"github.com/sirupsen/logrus"
)

const notificationTitle = "📣 Hello, is this thing on?"

func (a *api) upsertDeviceHandler(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()

	d := &domain.Device{}
	if err := json.NewDecoder(r.Body).Decode(d); err != nil {
		a.errorResponse(w, r, 500, err.Error())
		return
	}

	if err := a.deviceRepo.CreateOrUpdate(ctx, d); err != nil {
		a.errorResponse(w, r, 500, err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (a *api) testDeviceHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	ctx := context.Background()
	tok := vars["apns"]

	d, err := a.deviceRepo.GetByAPNSToken(ctx, tok)
	if err != nil {
		a.logger.WithFields(logrus.Fields{
			"err": err,
		}).Info("failed fetching device from database")
		a.errorResponse(w, r, 500, err.Error())
		return
	}

	accs, err := a.accountRepo.GetByAPNSToken(ctx, tok)
	if err != nil {
		a.errorResponse(w, r, 500, err.Error())
		return
	}

	users := make([]string, len(accs))
	for i := range accs {
		users[i] = accs[i].Username
	}

	body := fmt.Sprintf("Active usernames are: %s. Tap me for more info!", english.OxfordWordSeries(users, "and"))
	notification := &apns2.Notification{}
	notification.Topic = "com.christianselig.Apollo"
	notification.DeviceToken = d.APNSToken
	notification.Payload = payload.
		NewPayload().
		Category("test-notification").
		Custom("test_accounts", strings.Join(users, ",")).
		AlertTitle(notificationTitle).
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

func (a *api) deleteDeviceHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ctx := context.Background()

	dev, err := a.deviceRepo.GetByAPNSToken(ctx, vars["apns"])
	if err != nil {
		a.errorResponse(w, r, 500, err.Error())
		return
	}

	accs, err := a.accountRepo.GetByAPNSToken(ctx, vars["apns"])
	if err != nil {
		a.errorResponse(w, r, 500, err.Error())
		return
	}

	for _, acc := range accs {
		a.accountRepo.Disassociate(ctx, &acc, &dev)
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

	body := fmt.Sprintf("Active usernames are: %s. Tap me for more info!", english.OxfordWordSeries(users, "and"))
	notification := &apns2.Notification{}
	notification.Topic = "com.christianselig.Apollo"
	notification.DeviceToken = d.APNSToken
	notification.Payload = payload.
		NewPayload().
		Category("test-notification").
		Custom("test_accounts", strings.Join(users, ",")).
		AlertTitle(notificationTitle).
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

func (a *api) deleteDeviceHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.WriteHeader(http.StatusOK)
}
