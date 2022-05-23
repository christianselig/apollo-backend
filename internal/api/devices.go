package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/dustin/go-humanize/english"
	"github.com/gorilla/mux"
	"github.com/sideshow/apns2"
	"github.com/sideshow/apns2/payload"
	"go.uber.org/zap"

	"github.com/christianselig/apollo-backend/internal/domain"
)

const notificationTitle = "📣 Hello, is this thing on?"

func (a *api) upsertDeviceHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	d := &domain.Device{}
	if err := json.NewDecoder(r.Body).Decode(d); err != nil {
		a.errorResponse(w, r, 500, err)
		return
	}

	d.ExpiresAt = time.Now().Add(domain.DeviceReceiptCheckPeriodDuration)
	d.GracePeriodExpiresAt = d.ExpiresAt.Add(domain.DeviceGracePeriodAfterReceiptExpiry)

	if err := a.deviceRepo.CreateOrUpdate(ctx, d); err != nil {
		a.errorResponse(w, r, 500, err)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (a *api) testDeviceHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	vars := mux.Vars(r)
	tok := vars["apns"]

	d, err := a.deviceRepo.GetByAPNSToken(ctx, tok)
	if err != nil {
		a.logger.Error("failed to fetch device from database", zap.Error(err))
		a.errorResponse(w, r, 500, err)
		return
	}

	accs, err := a.accountRepo.GetByAPNSToken(ctx, tok)
	if err != nil {
		a.errorResponse(w, r, 500, err)
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
		AlertBody(body).
		MutableContent().
		Sound("traloop.wav")

	client := apns2.NewTokenClient(a.apns)
	if !d.Sandbox {
		client = client.Production()
	}

	if _, err := client.Push(notification); err != nil {
		a.logger.Info("failed to send test notification", zap.Error(err))
		a.errorResponse(w, r, 500, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (a *api) deleteDeviceHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	vars := mux.Vars(r)

	dev, err := a.deviceRepo.GetByAPNSToken(ctx, vars["apns"])
	if err != nil {
		a.errorResponse(w, r, 500, err)
		return
	}

	accs, err := a.accountRepo.GetByAPNSToken(ctx, vars["apns"])
	if err != nil {
		a.errorResponse(w, r, 500, err)
		return
	}

	for _, acc := range accs {
		_ = a.accountRepo.Disassociate(ctx, &acc, &dev)
	}

	_ = a.deviceRepo.Delete(ctx, vars["apns"])

	w.WriteHeader(http.StatusOK)
}
