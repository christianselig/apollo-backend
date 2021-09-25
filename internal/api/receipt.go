package api

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"

	"github.com/christianselig/apollo-backend/internal/domain"
	"github.com/christianselig/apollo-backend/internal/itunes"
)

func (a *api) checkReceiptHandler(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()

	vars := mux.Vars(r)
	apns := vars["apns"]

	body, _ := ioutil.ReadAll(r.Body)
	iapr, err := itunes.NewIAPResponse(string(body), true)

	if err != nil {
		a.logger.WithFields(logrus.Fields{
			"err": err,
		}).Info("failed verifying receipt")
		a.errorResponse(w, r, 500, err.Error())
		return
	}

	if apns != "" {
		dev, err := a.deviceRepo.GetByAPNSToken(ctx, apns)
		if err != nil {
			a.errorResponse(w, r, 500, err.Error())
			return
		}

		if iapr.DeleteDevice {
			accs, err := a.accountRepo.GetByAPNSToken(ctx, apns)
			if err != nil {
				a.errorResponse(w, r, 500, err.Error())
				return
			}

			for _, acc := range accs {
				_ = a.accountRepo.Disassociate(ctx, &acc, &dev)
			}

			_ = a.deviceRepo.Delete(ctx, apns)
		} else {
			dev.ActiveUntil = time.Now().Unix() + domain.DeviceActiveAfterReceitCheckDuration
			_ = a.deviceRepo.Update(ctx, &dev)
		}
	}

	w.WriteHeader(http.StatusOK)

	bb, _ := json.Marshal(iapr.VerificationInfo)
	_, _ = w.Write(bb)
}
