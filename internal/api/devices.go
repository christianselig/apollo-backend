package api

import (
	"encoding/json"
	"net/http"

	"github.com/julienschmidt/httprouter"

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
