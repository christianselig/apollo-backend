package main

import (
	"encoding/json"
	"net/http"

	"github.com/julienschmidt/httprouter"

	"github.com/andremedeiros/apollo/internal/data"
)

func (app *application) upsertDeviceHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	d := &data.Device{}
	if err := json.NewDecoder(r.Body).Decode(d); err != nil {
		app.errorResponse(w, r, 500, err.Error())
		return
	}

	if err := app.models.Devices.Upsert(d); err != nil {
		app.errorResponse(w, r, 500, err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
}
