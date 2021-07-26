package api

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
)

func (a *api) checkReceiptHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Write([]byte(`{"status_code": 11, "status_message": "Receipt is valid, lifetime subscription", "subscription_type": "lifetime"}`))
	w.WriteHeader(http.StatusOK)
}
