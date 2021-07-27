package api

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
)

const receiptResponse = `{
    "products": [
        {
            "name": "ultra",
            "status": "SANBOX",
            "subscription_type": "SANDBOX"
        },
        {
            "name": "pro",
            "status": "SANDBOX"
        },
        {
            "name": "community_icons",
            "status": "SANDBOX"
        },
        {
            "name": "spca",
            "status": "SANDBOX"
        }
    ]
}`

func (a *api) checkReceiptHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Write([]byte(receiptResponse))
	w.WriteHeader(http.StatusOK)
}
