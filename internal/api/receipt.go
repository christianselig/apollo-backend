package api

import (
	"net/http"
)

const receiptResponse = `{
    "products": [
        {
            "name": "ultra",
            "status": "SANDBOX",
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

func (a *api) checkReceiptHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte(receiptResponse))
	w.WriteHeader(http.StatusOK)
}
