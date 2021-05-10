package main

import (
	"github.com/julienschmidt/httprouter"
)

func (app *application) routes() *httprouter.Router {
	router := httprouter.New()

	router.GET("/v1/health", app.healthCheckHandler)

	router.POST("/v1/device", app.upsertDeviceHandler)
	router.POST("/v1/device/:apns/account", app.upsertAccountHandler)

	return router
}
