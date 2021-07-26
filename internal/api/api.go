package api

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/julienschmidt/httprouter"
	"github.com/sideshow/apns2/token"
	"github.com/sirupsen/logrus"

	"github.com/christianselig/apollo-backend/internal/data"
	"github.com/christianselig/apollo-backend/internal/reddit"
)

type api struct {
	logger *logrus.Logger
	statsd *statsd.Client
	db     *pgxpool.Pool
	reddit *reddit.Client
	models *data.Models
	apns   *token.Token
}

func NewAPI(ctx context.Context, logger *logrus.Logger, statsd *statsd.Client, db *pgxpool.Pool) *api {
	reddit := reddit.NewClient(
		os.Getenv("REDDIT_CLIENT_ID"),
		os.Getenv("REDDIT_CLIENT_SECRET"),
		statsd,
		16,
	)

	var apns *token.Token
	{
		authKey, err := token.AuthKeyFromFile(os.Getenv("APPLE_KEY_PATH"))
		if err != nil {
			panic(err)
		}

		apns = &token.Token{
			AuthKey: authKey,
			KeyID:   os.Getenv("APPLE_KEY_ID"),
			TeamID:  os.Getenv("APPLE_TEAM_ID"),
		}
	}

	models := data.NewModels(ctx, db)

	return &api{logger, statsd, db, reddit, models, apns}
}

func (a *api) Server(port int) *http.Server {
	return &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: a.Routes(),
	}
}

func (a *api) Routes() *httprouter.Router {
	router := httprouter.New()

	router.GET("/v1/health", a.healthCheckHandler)

	router.POST("/v1/device", a.upsertDeviceHandler)
	router.POST("/v1/device/:apns/test", a.testDeviceHandler)
	router.POST("/v1/device/:apns/account", a.upsertAccountHandler)
	router.DELETE("/v1/device/:apns", a.deleteDeviceHandler)

	router.POST("/v1/receipt", a.checkReceiptHandler)
	router.POST("/v1/receipt/:apns", a.checkReceiptHandler)

	return router
}
