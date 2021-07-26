package api

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/julienschmidt/httprouter"
	"github.com/sirupsen/logrus"

	"github.com/christianselig/apollo-backend/internal/domain"
	"github.com/christianselig/apollo-backend/internal/reddit"
	"github.com/christianselig/apollo-backend/internal/repository"
)

type api struct {
	logger *logrus.Logger
	statsd *statsd.Client
	reddit *reddit.Client

	accountRepo domain.AccountRepository
	deviceRepo  domain.DeviceRepository
}

func NewAPI(ctx context.Context, logger *logrus.Logger, statsd *statsd.Client, pool *pgxpool.Pool) *api {
	reddit := reddit.NewClient(
		os.Getenv("REDDIT_CLIENT_ID"),
		os.Getenv("REDDIT_CLIENT_SECRET"),
		statsd,
		16,
	)

	accountRepo := repository.NewPostgresAccount(pool)
	deviceRepo := repository.NewPostgresDevice(pool)

	return &api{
		logger:      logger,
		statsd:      statsd,
		reddit:      reddit,
		accountRepo: accountRepo,
		deviceRepo:  deviceRepo,
	}
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
	router.POST("/v1/device/:apns/account", a.upsertAccountHandler)

	return router
}
