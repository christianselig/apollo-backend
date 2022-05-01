package api

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/bugsnag/bugsnag-go/v2"
	"github.com/go-redis/redis/v8"
	"github.com/gorilla/mux"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/sideshow/apns2/token"
	"github.com/sirupsen/logrus"

	"github.com/christianselig/apollo-backend/internal/domain"
	"github.com/christianselig/apollo-backend/internal/reddit"
	"github.com/christianselig/apollo-backend/internal/repository"
)

type api struct {
	logger *logrus.Logger
	statsd *statsd.Client
	reddit *reddit.Client
	apns   *token.Token

	accountRepo   domain.AccountRepository
	deviceRepo    domain.DeviceRepository
	subredditRepo domain.SubredditRepository
	watcherRepo   domain.WatcherRepository
	userRepo      domain.UserRepository
}

func NewAPI(ctx context.Context, logger *logrus.Logger, statsd *statsd.Client, redis *redis.Client, pool *pgxpool.Pool) *api {
	reddit := reddit.NewClient(
		os.Getenv("REDDIT_CLIENT_ID"),
		os.Getenv("REDDIT_CLIENT_SECRET"),
		statsd,
		redis,
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

	accountRepo := repository.NewPostgresAccount(pool)
	deviceRepo := repository.NewPostgresDevice(pool)
	subredditRepo := repository.NewPostgresSubreddit(pool)
	watcherRepo := repository.NewPostgresWatcher(pool)
	userRepo := repository.NewPostgresUser(pool)

	return &api{
		logger: logger,
		statsd: statsd,
		reddit: reddit,
		apns:   apns,

		accountRepo:   accountRepo,
		deviceRepo:    deviceRepo,
		subredditRepo: subredditRepo,
		watcherRepo:   watcherRepo,
		userRepo:      userRepo,
	}
}

func (a *api) Server(port int) *http.Server {
	return &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: bugsnag.Handler(a.Routes()),
	}
}

func (a *api) Routes() *mux.Router {
	r := mux.NewRouter()

	r.HandleFunc("/v1/health", a.healthCheckHandler).Methods("GET")

	r.HandleFunc("/v1/device", a.upsertDeviceHandler).Methods("POST")
	r.HandleFunc("/v1/device/{apns}", a.deleteDeviceHandler).Methods("DELETE")
	r.HandleFunc("/v1/device/{apns}/test", a.testDeviceHandler).Methods("POST")

	r.HandleFunc("/v1/device/{apns}/account", a.upsertAccountHandler).Methods("POST")
	r.HandleFunc("/v1/device/{apns}/accounts", a.upsertAccountsHandler).Methods("POST")
	r.HandleFunc("/v1/device/{apns}/account/{redditID}", a.disassociateAccountHandler).Methods("DELETE")
	r.HandleFunc("/v1/device/{apns}/account/{redditID}/notifications", a.notificationsAccountHandler).Methods("PATCH")
	r.HandleFunc("/v1/device/{apns}/account/{redditID}/notifications", a.getNotificationsAccountHandler).Methods("GET")

	r.HandleFunc("/v1/device/{apns}/account/{redditID}/watcher", a.createWatcherHandler).Methods("POST")
	r.HandleFunc("/v1/device/{apns}/account/{redditID}/watcher/{watcherID}", a.deleteWatcherHandler).Methods("DELETE")
	r.HandleFunc("/v1/device/{apns}/account/{redditID}/watcher/{watcherID}", a.editWatcherHandler).Methods("PATCH")
	r.HandleFunc("/v1/device/{apns}/account/{redditID}/watchers", a.listWatchersHandler).Methods("GET")

	r.HandleFunc("/v1/receipt", a.checkReceiptHandler).Methods("POST")
	r.HandleFunc("/v1/receipt/{apns}", a.checkReceiptHandler).Methods("POST")

	r.HandleFunc("/v1/contact", a.contactHandler).Methods("POST")

	r.HandleFunc("/v1/test/bugsnag", a.testBugsnagHandler).Methods("POST")

	r.Use(a.loggingMiddleware)

	return r
}

func (a *api) testBugsnagHandler(w http.ResponseWriter, r *http.Request) {
	if err := bugsnag.Notify(fmt.Errorf("Test error")); err != nil {
		a.errorResponse(w, r, 500, err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
}

type LoggingResponseWriter struct {
	w          http.ResponseWriter
	statusCode int
	bytes      int
}

func (lrw *LoggingResponseWriter) Header() http.Header {
	return lrw.w.Header()
}

func (lrw *LoggingResponseWriter) Write(bb []byte) (int, error) {
	wb, err := lrw.w.Write(bb)
	lrw.bytes += wb
	return wb, err
}

func (lrw *LoggingResponseWriter) WriteHeader(statusCode int) {
	lrw.w.WriteHeader(statusCode)
	lrw.statusCode = statusCode
}

func (a *api) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip logging health checks
		if r.RequestURI == "/v1/health" {
			next.ServeHTTP(w, r)
			return
		}

		start := time.Now()
		lrw := &LoggingResponseWriter{w: w}
		// Do stuff here
		// Call the next handler, which can be another middleware in the chain, or the final handler.
		next.ServeHTTP(lrw, r)

		remoteAddr := r.Header.Get("X-Forwarded-For")
		if remoteAddr == "" {
			if ip, _, err := net.SplitHostPort(r.RemoteAddr); err != nil {
				remoteAddr = "unknown"
			} else {
				remoteAddr = ip
			}
		}

		logEntry := a.logger.WithFields(logrus.Fields{
			"duration":       time.Since(start).Milliseconds(),
			"method":         r.Method,
			"remote#addr":    remoteAddr,
			"response#bytes": lrw.bytes,
			"status":         lrw.statusCode,
			"uri":            r.RequestURI,
		})

		if lrw.statusCode == 200 {
			logEntry.Info()
		} else {
			err := lrw.Header().Get("X-Apollo-Error")
			logEntry.Error(err)
		}
	})
}
