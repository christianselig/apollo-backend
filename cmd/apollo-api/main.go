package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"

	"github.com/christianselig/apollo-backend/internal/data"
	"github.com/christianselig/apollo-backend/internal/reddit"
)

type config struct {
	port int
}

type application struct {
	cfg    config
	logger *logrus.Logger
	pool   *pgxpool.Pool
	models *data.Models
	client *reddit.Client
}

func main() {
	_ = godotenv.Load()
	ctx, cancel := context.WithCancel(context.Background())

	var logger *logrus.Logger
	{
		logger = logrus.New()
		if os.Getenv("ENV") == "" {
			logger.SetLevel(logrus.DebugLevel)
		} else {
			logger.SetFormatter(&logrus.TextFormatter{
				DisableColors: true,
				FullTimestamp: true,
			})
		}
	}

	var cfg config

	// Set up Postgres connection
	var pool *pgxpool.Pool
	{
		url := fmt.Sprintf("%s?sslmode=require", os.Getenv("DATABASE_CONNECTION_POOL_URL"))
		config, err := pgxpool.ParseConfig(url)
		if err != nil {
			panic(err)
		}

		// Setting the build statement cache to nil helps this work with pgbouncer
		config.ConnConfig.BuildStatementCache = nil
		config.ConnConfig.PreferSimpleProtocol = true

		pool, err = pgxpool.ConnectConfig(ctx, config)
		if err != nil {
			panic(err)
		}
		defer pool.Close()
	}

	statsd, err := statsd.New("127.0.0.1:8125")
	if err != nil {
		log.Fatal(err)
	}

	rc := reddit.NewClient(
		os.Getenv("REDDIT_CLIENT_ID"),
		os.Getenv("REDDIT_CLIENT_SECRET"),
		statsd,
	)

	app := &application{
		cfg,
		logger,
		pool,
		data.NewModels(ctx, pool),
		rc,
	}

	port, ok := os.LookupEnv("PORT")
	if !ok {
		port = "4000"
	}

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%s", port),
		Handler: app.routes(),
	}

	logger.Printf("starting server on %s", srv.Addr)
	go srv.ListenAndServe()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)

	<-signals // wait for signal

	srv.Shutdown(ctx)
	cancel()

	go func() {
		<-signals // hard exit on second signal (in case shutdown gets stuck)
		os.Exit(1)
	}()
}
