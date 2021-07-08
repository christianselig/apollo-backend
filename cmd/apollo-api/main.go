package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"

	"github.com/christianselig/apollo-backend/internal/data"
	"github.com/christianselig/apollo-backend/internal/reddit"
)

type config struct {
	port int
}

type application struct {
	cfg    config
	logger *log.Logger
	db     *sql.DB
	models *data.Models
	client *reddit.Client
}

func main() {
	logger := log.New(os.Stdout, "", log.Ldate|log.Ltime)

	if err := godotenv.Load(); err != nil {
		logger.Printf("Couldn't find .env so I will read from existing ENV.")
	}

	var cfg config

	dburl, ok := os.LookupEnv("DATABASE_CONNECTION_POOL_URL")
	if !ok {
		dburl = os.Getenv("DATABASE_URL")
	}

	db, err := sql.Open("postgres", fmt.Sprintf("%s?binary_parameters=yes", dburl))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

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
		db,
		data.NewModels(db),
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
	err = srv.ListenAndServe()
	logger.Fatal(err)
}
