package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"

	"github.com/andremedeiros/apollo/internal/data"
	"github.com/andremedeiros/apollo/internal/reddit"
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
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	var cfg config
	flag.IntVar(&cfg.port, "port", 4000, "API server port")
	flag.Parse()

	logger := log.New(os.Stdout, "", log.Ldate|log.Ltime)

	db, err := sql.Open("postgres", os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rc := reddit.NewClient(os.Getenv("REDDIT_CLIENT_ID"), os.Getenv("REDDIT_CLIENT_SECRET"))

	app := &application{
		cfg,
		logger,
		db,
		data.NewModels(db),
		rc,
	}

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.port),
		Handler: app.routes(),
	}

	logger.Printf("starting server on %s", srv.Addr)
	err = srv.ListenAndServe()
	logger.Fatal(err)
}
