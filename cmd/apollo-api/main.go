package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/andremedeiros/apollo/internal/data"
	"github.com/andremedeiros/apollo/internal/reddit"

	faktory "github.com/contribsys/faktory/client"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

type config struct {
	port int
}

type application struct {
	cfg     config
	logger  *log.Logger
	db      *sql.DB
	faktory *faktory.Client
	models  *data.Models
	client  *reddit.Client
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

	faktory, err := faktory.Open()
	if err != nil {
		log.Fatal(err)
	}
	defer faktory.Close()

	rc := reddit.NewClient(os.Getenv("REDDIT_CLIENT_ID"), os.Getenv("REDDIT_CLIENT_SECRET"))

	app := &application{
		cfg,
		logger,
		db,
		faktory,
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

	/*
		rc := reddit.NewClient("C7MjYkx1czyRDA", "I2AsVWbrf8h4vdQxVa5Pvf84vScF1w")
		rac := rc.NewAuthenticatedClient("2532458-kGp6OeR-LMoQNrUXRL-7UNfyBbViRA", "2532458-UE7IvJK3-VuTdMJB0bgMv58fxKQhww")
		rac.MessageInbox()
	*/
}
