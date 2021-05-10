package main

import (
	"database/sql"
	"log"
	"os"

	worker "github.com/contribsys/faktory_worker_go"
	"github.com/joho/godotenv"

	"github.com/andremedeiros/apollo/internal/data"
	"github.com/andremedeiros/apollo/internal/reddit"
)

type application struct {
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

	logger := log.New(os.Stdout, "", log.Ldate|log.Ltime)

	db, err := sql.Open("postgres", os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rc := reddit.NewClient(os.Getenv("REDDIT_CLIENT_ID"), os.Getenv("REDDIT_CLIENT_SECRET"))

	app := &application{
		logger,
		db,
		data.NewModels(db),
		rc,
	}

	mgr := worker.NewManager()
	mgr.ProcessStrictPriorityQueues("critical", "default", "bulk")
	mgr.Concurrency = 20
	mgr.Run()
}
