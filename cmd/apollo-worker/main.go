package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/sideshow/apns2"
	"github.com/sideshow/apns2/payload"
	"github.com/sideshow/apns2/token"

	"github.com/christianselig/apollo-backend/internal/data"
	"github.com/christianselig/apollo-backend/internal/reddit"
)

type application struct {
	logger *log.Logger
	db     *sql.DB
	models *data.Models
	client *reddit.Client
}

var (
	workers int     = runtime.NumCPU() * 6
	rate    float64 = 0.1
	backoff int     = 5
)

func accountWorker(id int, rc *reddit.Client, db *sql.DB, logger *log.Logger, statsd *statsd.Client, quit chan bool) {
	authKey, err := token.AuthKeyFromBytes([]byte(os.Getenv("APPLE_KEY_PKEY")))
	token := &token.Token{
		AuthKey: authKey,
		KeyID:   os.Getenv("APPLE_KEY_ID"),
		TeamID:  os.Getenv("APPLE_TEAM_ID"),
	}

	if err != nil {
		log.Fatal("token error:", err)
	}

	sandboxClient := apns2.NewTokenClient(token)
	productionClient := apns2.NewTokenClient(token).Production()

	for {
		select {
		case <-quit:
			return
		default:
			now := float64(time.Now().UTC().UnixNano()/int64(time.Millisecond)) / 1000
			tx, err := db.Begin()

			if err != nil {
				log.Fatal(err)
				continue
			}

			query := `
				SELECT id, username, access_token, refresh_token, expires_at, last_message_id, last_checked_at FROM accounts
				WHERE last_checked_at <= $1::float - 5
				ORDER BY last_checked_at
				LIMIT 1
				FOR UPDATE SKIP LOCKED`
			args := []interface{}{now}

			account := &data.Account{}
			err = tx.QueryRow(query, args...).Scan(&account.ID, &account.Username, &account.AccessToken, &account.RefreshToken, &account.ExpiresAt, &account.LastMessageID, &account.LastCheckedAt)

			if account.ID == 0 {
				tx.Commit()
				time.Sleep(100 * time.Millisecond)
				continue
			}

			if account.LastCheckedAt > 0 {
				latency := now - account.LastCheckedAt - float64(backoff)
				statsd.Histogram("apollo.queue.delay", latency, []string{}, rate)
			}

			_, err = tx.Exec(`UPDATE accounts SET last_checked_at = $1 WHERE id = $2`, now, account.ID)

			rac := rc.NewAuthenticatedClient(account.RefreshToken, account.AccessToken)
			if account.ExpiresAt < int64(now) {
				tokens, _ := rac.RefreshTokens()
				tx.Exec(`UPDATE accounts SET access_token = $1, refresh_token = $2, expires_at = $3 WHERE id = $4`,
					tokens.AccessToken, tokens.RefreshToken, now+3500, account.ID)
			}

			t1 := time.Now()
			msgs, err := rac.MessageInbox(account.LastMessageID)
			t2 := time.Now()
			statsd.Histogram("reddit.api.latency", float64(t2.Sub(t1).Milliseconds()), []string{}, rate)

			if err != nil {
				log.Fatal(err)
			}

			if len(msgs.MessageListing.Messages) == 0 {
				tx.Commit()
				continue
			}

			// Set latest message we alerted on
			latestMsg := msgs.MessageListing.Messages[0]

			latency := now - latestMsg.CreatedAt
			statsd.Histogram("apollo.notification.latency", latency, []string{}, rate)

			_, err = tx.Exec(`UPDATE accounts SET last_message_id = $1 WHERE id = $2`, latestMsg.FullName(), account.ID)
			if err != nil {
				log.Fatal(err)
			}

			// If no latest message recorded, we're not going to notify on every message. Remember that and move on.
			if account.LastMessageID == "" {
				tx.Commit()
				continue
			}

			query = `
				SELECT apns_token, sandbox FROM devices
				LEFT JOIN devices_accounts ON devices.id = devices_accounts.device_id
				WHERE devices_accounts.account_id = $1`

			rows, err := tx.Query(query, account.ID)
			if err != nil {
				logger.Fatal(err)
			}
			defer rows.Close()

			devices := []data.Device{}
			for rows.Next() {
				device := data.Device{}
				rows.Scan(&device.APNSToken, &device.Sandbox)
				devices = append(devices, device)
			}

			for _, msg := range msgs.MessageListing.Messages {
				for _, device := range devices {
					notification := &apns2.Notification{}
					notification.DeviceToken = device.APNSToken
					notification.Topic = "com.christianselig.Apollo"
					notification.Payload = payload.NewPayload().AlertTitle(msg.Subject).AlertBody(msg.Body)
					client := productionClient
					if device.Sandbox {
						client = sandboxClient
					}
					t1 := time.Now()
					res, err := client.Push(notification)
					t2 := time.Now()
					statsd.Histogram("apns.notification.latency", float64(t2.Sub(t1).Milliseconds()), []string{}, float64(1))
					if err != nil {
						statsd.Incr("apns.notification.errors", []string{}, float64(1))
						logger.Printf("apns error account=%s token=%s err=%s status=%d reason=%q", account.Username, device.APNSToken, err, res.StatusCode, res.Reason)
					} else {
						statsd.Incr("apns.notification.sent", []string{}, float64(1))
						logger.Printf("apns success account=%s token=%s", account.Username, device.APNSToken)
					}
				}
			}

			tx.Commit()
		}
	}
}

func main() {
	logger := log.New(os.Stdout, "", log.Ldate|log.Ltime)

	if err := godotenv.Load(); err != nil {
		logger.Printf("Couldn't find .env so I will read from existing ENV.")
	}

	rc := reddit.NewClient(os.Getenv("REDDIT_CLIENT_ID"), os.Getenv("REDDIT_CLIENT_SECRET"))

	dburl, ok := os.LookupEnv("DATABASE_CONNECTION_POOL_URL")
	if !ok {
		dburl = os.Getenv("DATABASE_URL")
	}

	db, err := sql.Open("postgres", fmt.Sprintf("%s?binary_parameters=yes", dburl))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	logger.Printf("Starting with %d workers.", workers)

	db.SetMaxOpenConns(workers)

	statsd, err := statsd.New("127.0.0.1:8125")
	if err != nil {
		log.Fatal(err)
	}

	// This is a very conservative value -- seen as most of the work that is done in these jobs is
	//
	runtime.GOMAXPROCS(workers)
	quitCh := make(chan bool, workers)
	for i := 0; i < workers; i++ {
		go accountWorker(i, rc, db, logger, statsd, quitCh)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	<-sigs

	for i := 0; i < workers; i++ {
		quitCh <- true
	}
	os.Exit(0)
}
