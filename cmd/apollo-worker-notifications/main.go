package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/adjust/rmq/v4"
	"github.com/go-redis/redis/v8"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/joho/godotenv"
	"github.com/sideshow/apns2"
	"github.com/sideshow/apns2/payload"
	"github.com/sideshow/apns2/token"
	"github.com/sirupsen/logrus"

	"github.com/christianselig/apollo-backend/internal/data"
	"github.com/christianselig/apollo-backend/internal/reddit"
)

const (
	pollDuration = 100 * time.Millisecond
	backoff      = 5
	rate         = 0.1
)

func main() {
	_ = godotenv.Load()

	errChan := make(chan error, 10)
	go logErrors(errChan)

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

	// Set up Postgres connection
	var pool *pgxpool.Pool
	{
		config, err := pgxpool.ParseConfig(os.Getenv("DATABASE_CONNECTION_POOL_URL"))
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

	var apnsToken *token.Token
	{
		authKey, err := token.AuthKeyFromBytes([]byte(os.Getenv("APPLE_KEY_PKEY")))
		if err != nil {
			panic(err)
		}

		apnsToken = &token.Token{
			AuthKey: authKey,
			KeyID:   os.Getenv("APPLE_KEY_ID"),
			TeamID:  os.Getenv("APPLE_TEAM_ID"),
		}
	}

	if err != nil {
		log.Fatal("token error:", err)
	}

	// Set up Redis connection
	var redisConn *redis.Client
	{
		opt, err := redis.ParseURL(os.Getenv("REDISCLOUD_URL"))
		if err != nil {
			panic(err)
		}

		redisConn = redis.NewClient(opt)
		if err := redisConn.Ping(ctx).Err(); err != nil {
			panic(err)
		}
	}

	connection, err := rmq.OpenConnectionWithRedisClient("consumer", redisConn, errChan)
	if err != nil {
		panic(err)
	}

	queue, err := connection.OpenQueue("notifications")
	if err != nil {
		panic(err)
	}

	numConsumers := runtime.NumCPU() * 8
	prefetchLimit := int64(numConsumers * 8)

	if err := queue.StartConsuming(prefetchLimit, pollDuration); err != nil {
		panic(err)
	}

	for i := 0; i < numConsumers; i++ {
		name := fmt.Sprintf("consumer %d", i)

		consumer := NewConsumer(i, logger, statsd, redisConn, pool, rc, apnsToken)
		if _, err := queue.AddConsumer(name, consumer); err != nil {
			panic(err)
		}
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT)
	defer signal.Stop(signals)

	<-signals // wait for signal
	cancel()
	go func() {
		<-signals // hard exit on second signal (in case shutdown gets stuck)
		os.Exit(1)
	}()

	<-connection.StopAllConsuming() // wait for all Consume() calls to finish
}

type Consumer struct {
	tag            int
	logger         *logrus.Logger
	statsd         *statsd.Client
	redis          *redis.Client
	pool           *pgxpool.Pool
	reddit         *reddit.Client
	apnsSandbox    *apns2.Client
	apnsProduction *apns2.Client
}

func NewConsumer(tag int, logger *logrus.Logger, statsd *statsd.Client, redis *redis.Client, pool *pgxpool.Pool, rc *reddit.Client, apnsToken *token.Token) *Consumer {
	return &Consumer{
		tag,
		logger,
		statsd,
		redis,
		pool,
		rc,
		apns2.NewTokenClient(apnsToken),
		apns2.NewTokenClient(apnsToken).Production(),
	}
}

func (c *Consumer) Consume(delivery rmq.Delivery) {
	ctx := context.Background()

	defer c.redis.HDel(ctx, "locks:accounts", delivery.Payload())

	c.logger.WithFields(logrus.Fields{
		"accountID": delivery.Payload(),
	}).Debug("starting job")

	id, err := strconv.ParseInt(delivery.Payload(), 10, 64)
	if err != nil {
		c.logger.WithFields(logrus.Fields{
			"accountID": delivery.Payload(),
			"err":       err,
		}).Error("failed to parse account ID")

		delivery.Reject()
		return
	}

	now := float64(time.Now().UnixNano()/int64(time.Millisecond)) / 1000

	stmt := `SELECT
			id,
			access_token,
			refresh_token,
			expires_at,
			last_message_id,
			last_checked_at
		FROM accounts
		WHERE id = $1`
	account := &data.Account{}
	if err := c.pool.QueryRow(ctx, stmt, id).Scan(
		&account.ID,
		&account.AccessToken,
		&account.RefreshToken,
		&account.ExpiresAt,
		&account.LastMessageID,
		&account.LastCheckedAt,
	); err != nil {
		c.logger.WithFields(logrus.Fields{
			"accountID": id,
			"err":       err,
		}).Error("failed to fetch account from database")

		delivery.Reject()
		return
	}

	if account.LastCheckedAt > 0 {
		latency := now - account.LastCheckedAt - float64(backoff)
		c.statsd.Histogram("apollo.queue.delay", latency, []string{}, rate)
	}

	rac := c.reddit.NewAuthenticatedClient(account.RefreshToken, account.AccessToken)
	if account.ExpiresAt < int64(now) {
		c.logger.WithFields(logrus.Fields{
			"accountID": id,
		}).Debug("refreshing reddit token")

		tokens, err := rac.RefreshTokens()
		if err != nil {
			c.logger.WithFields(logrus.Fields{
				"accountID": id,
				"err":       err,
			}).Error("failed to refresh reddit tokens")

			delivery.Reject()
			return
		}
		err = c.pool.BeginFunc(ctx, func(tx pgx.Tx) error {
			stmt := `
				UPDATE accounts
				SET access_token = $1, refresh_token = $2, expires_at = $3 WHERE id = $4`
			_, err := tx.Exec(ctx, stmt, tokens.AccessToken, tokens.RefreshToken, int64(now+3540), account.ID)
			return err
		})
		if err != nil {
			c.logger.WithFields(logrus.Fields{
				"accountID": id,
				"err":       err,
			}).Error("failed to update reddit tokens for account")

			delivery.Reject()
			return
		}
	}

	c.logger.WithFields(logrus.Fields{
		"accountID": id,
	}).Debug("fetching message inbox")
	msgs, err := rac.MessageInbox(account.LastMessageID)

	if err != nil {
		c.logger.WithFields(logrus.Fields{
			"accountID": id,
			"err":       err,
		}).Error("failed to fetch message inbox")

		delivery.Reject()
		return
	}

	c.logger.WithFields(logrus.Fields{
		"accountID": id,
		"count":     len(msgs.MessageListing.Messages),
	}).Debug("fetched messages")

	if err = c.pool.BeginFunc(ctx, func(tx pgx.Tx) error {
		stmt := `
			UPDATE accounts
			SET last_checked_at = $1
			WHERE id = $2`
		_, err := tx.Exec(ctx, stmt, now, account.ID)
		return err
	}); err != nil {
		c.logger.WithFields(logrus.Fields{
			"accountID": id,
			"err":       err,
		}).Error("failed to update last_checked_at for account")

		delivery.Reject()
		return
	}

	if len(msgs.MessageListing.Messages) == 0 {
		c.logger.WithFields(logrus.Fields{
			"accountID": id,
		}).Debug("no new messages, bailing early")

		delivery.Ack()
		return
	}

	// Set latest message we alerted on
	latestMsg := msgs.MessageListing.Messages[0]

	// Let's populate this with the latest message so we don't flood users with stuff
	if account.LastMessageID == "" {
		if err = c.pool.BeginFunc(ctx, func(tx pgx.Tx) error {
			stmt := `
				UPDATE accounts
				SET last_message_id = $1
				WHERE id = $2`
			_, err := tx.Exec(ctx, stmt, latestMsg.FullName(), account.ID)
			return err
		}); err != nil {
			c.logger.WithFields(logrus.Fields{
				"accountID": id,
				"err":       err,
			}).Error("failed to update last_message_id for account")

			delivery.Reject()
		} else {
			c.logger.WithFields(logrus.Fields{
				"accountID": delivery.Payload(),
			}).Debug("populating first message ID to prevent spamming")

			delivery.Ack()
		}
		return
	}

	devices := []data.Device{}
	stmt = `
		SELECT apns_token, sandbox
		FROM devices
		LEFT JOIN devices_accounts ON devices.id = devices_accounts.device_id
		WHERE devices_accounts.account_id = $1`
	rows, err := c.pool.Query(ctx, stmt, account.ID)
	if err != nil {
		c.logger.WithFields(logrus.Fields{
			"accountID": id,
			"err":       err,
		}).Error("failed to fetch account devices")

		delivery.Reject()
		return
	}
	defer rows.Close()
	for rows.Next() {
		var device data.Device
		rows.Scan(&device.APNSToken, &device.Sandbox)
		devices = append(devices, device)
	}

	for _, msg := range msgs.MessageListing.Messages {
		notification := &apns2.Notification{}
		notification.Topic = "com.christianselig.Apollo"
		notification.Payload = payload.NewPayload().AlertTitle(msg.Subject).AlertBody(msg.Body)

		for _, device := range devices {
			notification.DeviceToken = device.APNSToken
			client := c.apnsProduction
			if device.Sandbox {
				client = c.apnsSandbox
			}

			res, err := client.Push(notification)
			if err != nil {
				c.statsd.Incr("apns.notification.errors", []string{}, 1)
				c.logger.WithFields(logrus.Fields{
					"accountID": id,
					"err":       err,
					"status":    res.StatusCode,
					"reason":    res.Reason,
				}).Error("failed to send notification")
			} else {
				c.statsd.Incr("apns.notification.sent", []string{}, 1)
				c.logger.WithFields(logrus.Fields{
					"accountID":  delivery.Payload(),
					"token":      device.APNSToken,
					"redditUser": account.Username,
				}).Debug("sent notification")
			}
		}
	}

	delivery.Ack()

	c.logger.WithFields(logrus.Fields{
		"accountID": delivery.Payload(),
	}).Debug("finishing job")
}

func logErrors(errChan <-chan error) {
	for err := range errChan {
		log.Print("error: ", err)
	}
}
