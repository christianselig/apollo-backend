package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/adjust/rmq/v4"
	"github.com/go-co-op/gocron"
	"github.com/go-redis/redis/v8"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
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

	statsd, err := statsd.New("127.0.0.1:8125")
	if err != nil {
		logger.WithFields(logrus.Fields{
			"err": err,
		}).Error("failed to set up stats")
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

	// Set up queues
	var (
		notificationsQueue rmq.Queue
	)
	{
		connection, err := rmq.OpenConnectionWithRedisClient("producer", redisConn, errChan)
		if err != nil {
			panic(err)
		}

		notificationsQueue, err = connection.OpenQueue("notifications")
		if err != nil {
			panic(err)
		}
	}

	s := gocron.NewScheduler(time.UTC)
	s.Every(1).Second().Do(func() { enqueueAccounts(ctx, logger, statsd, pool, redisConn, notificationsQueue) })
	s.StartAsync()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)

	<-signals // wait for signal
	cancel()
	go func() {
		<-signals // hard exit on second signal (in case shutdown gets stuck)
		os.Exit(1)
	}()

	s.Stop()
}

func enqueueAccounts(ctx context.Context, logger *logrus.Logger, statsd *statsd.Client, pool *pgxpool.Pool, redisConn *redis.Client, queue rmq.Queue) {
	start := time.Now()

	now := float64(time.Now().UnixNano()/int64(time.Millisecond)) / 1000

	// Start looking for accounts that were last checked at least 5 seconds ago
	// and at most 6 seconds ago. Also look for accounts that haven't been checked
	// in over a minute.
	ts := start.Unix()
	left := ts - 6
	right := left + 1
	expired := ts - 60

	ids := []int64{}

	err := pool.BeginFunc(ctx, func(tx pgx.Tx) error {
		stmt := `
			WITH account AS (
			  SELECT id
				FROM accounts
				WHERE
					last_checked_at BETWEEN $1 AND $2
					OR last_checked_at < $3
				ORDER BY last_checked_at
			)
			UPDATE accounts
			SET last_enqueued_at = $4
			WHERE accounts.id IN(SELECT id FROM account)
			RETURNING accounts.id`
		rows, err := tx.Query(ctx, stmt, left, right, expired, now)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id int64
			rows.Scan(&id)
			ids = append(ids, id)
		}
		return nil
	})

	if err != nil {
		logger.WithFields(logrus.Fields{
			"err": err,
		}).Error("failed to fetch batch of accounts")
		return
	}

	logger.WithFields(logrus.Fields{
		"count": len(ids),
		"start": left,
		"end":   right,
	}).Debug("enqueueing account batch")

	enqueued := 0
	skipped := 0
	failed := 0
	for _, id := range ids {
		payload := fmt.Sprintf("%d", id)
		lockKey := fmt.Sprintf("locks:accounts:%s", payload)

		_, err := redisConn.Get(ctx, lockKey).Result()
		if err == nil {
			skipped++
			continue
		} else if err != redis.Nil {
			logger.WithFields(logrus.Fields{
				"lockKey": lockKey,
				"err":     err,
			}).Error("failed to check for account lock")
		}

		if err := redisConn.SetEX(ctx, lockKey, true, 60*time.Second).Err(); err != nil {
			logger.WithFields(logrus.Fields{
				"lockKey": lockKey,
				"err":     err,
			}).Error("failed to lock account")
		}

		if err = queue.Publish(payload); err != nil {
			logger.WithFields(logrus.Fields{
				"accountID": payload,
				"err":       err,
			}).Error("failed to enqueue account")
			failed++
		} else {
			enqueued++
		}
	}

	statsd.Histogram("apollo.queue.enqueued", float64(enqueued), []string{}, 1)
	statsd.Histogram("apollo.queue.skipped", float64(skipped), []string{}, 1)
	statsd.Histogram("apollo.queue.failed", float64(failed), []string{}, 1)
	statsd.Histogram("apollo.queue.runtime", float64(time.Now().Sub(start).Milliseconds()), []string{}, 1)

	logger.WithFields(logrus.Fields{
		"count":   enqueued,
		"skipped": skipped,
		"failed":  failed,
		"start":   left,
		"end":     right,
	}).Info("done enqueueing account batch")
}

func logErrors(errChan <-chan error) {
	for err := range errChan {
		log.Print("error: ", err)
	}
}
