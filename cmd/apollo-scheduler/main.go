package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

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
	s.Every(1).Second().Do(func() { enqueueAccounts(ctx, logger, pool, redisConn, notificationsQueue) })
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

func enqueueAccounts(ctx context.Context, logger *logrus.Logger, pool *pgxpool.Pool, redisConn *redis.Client, queue rmq.Queue) {
	now := float64(time.Now().UnixNano()/int64(time.Millisecond)) / 1000

	start := time.Now().Unix()
	end := start + 1

	ids := []int64{}

	err := pool.BeginFunc(ctx, func(tx pgx.Tx) error {
		stmt := `
			WITH account AS (
			  SELECT id
				FROM accounts
				WHERE
					(last_checked_at + 5) BETWEEN $1 and $2
					OR last_checked_at + 60 <= $1
				ORDER BY last_checked_at
			)
			UPDATE accounts
			SET last_enqueued_at = $3
			WHERE accounts.id IN(SELECT id FROM account)
			RETURNING accounts.id`
		rows, err := tx.Query(ctx, stmt, start, end, now)
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
		"start": start,
		"end":   end,
	}).Debug("enqueueing account batch")

	enqueued := 0
	skipped := 0
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

		enqueued++
		_ = queue.Publish(payload)
	}

	logger.WithFields(logrus.Fields{
		"count":   enqueued,
		"skipped": skipped,
		"start":   start,
		"end":     end,
	}).Info("done enqueueing account batch")
}

func logErrors(errChan <-chan error) {
	for err := range errChan {
		log.Print("error: ", err)
	}
}
