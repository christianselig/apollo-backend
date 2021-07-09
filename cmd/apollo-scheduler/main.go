package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
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

const (
	batchSize      = 100
	checkTimeout   = 60 // how long until we force a check
	enqueueTimeout = 5  // how long until we try to re-enqueue
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
	s.Every(1).Minute().Do(func() { reportStats(ctx, logger, statsd, pool, redisConn) })
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

func reportStats(ctx context.Context, logger *logrus.Logger, statsd *statsd.Client, pool *pgxpool.Pool, redisConn *redis.Client) {
	var (
		count int64

		metrics = []struct {
			query string
			name  string
		}{
			{"SELECT COUNT(*) FROM accounts", "apollo.registrations.accounts"},
			{"SELECT COUNT(*) FROM devices", "apollo.registrations.devices"},
		}
	)

	for _, metric := range metrics {
		pool.QueryRow(ctx, metric.query).Scan(&count)
		statsd.Gauge(metric.name, float64(count), []string{}, 1)

		logger.WithFields(logrus.Fields{
			"count":  count,
			"metric": metric.name,
		}).Debug("fetched metrics")
	}
}

func enqueueAccounts(ctx context.Context, logger *logrus.Logger, statsd *statsd.Client, pool *pgxpool.Pool, redisConn *redis.Client, queue rmq.Queue) {
	start := time.Now()

	now := float64(time.Now().UnixNano()/int64(time.Millisecond)) / 1000

	// Start looking for accounts that were last checked at least 5 seconds ago
	// and at most 6 seconds ago. Also look for accounts that haven't been checked
	// in over a minute.
	ts := start.Unix()
	ready := ts - enqueueTimeout
	expired := ts - checkTimeout

	ids := []int64{}

	err := pool.BeginFunc(ctx, func(tx pgx.Tx) error {
		stmt := `
			WITH account AS (
			  SELECT id
				FROM accounts
				WHERE
					last_enqueued_at < $1
					OR last_checked_at < $2
				ORDER BY last_checked_at
				LIMIT 1000
			)
			UPDATE accounts
			SET last_enqueued_at = $3
			WHERE accounts.id IN(SELECT id FROM account)
			RETURNING accounts.id`
		rows, err := tx.Query(ctx, stmt, ready, expired, now)
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
		"start": ready,
	}).Debug("enqueueing account batch")

	enqueued := 0
	skipped := 0

	// Split ids in batches
	for i := 0; i < len(ids); i += batchSize {
		j := i + batchSize
		if j > len(ids) {
			j = len(ids)
		}
		batch := Int64Slice(ids[i:j])

		logger.WithFields(logrus.Fields{
			"len": len(batch),
		}).Debug("enqueueing batch")

		lua := fmt.Sprintf(`
			local retv={}
			local ids=cjson.decode(ARGV[1])

			for i=1, #ids do
				local key = "locks:accounts:" .. ids[i]
				if redis.call("exists", key) == 0 then
					redis.call("setex", key, %d, 1)
					retv[#retv + 1] = ids[i]
				end
			end

			return retv
		`, checkTimeout)

		res, err := redisConn.Eval(ctx, lua, []string{}, batch).Result()
		if err != nil {
			logger.WithFields(logrus.Fields{
				"err": err,
			}).Error("failed to check for locked accounts")

		}

		vals := res.([]interface{})
		skipped += len(batch) - len(vals)
		enqueued += len(vals)

		if len(vals) == 0 {
			continue
		}

		batchIds := make([]string, len(vals))
		for k, v := range vals {
			batchIds[k] = strconv.FormatInt(v.(int64), 10)
		}

		if err = queue.Publish(batchIds...); err != nil {
			logger.WithFields(logrus.Fields{
				"err": err,
			}).Error("failed to enqueue account")
		}
	}

	statsd.Histogram("apollo.queue.enqueued", float64(enqueued), []string{}, 1)
	statsd.Histogram("apollo.queue.skipped", float64(skipped), []string{}, 1)
	statsd.Histogram("apollo.queue.runtime", float64(time.Now().Sub(start).Milliseconds()), []string{}, 1)

	logger.WithFields(logrus.Fields{
		"count":   enqueued,
		"skipped": skipped,
		"start":   ready,
	}).Debug("done enqueueing account batch")
}

func logErrors(errChan <-chan error) {
	for err := range errChan {
		log.Print("error: ", err)
	}
}

type Int64Slice []int64

func (ii Int64Slice) MarshalBinary() (data []byte, err error) {
	bytes, err := json.Marshal(ii)
	return bytes, err
}
