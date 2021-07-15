package cmdutil

import (
	"context"
	"fmt"
	"os"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/adjust/rmq/v4"
	"github.com/go-redis/redis/v8"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/sirupsen/logrus"
)

func NewLogrusLogger(debug bool) *logrus.Logger {
	logger := logrus.New()

	if debug || os.Getenv("ENV") == "" {
		logger.SetLevel(logrus.DebugLevel)
	} else {
		logger.SetFormatter(&logrus.TextFormatter{
			DisableColors: true,
			FullTimestamp: true,
		})
	}

	return logger
}

func NewStatsdClient(tags ...string) (*statsd.Client, error) {
	if env := os.Getenv("ENV"); env != "" {
		tags = append(tags, fmt.Sprintf("env:%s", env))
	}

	return statsd.New("127.0.0.1:8125", statsd.WithTags(tags))
}

func NewRedisClient(ctx context.Context) (*redis.Client, error) {
	opt, err := redis.ParseURL(os.Getenv("REDISCLOUD_URL"))
	if err != nil {
		return nil, err
	}
	opt.MinIdleConns = 16

	client := redis.NewClient(opt)
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	return client, nil
}

func NewDatabasePool(ctx context.Context, maxConns int) (*pgxpool.Pool, error) {
	url := fmt.Sprintf(
		"%s?sslmode=require&pool_max_conns=%d&pool_min_conns=%d",
		os.Getenv("DATABASE_CONNECTION_POOL_URL"),
		maxConns,
		maxConns/8,
	)
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}

	// Setting the build statement cache to nil helps this work with pgbouncer
	config.ConnConfig.BuildStatementCache = nil
	config.ConnConfig.PreferSimpleProtocol = true

	return pgxpool.ConnectConfig(ctx, config)
}

func NewQueueClient(logger *logrus.Logger, conn *redis.Client, identifier string) (rmq.Connection, error) {
	errChan := make(chan error, 10)
	go func() {
		for err := range errChan {
			logger.WithFields(logrus.Fields{
				"err": err,
			}).Error("error occured with queue")
		}
	}()

	return rmq.OpenConnectionWithRedisClient(identifier, conn, errChan)
}
