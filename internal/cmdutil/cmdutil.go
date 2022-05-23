package cmdutil

import (
	"context"
	"fmt"
	"os"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/adjust/rmq/v4"
	"github.com/go-redis/redis/v8"
	"github.com/jackc/pgx/v4/pgxpool"
	"go.uber.org/zap"
)

func NewLogger(debug bool) *zap.Logger {
	logger, _ := zap.NewProduction()
	if debug || os.Getenv("ENV") == "" {
		logger, _ = zap.NewDevelopment()
	}

	return logger
}

func NewStatsdClient(tags ...string) (*statsd.Client, error) {
	if env := os.Getenv("ENV"); env != "" {
		tags = append(tags, fmt.Sprintf("env:%s", env))
	}

	return statsd.New(os.Getenv("STATSD_URL"), statsd.WithTags(tags))
}

func NewRedisClient(ctx context.Context) (*redis.Client, error) {
	opt, err := redis.ParseURL(os.Getenv("REDIS_URL"))
	if err != nil {
		return nil, err
	}
	opt.PoolSize = 16

	client := redis.NewClient(opt)
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	return client, nil
}

func NewDatabasePool(ctx context.Context, maxConns int) (*pgxpool.Pool, error) {
	if maxConns == 0 {
		maxConns = 1
	}

	url := fmt.Sprintf(
		"%s?pool_max_conns=%d&pool_min_conns=%d",
		os.Getenv("DATABASE_CONNECTION_POOL_URL"),
		maxConns,
		2,
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

func NewQueueClient(logger *zap.Logger, conn *redis.Client, identifier string) (rmq.Connection, error) {
	errChan := make(chan error, 10)
	go func() {
		for err := range errChan {
			logger.Error("error occurred within queue", zap.Error(err))
		}
	}()

	return rmq.OpenConnectionWithRedisClient(identifier, conn, errChan)
}
