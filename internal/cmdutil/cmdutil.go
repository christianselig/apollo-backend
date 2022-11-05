package cmdutil

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/adjust/rmq/v5"
	"github.com/go-redis/redis/extra/redisotel/v8"
	"github.com/go-redis/redis/v8"
	"github.com/jackc/pgx/v4/pgxpool"
	"go.uber.org/zap"
)

func NewLogger(service string) *zap.Logger {
	env := os.Getenv("ENV")
	logger, _ := zap.NewProduction(zap.Fields(
		zap.String("env", env),
		zap.String("service", service),
	))

	if env == "" || env == "development" {
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

func NewRedisLocksClient(ctx context.Context, maxConns int) (*redis.Client, error) {
	return newRedisClient(ctx, "REDIS_LOCKS_URL", maxConns)
}

func NewRedisQueueClient(ctx context.Context, maxConns int) (*redis.Client, error) {
	return newRedisClient(ctx, "REDIS_QUEUE_URL", maxConns)
}

func newRedisClient(ctx context.Context, env string, maxConns int) (*redis.Client, error) {
	opt, err := redis.ParseURL(os.Getenv(env))
	if err != nil {
		return nil, err
	}
	opt.PoolSize = maxConns

	client := redis.NewClient(opt)
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	client.AddHook(redisotel.NewTracingHook())

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
	config.MaxConnLifetime = 1 * time.Hour
	config.MaxConnIdleTime = 30 * time.Second
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
