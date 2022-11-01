package worker

import (
	"context"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/adjust/rmq/v5"
	"github.com/go-redis/redis/v8"
	"github.com/jackc/pgx/v4/pgxpool"
	"go.uber.org/zap"
)

const pollDuration = 50 * time.Millisecond

type NewWorkerFn func(context.Context, *zap.Logger, *statsd.Client, *pgxpool.Pool, *redis.Client, rmq.Connection, int) Worker
type Worker interface {
	Start() error
	Stop()
}
