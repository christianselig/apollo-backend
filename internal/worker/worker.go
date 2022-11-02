package worker

import (
	"context"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/go-redis/redis/v8"
	"github.com/jackc/pgx/v4/pgxpool"
	"go.uber.org/zap"
)

const pollDuration = 100 * time.Millisecond

type NewWorkerFn func(context.Context, *zap.Logger, *statsd.Client, *pgxpool.Pool, *redis.Client, int) Worker
type Worker interface {
	Process(ctx context.Context, args ...interface{}) error
}
