package worker

import (
	"context"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/adjust/rmq/v5"
	"github.com/go-redis/redis/v8"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

const pollDuration = 100 * time.Millisecond

type NewWorkerFn func(context.Context, *zap.Logger, trace.Tracer, *statsd.Client, *pgxpool.Pool, *redis.Client, rmq.Connection, int) Worker
type Worker interface {
	Start() error
	Stop()
}
