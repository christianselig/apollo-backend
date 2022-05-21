package worker

import (
	"context"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/adjust/rmq/v4"
	"github.com/go-redis/redis/v8"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/sirupsen/logrus"
)

const pollDuration = 100 * time.Millisecond

type NewWorkerFn func(context.Context, *logrus.Logger, *statsd.Client, *pgxpool.Pool, *redis.Client, rmq.Connection, int) Worker
type Worker interface {
	Start() error
	Stop()
}
