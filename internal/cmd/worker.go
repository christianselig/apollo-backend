package cmd

import (
	"context"
	"fmt"
	"runtime"

	faktoryworker "github.com/contribsys/faktory_worker_go"
	"github.com/spf13/cobra"

	"github.com/christianselig/apollo-backend/internal/cmdutil"
	"github.com/christianselig/apollo-backend/internal/worker"
)

var (
	queues = map[string]worker.NewWorkerFn{
		"LiveActivityJob":       worker.NewLiveActivitiesWorker,
		"NotificationCheckJob":  worker.NewNotificationsWorker,
		"StuckNotificationsJob": worker.NewStuckNotificationsWorker,
		"SubredditWatcherJob":   worker.NewSubredditsWorker,
		"SubredditTrendingJob":  worker.NewTrendingWorker,
	}
)

func WorkerCmd(ctx context.Context) *cobra.Command {
	var consumers int
	var queueID string

	cmd := &cobra.Command{
		Use:   "worker",
		Args:  cobra.ExactArgs(0),
		Short: "Work through job queues.",
		RunE: func(cmd *cobra.Command, args []string) error {
			if queueID == "" {
				return fmt.Errorf("need a queue to work on")
			}

			runtime.SetBlockProfileRate(1)
			runtime.SetMutexProfileFraction(1)

			svc := fmt.Sprintf("worker: %s", queueID)
			logger := cmdutil.NewLogger(svc)
			defer func() { _ = logger.Sync() }()

			tag := fmt.Sprintf("worker:%s", queueID)
			statsd, err := cmdutil.NewStatsdClient(tag)
			if err != nil {
				return err
			}
			defer statsd.Close()

			poolSize := consumers / 4

			db, err := cmdutil.NewDatabasePool(ctx, poolSize)
			if err != nil {
				return err
			}
			defer db.Close()

			redis, err := cmdutil.NewRedisClient(ctx, consumers)
			if err != nil {
				return err
			}
			defer redis.Close()

			workerFn, ok := queues[queueID]
			if !ok {
				return fmt.Errorf("queue does not exist: %s", queueID)
			}
			worker := workerFn(ctx, logger, statsd, db, redis, consumers)

			mgr := faktoryworker.NewManager()
			mgr.Concurrency = consumers
			mgr.Register(queueID, worker.Process)
			return mgr.RunWithContext(ctx)
		},
	}

	cmd.Flags().IntVar(&consumers, "consumers", runtime.NumCPU()*64, "The consumers to run")
	cmd.Flags().StringVar(&queueID, "queue", "", "The queue to work on")

	return cmd
}
