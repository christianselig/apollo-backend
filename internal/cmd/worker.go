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

	cmd := &cobra.Command{
		Use:   "worker",
		Args:  cobra.ExactArgs(0),
		Short: "Work through job queues.",
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime.SetBlockProfileRate(1)
			runtime.SetMutexProfileFraction(1)

			svc := fmt.Sprintf("worker")
			logger := cmdutil.NewLogger(svc)
			defer func() { _ = logger.Sync() }()

			tag := fmt.Sprintf("worker")
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

			mgr := faktoryworker.NewManager()
			mgr.Concurrency = consumers
			for queue, workerFn := range queues {
				worker := workerFn(ctx, logger, statsd, db, redis, consumers)
				mgr.Register(queue, worker.Process)
			}

			return mgr.RunWithContext(ctx)
		},
	}

	cmd.Flags().IntVar(&consumers, "consumers", runtime.NumCPU()*64, "The consumers to run")

	return cmd
}
