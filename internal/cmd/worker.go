package cmd

import (
	"context"
	"fmt"
	"runtime"

	"github.com/spf13/cobra"

	"github.com/christianselig/apollo-backend/internal/cmdutil"
	"github.com/christianselig/apollo-backend/internal/worker"
)

var (
	queues = map[string]worker.NewWorkerFn{
		"live-activities":     worker.NewLiveActivitiesWorker,
		"notifications":       worker.NewNotificationsWorker,
		"stuck-notifications": worker.NewStuckNotificationsWorker,
		"subreddits":          worker.NewSubredditsWorker,
		"trending":            worker.NewTrendingWorker,
		"users":               worker.NewUsersWorker,
	}
)

func WorkerCmd(ctx context.Context) *cobra.Command {
	var multiplier int
	var queueID string

	cmd := &cobra.Command{
		Use:   "worker",
		Args:  cobra.ExactArgs(0),
		Short: "Work through job queues.",
		RunE: func(cmd *cobra.Command, args []string) error {
			if queueID == "" {
				return fmt.Errorf("need a queue to work on")
			}

			svc := fmt.Sprintf("worker: %s", queueID)
			logger := cmdutil.NewLogger(svc)
			defer func() { _ = logger.Sync() }()

			tag := fmt.Sprintf("worker:%s", queueID)
			statsd, err := cmdutil.NewStatsdClient(tag)
			if err != nil {
				return err
			}
			defer statsd.Close()

			consumers := runtime.NumCPU() * multiplier
			poolSize := multiplier / 4

			db, err := cmdutil.NewDatabasePool(ctx, poolSize)
			if err != nil {
				return err
			}
			defer db.Close()

			redis, err := cmdutil.NewRedisClient(ctx, multiplier)
			if err != nil {
				return err
			}
			defer redis.Close()

			queue, err := cmdutil.NewQueueClient(logger, redis, "worker")
			if err != nil {
				return err
			}

			workerFn, ok := queues[queueID]
			if !ok {
				return fmt.Errorf("invalid queue: %s", queueID)
			}

			worker := workerFn(ctx, logger, statsd, db, redis, queue, consumers)
			if err := worker.Start(); err != nil {
				return err
			}

			<-ctx.Done()

			worker.Stop()

			return nil
		},
	}

	cmd.Flags().IntVar(&multiplier, "multiplier", 32, "The multiplier (by CPUs) to run")
	cmd.Flags().StringVar(&queueID, "queue", "", "The queue to work on")

	return cmd
}
