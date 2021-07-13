package cmd

import (
	"context"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/adjust/rmq/v4"
	"github.com/go-redis/redis/v8"
	_ "github.com/heroku/x/hmetrics/onload"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type Command struct {
	ctx    context.Context
	logger *logrus.Logger
	statsd *statsd.Client
	redis  *redis.Client
	jobs   *rmq.Connection
	db     *pgxpool.Pool
}

func Execute(ctx context.Context) int {
	_ = godotenv.Load()

	rootCmd := &cobra.Command{
		Use:   "apollo",
		Short: "Apollo is an amazing Reddit client for iOS. This isn't it, but it helps.",
	}

	rootCmd.AddCommand(APICmd(ctx))
	rootCmd.AddCommand(SchedulerCmd(ctx))
	rootCmd.AddCommand(WorkerCmd(ctx))

	if err := rootCmd.Execute(); err != nil {
		return 1
	}

	return 0
}
