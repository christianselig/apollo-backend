package cmd

import (
	"context"
	"os"
	"strconv"

	"github.com/christianselig/apollo-backend/internal/api"
	"github.com/christianselig/apollo-backend/internal/cmdutil"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func APICmd(ctx context.Context) *cobra.Command {
	var port int

	cmd := &cobra.Command{
		Use:   "api",
		Args:  cobra.ExactArgs(0),
		Short: "Runs the RESTful API.",
		RunE: func(cmd *cobra.Command, args []string) error {
			port = 4000
			if os.Getenv("PORT") != "" {
				port, _ = strconv.Atoi(os.Getenv("PORT"))
			}

			logger := cmdutil.NewLogrusLogger(false)

			statsd, err := cmdutil.NewStatsdClient()
			if err != nil {
				return err
			}
			defer statsd.Close()

			db, err := cmdutil.NewDatabasePool(ctx, 1)
			if err != nil {
				return err
			}
			defer db.Close()

			redis, err := cmdutil.NewRedisClient(ctx)
			if err != nil {
				return err
			}
			defer redis.Close()

			api := api.NewAPI(ctx, logger, statsd, db)
			srv := api.Server(port)

			go func() { _ = srv.ListenAndServe() }()

			logger.WithFields(logrus.Fields{
				"port": port,
			}).Info("started api")

			<-ctx.Done()

			_ = srv.Shutdown(ctx)

			return nil
		},
	}

	return cmd
}
