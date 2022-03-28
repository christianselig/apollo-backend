package cmd

import (
	"context"
	"os"
	"runtime"
	"runtime/pprof"

	"github.com/bugsnag/bugsnag-go/v2"
	_ "github.com/heroku/x/hmetrics/onload"
	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
)

func Execute(ctx context.Context) int {
	_ = godotenv.Load()

	if key, ok := os.LookupEnv("BUGSNAG_API_KEY"); ok {
		bugsnag.Configure(bugsnag.Configuration{
			APIKey:          key,
			ReleaseStage:    os.Getenv("ENV"),
			ProjectPackages: []string{"main", "github.com/christianselig/apollo-backend"},
		})
	}

	profile := false

	rootCmd := &cobra.Command{
		Use:   "apollo",
		Short: "Apollo is an amazing Reddit client for iOS. This isn't it, but it helps.",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if !profile {
				return nil
			}

			f, perr := os.Create("cpu.pprof")
			if perr != nil {
				return perr
			}

			_ = pprof.StartCPUProfile(f)
			return nil
		},
		PersistentPostRunE: func(cmd *cobra.Command, args []string) error {
			if !profile {
				return nil
			}

			pprof.StopCPUProfile()

			f, perr := os.Create("mem.pprof")
			if perr != nil {
				return perr
			}
			defer f.Close()

			runtime.GC()
			err := pprof.WriteHeapProfile(f)
			return err
		},
	}

	rootCmd.PersistentFlags().BoolVarP(&profile, "profile", "p", false, "record CPU pprof")

	rootCmd.AddCommand(APICmd(ctx))
	rootCmd.AddCommand(SchedulerCmd(ctx))
	rootCmd.AddCommand(WorkerCmd(ctx))

	if err := rootCmd.Execute(); err != nil {
		return 1
	}

	return 0
}
