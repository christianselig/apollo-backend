package cmd

import (
	"context"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/pprof"

	"github.com/bugsnag/bugsnag-go/v2"
	_ "github.com/heroku/x/hmetrics/onload"
	"github.com/joho/godotenv"
	"github.com/spf13/cobra"

	_ "github.com/honeycombio/honeycomb-opentelemetry-go"
	"github.com/honeycombio/opentelemetry-go-contrib/launcher"
)

func Execute(ctx context.Context) int {
	_ = godotenv.Load()

	if key, ok := os.LookupEnv("BUGSNAG_API_KEY"); ok {
		bugsnag.Configure(bugsnag.Configuration{
			APIKey:          key,
			ReleaseStage:    os.Getenv("ENV"),
			ProjectPackages: []string{"main", "github.com/christianselig/apollo-backend"},
			AppVersion:      os.Getenv("RENDER_GIT_COMMIT"),
		})
	}

	otelShutdown, err := launcher.ConfigureOpenTelemetry()
	if err == nil {
		defer otelShutdown()
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

	go func() {
		_ = http.ListenAndServe("localhost:6060", nil)
	}()

	if err := rootCmd.Execute(); err != nil {
		return 1
	}

	return 0
}
