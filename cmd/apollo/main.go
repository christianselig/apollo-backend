package main

import (
	"context"
	"os"
	"os/signal"

	"github.com/christianselig/apollo-backend/internal/cmd"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer cancel()

	ret := cmd.Execute(ctx)
	os.Exit(ret)
}
