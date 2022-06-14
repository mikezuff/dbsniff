package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/mikezuff/dbsniff"
)

func main() {
	connStr := os.Getenv("PSQL_CONN")
	if connStr == "" {
		fmt.Fprintln(os.Stderr, "PSQL_CONN unset")
		os.Exit(1)
	}

	lAddr := "127.0.0.1:6543"
	s, err := dbsniff.NewPostgresServer(lAddr, connStr)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
		sig := <-signals
		fmt.Println("shutdown on signal:", sig)
		s.Shutdown()
	}()

	s.Serve()
}
