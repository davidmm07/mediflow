// Package server runs an http.Handler with the graceful-shutdown behaviour
// every MediFlow service needs: stop accepting new connections on SIGTERM,
// let in-flight requests drain, then run caller-supplied cleanup.
package server

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
)

// Run starts an HTTP server on addr and blocks until the process receives
// SIGINT/SIGTERM, then shuts down with a 15s drain window. cleanup runs
// after the server stops, for closing Kafka writers and Mongo clients.
func Run(addr string, handler http.Handler, log zerolog.Logger, cleanup func()) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info().Str("addr", addr).Msg("http server listening")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-errCh:
		return err
	case sig := <-stop:
		log.Info().Str("signal", sig.String()).Msg("shutting down")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	err := srv.Shutdown(ctx)
	if cleanup != nil {
		cleanup()
	}
	return err
}
