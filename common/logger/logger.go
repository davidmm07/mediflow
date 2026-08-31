// Package logger provides a process-wide structured logger shared by all
// MediFlow services so log lines are consistently formatted and greppable
// across the fleet (service name, level, timestamp, request id).
package logger

import (
	"os"

	"github.com/rs/zerolog"
)

// New builds a zerolog.Logger tagged with the given service name. It writes
// pretty console output when MEDIFLOW_ENV=local and JSON otherwise, so local
// runs stay readable while container logs stay machine-parseable.
func New(service string) zerolog.Logger {
	level := zerolog.InfoLevel
	if lvl, err := zerolog.ParseLevel(os.Getenv("LOG_LEVEL")); err == nil {
		level = lvl
	}

	var writer zerolog.LevelWriter = zerolog.MultiLevelWriter(os.Stdout)
	if os.Getenv("MEDIFLOW_ENV") == "local" {
		console := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "15:04:05"}
		writer = zerolog.MultiLevelWriter(console)
	}

	return zerolog.New(writer).
		Level(level).
		With().
		Timestamp().
		Str("service", service).
		Logger()
}
