package logging

import (
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Logger is the global structured logger for the gateway.
var Logger zerolog.Logger

// AddHook appends a zerolog.Hook to both the package-level Logger and the
// global zerolog logger so packages using "github.com/rs/zerolog/log" also
// receive the hook.
func AddHook(h zerolog.Hook) {
	Logger = Logger.Hook(h)
	log.Logger = log.Logger.Hook(h)
}

// useConsoleWriter returns true when human-readable console output should be
// used. The decision order is:
//  1. AETHER_LOG_FORMAT env var: "json" → false, "console" → true
//  2. TTY detection: stderr is a character device → true, otherwise → false
func useConsoleWriter() bool {
	switch strings.ToLower(os.Getenv("AETHER_LOG_FORMAT")) {
	case "json":
		return false
	case "console":
		return true
	}
	// Fall back to TTY detection.
	if fi, err := os.Stderr.Stat(); err == nil && (fi.Mode()&os.ModeCharDevice) != 0 {
		return true
	}
	return false
}

// Init initializes the global logger with the given level string.
// Valid levels: "debug", "info", "warn", "error". Defaults to "info".
// Output format is auto-detected from the stderr TTY status, but can be
// overridden with the AETHER_LOG_FORMAT environment variable ("json" or
// "console").
func Init(level string) {
	var lvl zerolog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = zerolog.DebugLevel
	case "info":
		lvl = zerolog.InfoLevel
	case "warn", "warning":
		lvl = zerolog.WarnLevel
	case "error":
		lvl = zerolog.ErrorLevel
	default:
		lvl = zerolog.InfoLevel
	}

	var logger zerolog.Logger
	if useConsoleWriter() {
		output := zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}
		logger = zerolog.New(output).With().Timestamp().Logger().Level(lvl)
	} else {
		logger = zerolog.New(os.Stderr).With().Timestamp().Logger().Level(lvl)
	}

	Logger = logger

	// Also set the global zerolog logger so packages using
	// "github.com/rs/zerolog/log" get the same format and level.
	log.Logger = logger
}
