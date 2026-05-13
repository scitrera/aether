// Command proxy-sidecar runs the Aether proxy sidecar. The sidecar is composed
// of independent surfaces (terminator, initiator, relay) that the operator
// enables individually in the YAML config; one process can run any subset
// concurrently over a single shared gateway connection. See
// server/internal/proxysidecar/ for the runtime definitions and
// configs/proxy-sidecar.dev.yaml for an example configuration.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/scitrera/aether/internal/proxysidecar"
	versionpkg "github.com/scitrera/aether/internal/version"
)

var (
	version = versionpkg.Version

	configFile  = flag.String("config", "configs/proxy-sidecar.yaml", "Path to configuration file")
	devMode     = flag.Bool("dev", false, "Allow startup with development defaults")
	showVersion = flag.Bool("version", false, "Show version and exit")
	showHelp    = flag.Bool("help", false, "Show this help message")
)

const banner = `
 ____                       ____  _     _
|  _ \ _ __ _____  ___   _ / ___|(_) __| | ___  ___ __ _ _ __
| |_) | '__/ _ \ \/ / | | |\___ \| |/ _' |/ _ \/ __/ _' | '__|
|  __/| | | (_) >  <| |_| | ___) | | (_| |  __/ (_| (_| | |
|_|   |_|  \___/_/\_\\__, |____/|_|\__,_|\___|\___\__,_|_|
                     |___/

Aether Proxy Sidecar v%s
`

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, banner, version)
		fmt.Fprintf(os.Stderr, "\nUsage: %s [options] [-- <cmd> [args...]]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Surfaces (each opt-in via the YAML config):\n")
		fmt.Fprintf(os.Stderr, "  terminator  gateway -> local backend (HTTP/TCP/UDP/WS)\n")
		fmt.Fprintf(os.Stderr, "  initiator   local HTTP -> gateway\n")
		fmt.Fprintf(os.Stderr, "  relay       sandbox gRPC -> gateway (with credential injection)\n\n")
		fmt.Fprintf(os.Stderr, "Supervisor mode:\n")
		fmt.Fprintf(os.Stderr, "  When `-- <cmd> [args...]` is supplied, the sidecar starts the\n")
		fmt.Fprintf(os.Stderr, "  enabled surfaces, then execs the wrapped process with inherited\n")
		fmt.Fprintf(os.Stderr, "  stdio/env. SIGINT/SIGTERM are forwarded to the child; the sidecar\n")
		fmt.Fprintf(os.Stderr, "  exits with the wrapped process's status when it terminates.\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
	}

	parentArgs, childArgv := proxysidecar.SplitChildArgs(os.Args)
	os.Args = parentArgs
	flag.Parse()

	if *showHelp {
		flag.Usage()
		os.Exit(0)
	}
	if *showVersion {
		fmt.Printf("Aether Proxy Sidecar v%s\n", version)
		os.Exit(0)
	}

	fmt.Fprintf(os.Stderr, banner, version)

	cfg, err := loadConfig()
	if err != nil {
		log.Fatal().Err(err).Msg("load configuration")
	}
	if err := cfg.Validate(); err != nil {
		log.Fatal().Err(err).Msg("invalid configuration")
	}

	initLogger(cfg.Logging.Level, cfg.Logging.Format)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	hupCh := make(chan os.Signal, 1)
	signal.Notify(hupCh, syscall.SIGHUP)

	runner, err := proxysidecar.NewRunner(cfg, *configFile)
	if err != nil {
		log.Fatal().Err(err).Msg("build runner")
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.Run(ctx)
	}()

	var (
		sup         *proxysidecar.Supervisor
		childCh     <-chan struct{}
		childExited bool
	)
	if len(childArgv) > 0 {
		s, err := proxysidecar.NewSupervisor(childArgv)
		if err != nil {
			cancel()
			log.Fatal().Err(err).Msg("supervisor: new")
		}
		if err := s.Start(ctx); err != nil {
			cancel()
			log.Fatal().Err(err).Msg("supervisor: start")
		}
		sup = s
		childCh = sup.Done()
		log.Info().Strs("argv", childArgv).Msg("supervisor: wrapped process started")
	}

	for {
		select {
		case <-hupCh:
			log.Info().Msg("received SIGHUP, reloading config")
			go runner.Reload()
		case sig := <-sigCh:
			if sup != nil && !childExited {
				log.Info().Str("signal", sig.String()).Msg("forwarding signal to wrapped process")
				if err := sup.Signal(sig); err != nil {
					log.Warn().Err(err).Str("signal", sig.String()).Msg("supervisor: forward signal failed")
				}
				continue
			}
			log.Info().Str("signal", sig.String()).Msg("received signal, shutting down")
			cancel()
			goto done
		case <-childCh:
			childExited = true
			log.Info().Int("status", sup.ExitCode()).Msg("wrapped process exited, shutting down")
			cancel()
			goto done
		case err := <-errCh:
			if err != nil {
				if sup != nil && !childExited {
					log.Error().Err(err).Msg("proxy sidecar error; signalling wrapped process")
					_ = sup.Signal(syscall.SIGTERM)
					<-childCh
					childExited = true
					goto done
				}
				log.Fatal().Err(err).Msg("proxy sidecar error")
			}
			goto done
		}
	}
done:

	<-time.After(2 * time.Second)
	log.Info().Msg("Aether Proxy Sidecar stopped")
	if sup != nil {
		os.Exit(sup.ExitCode())
	}
}

func loadConfig() (*proxysidecar.Config, error) {
	if _, err := os.Stat(*configFile); os.IsNotExist(err) {
		if !*devMode {
			return nil, fmt.Errorf("config file not found: %s (pass -dev for defaults)", *configFile)
		}
		log.Warn().Str("path", *configFile).Msg("config file not found, using development defaults")
		return devDefaults(), nil
	}
	return proxysidecar.LoadConfig(*configFile)
}

func devDefaults() *proxysidecar.Config {
	return &proxysidecar.Config{
		Gateway: proxysidecar.GatewayConfig{
			Address:  "localhost:50051",
			Insecure: true,
		},
		Service: proxysidecar.ServiceConfig{
			Implementation: "proxy-sidecar",
			Specifier:      "instance-1",
		},
		Terminator: proxysidecar.TerminatorConfig{
			Enabled: true,
			Backends: []proxysidecar.BackendConfig{{
				Name:         "default",
				Kind:         proxysidecar.BackendKindHTTP,
				URL:          "http://localhost:61001",
				AllowPaths:   []string{"/*"},
				AllowMethods: []string{"GET", "POST", "PUT", "DELETE"},
				HeaderMode:   proxysidecar.HeaderModeStrict,
			}},
		},
		Logging: proxysidecar.LoggingConfig{Level: "info"},
	}
}

// initLogger configures both the package-level zerolog logger used directly
// (log.Info, log.Error, ...) and the DefaultContextLogger consulted via
// zerolog.Ctx(ctx). Setting only DefaultContextLogger leaves package-level
// log calls writing through zerolog's untouched defaults — which silently
// ignores cfg.Logging.Level.
func initLogger(level, format string) {
	var lvl zerolog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = zerolog.DebugLevel
	case "warn", "warning":
		lvl = zerolog.WarnLevel
	case "error":
		lvl = zerolog.ErrorLevel
	default:
		lvl = zerolog.InfoLevel
	}

	useConsole := format == "console"
	if format == "" {
		if fi, err := os.Stderr.Stat(); err == nil && (fi.Mode()&os.ModeCharDevice) != 0 {
			useConsole = true
		}
	}
	var logger zerolog.Logger
	if useConsole {
		out := zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}
		logger = zerolog.New(out).With().Timestamp().Logger().Level(lvl)
	} else {
		logger = zerolog.New(os.Stderr).With().Timestamp().Logger().Level(lvl)
	}
	log.Logger = logger
	zerolog.DefaultContextLogger = &logger
}
