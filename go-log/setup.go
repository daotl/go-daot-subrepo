package log

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/mattn/go-isatty"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

func init() {
	SetupLogging(configFromEnv())
}

// Logging environment variables
const (
	envLogging    = "GOLOG_LOG_LEVEL"
	envLoggingFmt = "GOLOG_LOG_FMT"
	envAutoColor  = "GOLOG_AUTO_COLOR"

	envLoggingFile = "GOLOG_FILE" // /path/to/file
	envLoggingURL  = "GOLOG_URL"  // url that will be processed by sink in the zap

	envLoggingOutput = "GOLOG_OUTPUT"     // possible values: stdout|stderr|file combine multiple values with '+'
	envLoggingLabels = "GOLOG_LOG_LABELS" // comma-separated key-value pairs, i.e. "app=example_app,dc=sjc-1"
)

type LogFormat int

const (
	ColorizedOutput LogFormat = iota
	PlaintextOutput
	JSONOutput
	ColorizedCompactOutput
	CompactOutput
)

type Config struct {
	// Format overrides the format of the log output. Defaults to ColorizedOutput
	Format LogFormat

	// AutoColor automatically switches between formats and their colorized counterparts
	// (ColorizedOutput <-> PlaintextOutput, ColorizedCompactOutput <-> CompactOutput),
	// if the current program is run from a terminal it switches to the colorized version, vice versa.
	AutoColor bool

	// Level is the default minimum enabled logging level.
	Level LogLevel

	// SubsystemLevels are the default levels per-subsystem. When unspecified, defaults to Level.
	SubsystemLevels map[string]LogLevel

	// Sampling configs log sampling, disabled if nil.
	Sampling *zap.SamplingConfig

	// Stdout indicates whether logs should be written to stdout.
	Stdout bool

	// Stderr indicates whether logs should be written to stderr.
	Stderr bool

	// File is a path to a file that logs will be written to.
	File string

	// Lumberjack configs log rolling using Lumberjack, disabled if nil.
	// Lumberjack.Filename will be ignored and set to File.
	Lumberjack *lumberjack.Logger

	// URL with schema supported by zap. Use zap.RegisterSink
	URL string

	// AutoStdout automatically enables stdout output if the current program is
	// run from a terminal, or ((File is not set or not correct) and (URL is not set)).
	AutoStdout bool

	// Labels is a set of key-values to apply to all loggers
	Labels map[string]string
}

// ErrNoSuchLogger is returned when the util pkg is asked for a non existant logger
var ErrNoSuchLogger = errors.New("error: No such logger")

var loggerMutex sync.RWMutex // guards access to global logger state

// loggers is the set of loggers in the system
var loggers = make(map[string]*zap.SugaredLogger)
var levels = make(map[string]zap.AtomicLevel)

// primaryFormat is the format of the primary core used for logging
var primaryFormat LogFormat = ColorizedOutput

// defaultLevel is the default log level
var defaultLevel LogLevel = LevelError

// primaryCore is the primary logging core
var primaryCore zapcore.Core

// loggerCore is the base for all loggers created by this package
var loggerCore = &lockedMultiCore{}

// SetupLogging will initialize the logger backend and set the flags.
// TODO calling this in `init` pushes all configuration to env variables
// - move it out of `init`? then we need to change all the code (js-ipfs, go-ipfs) to call this explicitly
// - have it look for a config file? need to define what that is
func SetupLogging(cfg Config) {
	loggerMutex.Lock()
	defer loggerMutex.Unlock()

	isTTY := isTerm(os.Stdout)

	primaryFormat = cfg.Format
	if cfg.AutoColor {
		if cfg.Format == ColorizedOutput && !isTTY {
			primaryFormat = PlaintextOutput
		} else if cfg.Format == PlaintextOutput && isTTY {
			primaryFormat = ColorizedOutput
		} else if cfg.Format == ColorizedCompactOutput && !isTTY {
			primaryFormat = CompactOutput
		} else if cfg.Format == CompactOutput && isTTY {
			primaryFormat = ColorizedCompactOutput
		}
	}

	defaultLevel = cfg.Level

	outputPaths := []string{}

	fileSet := false
	var lumberjackWriteSyncer zapcore.WriteSyncer
	// check if we log to a file
	if len(cfg.File) > 0 {
		if path, err := normalizePath(cfg.File); err != nil {
			fmt.Fprintf(os.Stderr, "failed to resolve log path '%q', logging to %s\n", cfg.File, outputPaths)
		} else {
			fileSet = true
			if cfg.Lumberjack != nil {
				cfg.Lumberjack.Filename = path
				lumberjackWriteSyncer = zapcore.AddSync(cfg.Lumberjack)
			} else {
				outputPaths = append(outputPaths, path)
			}
		}
	}

	urlSet := false
	if len(cfg.URL) > 0 {
		urlSet = true
		outputPaths = append(outputPaths, cfg.URL)
	}

	noFileOrURL := !(fileSet || urlSet)
	if cfg.Stdout || cfg.AutoStdout && (isTTY || noFileOrURL) {
		outputPaths = append(outputPaths, "stdout")
	}
	if cfg.Stderr {
		outputPaths = append(outputPaths, "stderr")
	}

	ws, _, err := zap.Open(outputPaths...)
	if err != nil {
		panic(fmt.Sprintf("unable to open logging output: %v", err))
	}

	if lumberjackWriteSyncer != nil {
		ws = zap.CombineWriteSyncers(ws, lumberjackWriteSyncer)
	}

	newPrimaryCore := newCore(primaryFormat, ws, LevelDebug) // the main core needs to log everything.

	// Enable log sampling, see https://github.com/uber-go/zap/blob/7b21229fb3f063275f4f169f8a79ad30aa001c51/config.go#L217
	if scfg := cfg.Sampling; scfg != nil {
		var samplerOpts []zapcore.SamplerOption
		if scfg.Hook != nil {
			samplerOpts = append(samplerOpts, zapcore.SamplerHook(scfg.Hook))
		}
		newPrimaryCore = zapcore.NewSamplerWithOptions(
			newPrimaryCore,
			time.Second,
			scfg.Initial,
			scfg.Thereafter,
			samplerOpts...,
		)
	}

	for k, v := range cfg.Labels {
		newPrimaryCore = newPrimaryCore.With([]zap.Field{zap.String(k, v)})
	}

	setPrimaryCore(newPrimaryCore)
	setAllLoggers(defaultLevel)

	for name, level := range cfg.SubsystemLevels {
		if leveler, ok := levels[name]; ok {
			leveler.SetLevel(zapcore.Level(level))
		} else {
			levels[name] = zap.NewAtomicLevelAt(zapcore.Level(level))
		}
	}
}

// SetPrimaryCore changes the primary logging core. If the SetupLogging was
// called then the previously configured core will be replaced.
func SetPrimaryCore(core zapcore.Core) {
	loggerMutex.Lock()
	defer loggerMutex.Unlock()

	setPrimaryCore(core)
}

func setPrimaryCore(core zapcore.Core) {
	if primaryCore != nil {
		loggerCore.ReplaceCore(primaryCore, core)
	} else {
		loggerCore.AddCore(core)
	}
	primaryCore = core
}

// SetDebugLogging calls SetAllLoggers with logging.DEBUG
func SetDebugLogging() {
	SetAllLoggers(LevelDebug)
}

// SetAllLoggers changes the logging level of all loggers to lvl
func SetAllLoggers(lvl LogLevel) {
	loggerMutex.RLock()
	defer loggerMutex.RUnlock()

	setAllLoggers(lvl)
}

func setAllLoggers(lvl LogLevel) {
	for _, l := range levels {
		l.SetLevel(zapcore.Level(lvl))
	}
}

// SetLogLevel changes the log level of a specific subsystem
// name=="*" changes all subsystems
func SetLogLevel(name, level string) error {
	lvl, err := LevelFromString(level)
	if err != nil {
		return err
	}

	// wildcard, change all
	if name == "*" {
		SetAllLoggers(lvl)
		return nil
	}

	loggerMutex.RLock()
	defer loggerMutex.RUnlock()

	// Check if we have a logger by that name
	if _, ok := levels[name]; !ok {
		return ErrNoSuchLogger
	}

	levels[name].SetLevel(zapcore.Level(lvl))

	return nil
}

// SetLogLevelRegex sets all loggers to level `l` that match expression `e`.
// An error is returned if `e` fails to compile.
func SetLogLevelRegex(e, l string) error {
	lvl, err := LevelFromString(l)
	if err != nil {
		return err
	}

	rem, err := regexp.Compile(e)
	if err != nil {
		return err
	}

	loggerMutex.Lock()
	defer loggerMutex.Unlock()
	for name := range loggers {
		if rem.MatchString(name) {
			levels[name].SetLevel(zapcore.Level(lvl))
		}
	}
	return nil
}

// GetSubsystems returns a slice containing the
// names of the current loggers
func GetSubsystems() []string {
	loggerMutex.RLock()
	defer loggerMutex.RUnlock()
	subs := make([]string, 0, len(loggers))

	for k := range loggers {
		subs = append(subs, k)
	}
	return subs
}

func getLogger(name string) *zap.SugaredLogger {
	loggerMutex.Lock()
	defer loggerMutex.Unlock()
	log, ok := loggers[name]
	if !ok {
		level, ok := levels[name]
		if !ok {
			level = zap.NewAtomicLevelAt(zapcore.Level(defaultLevel))
			levels[name] = level
		}
		log = zap.New(loggerCore).
			WithOptions(
				zap.IncreaseLevel(level),
				zap.AddCaller(),
			).
			Named(name).
			Sugar()

		loggers[name] = log
	}

	return log
}

// configFromEnv returns a Config with defaults populated using environment variables.
func configFromEnv() Config {
	cfg := Config{
		Format:          ColorizedOutput,
		Stderr:          true,
		Level:           LevelError,
		SubsystemLevels: map[string]LogLevel{},
		Labels:          map[string]string{},
	}

	format := os.Getenv(envLoggingFmt)

	var noExplicitFormat bool

	switch format {
	case "color":
		cfg.Format = ColorizedOutput
	case "nocolor":
		cfg.Format = PlaintextOutput
	case "json":
		cfg.Format = JSONOutput
	case "compactcolor":
		cfg.Format = ColorizedCompactOutput
	case "compactnocolor":
		cfg.Format = CompactOutput
	default:
		if format != "" {
			fmt.Fprintf(os.Stderr, "ignoring unrecognized log format '%s'\n", format)
		}
		noExplicitFormat = true
	}

	autoColorStr, autoColor := os.LookupEnv(envAutoColor)
	if autoColor {
		autoColorStr = strings.ToLower(strings.TrimSpace(autoColorStr))
		if autoColorStr == "no" || autoColorStr == "false" {
			autoColor = false
		}
	}
	cfg.AutoColor = autoColor

	lvl := os.Getenv(envLogging)
	if lvl != "" {
		for _, kvs := range strings.Split(lvl, ",") {
			kv := strings.SplitN(kvs, "=", 2)
			lvl, err := LevelFromString(kv[len(kv)-1])
			if err != nil {
				fmt.Fprintf(os.Stderr, "error setting log level %q: %s\n", kvs, err)
				continue
			}
			switch len(kv) {
			case 1:
				cfg.Level = lvl
			case 2:
				cfg.SubsystemLevels[kv[0]] = lvl
			}
		}
	}

	cfg.File = os.Getenv(envLoggingFile)
	// Disable stderr logging when a file is specified
	// https://github.com/ipfs/go-log/issues/83
	if cfg.File != "" {
		cfg.Stderr = false
	}

	cfg.URL = os.Getenv(envLoggingURL)
	output := os.Getenv(envLoggingOutput)
	outputOptions := strings.Split(output, "+")
	for _, opt := range outputOptions {
		switch opt {
		case "stdout":
			cfg.Stdout = true
		case "stderr":
			cfg.Stderr = true
		case "file":
			if cfg.File == "" {
				fmt.Fprint(os.Stderr, "please specify a GOLOG_FILE value to write to")
			}
		case "url":
			if cfg.URL == "" {
				fmt.Fprint(os.Stderr, "please specify a GOLOG_URL value to write to")
			}
		}
	}

	// Check that neither of the requested Std* nor the file are TTYs
	// At this stage (configFromEnv) we do not have a uniform list to examine yet
	if noExplicitFormat &&
		!(cfg.Stdout && isTerm(os.Stdout)) &&
		!(cfg.Stderr && isTerm(os.Stderr)) &&
		// check this last: expensive
		!(cfg.File != "" && pathIsTerm(cfg.File)) {
		cfg.Format = PlaintextOutput
	}

	labels := os.Getenv(envLoggingLabels)
	if labels != "" {
		labelKVs := strings.Split(labels, ",")
		for _, label := range labelKVs {
			kv := strings.Split(label, "=")
			if len(kv) != 2 {
				fmt.Fprint(os.Stderr, "invalid label k=v: ", label)
				continue
			}
			cfg.Labels[kv[0]] = kv[1]
		}
	}

	return cfg
}

func isTerm(f *os.File) bool {
	return isatty.IsTerminal(f.Fd()) || isatty.IsCygwinTerminal(f.Fd())
}

func pathIsTerm(p string) bool {
	// !!!no!!! O_CREAT, if we fail - we fail
	f, err := os.OpenFile(p, os.O_WRONLY, 0)
	if f != nil {
		defer f.Close() // nolint:errcheck
	}
	return err == nil && isTerm(f)
}
