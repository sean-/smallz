package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

const (
	KeyGoogleAgentEnable = "google.agent.enabled"

	KeyLogLevel = "log.level"

	KeyAgentLogFormat = "run.log-format"
	KeyPProfEnable    = "run.pprof.enable"
	KeyPProfPort      = "run.pprof.port"
	KeyAgentUseColor  = "run.use-color"
)

const (
	// Use a log format that resembles time.RFC3339Nano but includes all trailing
	// zeros so that we get fixed-width logging.
	LogTimeFormat = "2006-01-02T15:04:05.000000000Z07:00"
)

type LogFormat uint

const (
	LogFormatAuto LogFormat = iota
	LogFormatZerolog
	LogFormatHuman
)

func (f LogFormat) String() string {
	switch f {
	case LogFormatAuto:
		return "auto"
	case LogFormatZerolog:
		return "zerolog"
	case LogFormatHuman:
		return "human"
	default:
		panic(fmt.Sprintf("unknown log format: %d", f))
	}
}

func LogLevelParse(s string) (LogFormat, error) {
	switch logFormat := strings.ToLower(viper.GetString(KeyAgentLogFormat)); logFormat {
	case "auto":
		return LogFormatAuto, nil
	case "json", "zerolog":
		return LogFormatZerolog, nil
	case "human":
		return LogFormatHuman, nil
	default:
		return LogFormatAuto, fmt.Errorf("unsupported log format: %q", logFormat)
	}
}
