// Copyright © 2017 Joyent, Inc.

package config

import (
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type Config struct {
	LogFormat LogFormat
	UseColors bool
}

func NewDefault() (cfg *Config, err error) {
	config := Config{
		UseColors: viper.GetBool(KeyAgentUseColor),
	}

	config.LogFormat, err = LogLevelParse(viper.GetString(KeyAgentLogFormat))
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse the log format")
	}

	return &config, nil
}
