// Copyright © 2017 Joyent, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"bufio"
	_ "expvar"
	"fmt"
	"io"
	stdlog "log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/alecthomas/units"
	"github.com/google/gops/agent"
	gzip "github.com/klauspost/pgzip"
	isatty "github.com/mattn/go-isatty"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/sean-/smallz/buildtime"
	"github.com/sean-/smallz/config"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/watermint/bwlimit"
)

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:          buildtime.PROGNAME + ` <FILE>`,
	Short:        buildtime.PROGNAME + ` CLI`,
	SilenceUsage: true,
	Args:         cobra.ExactArgs(1),
	Long:         buildtime.PROGNAME + ` is an optionally throttled compression utility`,
	Example: `  $ echo 'Hello World' | smallz -c -9 - > stdout.gz
  $ smallz -dc stdout.gz
  Hello World`,

	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		// Re-initialize logging with user-supplied configuration parameters
		{
			// os.Stdout isn't guaranteed to be thread-safe, wrap in a sync writer.
			// Files are guaranteed to be safe, terminals are not.
			var logWriter io.Writer
			if isatty.IsTerminal(os.Stdout.Fd()) || isatty.IsCygwinTerminal(os.Stdout.Fd()) {
				logWriter = zerolog.SyncWriter(os.Stdout)
			} else {
				logWriter = os.Stdout
			}

			logFmt, err := config.LogLevelParse(viper.GetString(config.KeyAgentLogFormat))
			if err != nil {
				return errors.Wrap(err, "unable to parse log format")
			}

			if logFmt == config.LogFormatAuto {
				if isatty.IsTerminal(os.Stdout.Fd()) || isatty.IsCygwinTerminal(os.Stdout.Fd()) {
					logFmt = config.LogFormatHuman
				} else {
					logFmt = config.LogFormatZerolog
				}
			}

			var zlog zerolog.Logger
			switch logFmt {
			case config.LogFormatZerolog:
				zlog = zerolog.New(logWriter).With().Timestamp().Logger()
			case config.LogFormatHuman:
				useColor := viper.GetBool(config.KeyAgentUseColor)
				w := zerolog.ConsoleWriter{
					Out:     logWriter,
					NoColor: !useColor,
				}
				zlog = zerolog.New(w).With().Timestamp().Logger()
			default:
				return fmt.Errorf("unsupported log format: %q")
			}

			log.Logger = zlog

			stdlog.SetFlags(0)
			stdlog.SetOutput(zlog)
		}

		// Perform input validation

		switch logLevel := strings.ToUpper(viper.GetString(config.KeyLogLevel)); logLevel {
		case "DEBUG":
			zerolog.SetGlobalLevel(zerolog.DebugLevel)
		case "INFO":
			zerolog.SetGlobalLevel(zerolog.InfoLevel)
		case "WARN":
			zerolog.SetGlobalLevel(zerolog.WarnLevel)
		case "ERROR":
			zerolog.SetGlobalLevel(zerolog.ErrorLevel)
		case "FATAL":
			zerolog.SetGlobalLevel(zerolog.FatalLevel)
		default:
			// FIXME(seanc@): move the supported log levels into a global constant
			return fmt.Errorf("unsupported error level: %q (supported levels: %s)", logLevel,
				strings.Join([]string{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}, " "))
		}

		go func() {
			if !viper.GetBool(config.KeyGoogleAgentEnable) {
				log.Debug().Msg("gops(1) agent disabled by request")
				return
			}

			log.Debug().Msg("starting gops(1) agent")
			if err := agent.Listen(&agent.Options{}); err != nil {
				log.Fatal().Err(err).Msg("unable to start the gops(1) agent thread")
			}
		}()

		go func() {
			if !viper.GetBool(config.KeyPProfEnable) {
				log.Debug().Msg("pprof endpoint disabled by request")
				return
			}

			pprofPort := viper.GetInt(config.KeyPProfPort)
			log.Debug().Int("pprof-port", pprofPort).Msg("starting pprof endpoing agent")
			if err := http.ListenAndServe(fmt.Sprintf("localhost:%d", pprofPort), nil); err != nil {
				log.Fatal().Err(err).Msg("unable to start the pprof listener")
			}
		}()

		return nil
	},

	RunE: func(cmd *cobra.Command, args []string) error {
		switch {
		case viper.GetBool("compress"):
			return compress(cmd, args)
		case viper.GetBool("decompress"):
			return decompress(cmd, args)
		default:
			return fmt.Errorf("compress or decompress must be specified")
		}
	},
}

func Execute() {
	if err := RootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	// os.Stderr isn't guaranteed to be thread-safe, wrap in a sync writer.  Files
	// are guaranteed to be safe, terminals are not.
	w := zerolog.ConsoleWriter{
		Out:     os.Stderr,
		NoColor: true,
	}
	zlog := zerolog.New(zerolog.SyncWriter(w)).With().Timestamp().Logger()

	cobra.OnInitialize(initConfig)

	zerolog.DurationFieldUnit = time.Microsecond
	zerolog.DurationFieldInteger = true
	zerolog.TimeFieldFormat = config.LogTimeFormat
	zerolog.SetGlobalLevel(zerolog.DebugLevel)

	log.Logger = zlog

	stdlog.SetFlags(0)
	stdlog.SetOutput(zlog)

	{
		const (
			key          = config.KeyLogLevel
			longName     = "log-level"
			shortName    = "l"
			defaultValue = "INFO"
			description  = "Log level"
		)

		RootCmd.PersistentFlags().StringP(longName, shortName, defaultValue, description)
		viper.BindPFlag(key, RootCmd.PersistentFlags().Lookup(longName))
		viper.SetDefault(key, defaultValue)
	}

	{
		const (
			key         = config.KeyAgentLogFormat
			longName    = "log-format"
			shortName   = "F"
			description = `Specify the log format ("auto", "zerolog", or "human")`
		)

		defaultValue := config.LogFormatAuto.String()
		RootCmd.PersistentFlags().StringP(longName, shortName, defaultValue, description)
		viper.BindPFlag(key, RootCmd.PersistentFlags().Lookup(longName))
		viper.SetDefault(key, defaultValue)
	}

	{
		const (
			key         = config.KeyAgentUseColor
			longName    = "use-color"
			shortName   = ""
			description = "Use ASCII colors"
		)

		defaultValue := false
		if isatty.IsTerminal(os.Stdout.Fd()) || isatty.IsCygwinTerminal(os.Stdout.Fd()) {
			defaultValue = true
		}

		RootCmd.PersistentFlags().BoolP(longName, shortName, defaultValue, description)
		viper.BindPFlag(key, RootCmd.PersistentFlags().Lookup(longName))
		viper.SetDefault(key, defaultValue)
	}

	{
		const (
			key          = config.KeyGoogleAgentEnable
			longName     = "enable-agent"
			shortName    = ""
			defaultValue = false
			description  = "Enable the gops(1) agent interface"
		)

		RootCmd.PersistentFlags().BoolP(longName, shortName, defaultValue, description)
		viper.BindPFlag(key, RootCmd.PersistentFlags().Lookup(longName))
		viper.SetDefault(key, defaultValue)
	}

	{
		const (
			key          = config.KeyPProfEnable
			longName     = "enable-pprof"
			shortName    = ""
			defaultValue = false
			description  = "Enable the pprof endpoint interface"
		)

		RootCmd.PersistentFlags().BoolP(longName, shortName, defaultValue, description)
		viper.BindPFlag(key, RootCmd.PersistentFlags().Lookup(longName))
		viper.SetDefault(key, defaultValue)
	}

	{
		const (
			key          = config.KeyPProfPort
			longName     = "pprof-port"
			shortName    = ""
			defaultValue = 4243
			description  = "Specify the pprof port"
		)

		RootCmd.PersistentFlags().Uint16P(longName, shortName, defaultValue, description)
		viper.BindPFlag(key, RootCmd.PersistentFlags().Lookup(longName))
		viper.SetDefault(key, defaultValue)
	}

	{
		const (
			longName     = "compress"
			shortName    = "C"
			defaultValue = true
			description  = "Compress the input"
		)

		RootCmd.PersistentFlags().BoolP(longName, shortName, defaultValue, description)
		viper.BindPFlag(longName, RootCmd.PersistentFlags().Lookup(longName))
		viper.SetDefault(longName, defaultValue)
	}

	{
		const (
			longName     = "decompress"
			shortName    = "d"
			defaultValue = false
			description  = "Decompress the input"
		)

		RootCmd.PersistentFlags().BoolP(longName, shortName, defaultValue, description)
		viper.BindPFlag(longName, RootCmd.PersistentFlags().Lookup(longName))
		viper.SetDefault(longName, defaultValue)
	}

	{
		const (
			longName     = "stdout"
			shortName    = "c"
			defaultValue = true
			description  = "This option specifies that output will go to the standard output stream, leaving files intact."
		)

		RootCmd.PersistentFlags().BoolP(longName, shortName, defaultValue, description)
		viper.BindPFlag(longName, RootCmd.PersistentFlags().Lookup(longName))
		viper.SetDefault(longName, defaultValue)
	}

	{
		const (
			longName     = "read-bwlimit"
			shortName    = "R"
			defaultValue = "0B"
			description  = "Specify the bandwidth limit for inputs in bytes per sceond, 0B to disable"
		)

		RootCmd.PersistentFlags().StringP(longName, shortName, defaultValue, description)
		viper.BindPFlag(longName, RootCmd.PersistentFlags().Lookup(longName))
		viper.SetDefault(longName, defaultValue)
	}

	{
		const (
			longName     = "write-bwlimit"
			shortName    = "W"
			defaultValue = "0B"
			description  = "Specify the bandwidth limit for outputs in bytes per sceond, 0B to disable"
		)

		RootCmd.PersistentFlags().StringP(longName, shortName, defaultValue, description)
		viper.BindPFlag(longName, RootCmd.PersistentFlags().Lookup(longName))
		viper.SetDefault(longName, defaultValue)
	}

	{
		const (
			longName     = "output"
			shortName    = "o"
			defaultValue = ""
			description  = "Specify the output file to use"
		)

		RootCmd.PersistentFlags().StringP(longName, shortName, defaultValue, description)
		viper.BindPFlag(longName, RootCmd.PersistentFlags().Lookup(longName))
		viper.SetDefault(longName, defaultValue)
	}
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	viper.SetConfigName(buildtime.PROGNAME)
}

// compress reads from a source (or stdin) and compresses.
func compress(cmd *cobra.Command, args []string) (err error) {
	if len(args) != 1 {
		return fmt.Errorf("must have only one argument")
	}

	var bwlimitR, bwlimitW int
	if viper.IsSet("read-bwlimit") {
		bwlimitRB, err := units.ParseBase2Bytes(viper.GetString("read-bwlimit"))
		if err != nil {
			return errors.Wrapf(err, "unable to parse %s", "read-bwlimit")
		}

		bwlimitR = int(bwlimitRB)
	}

	if viper.IsSet("write-bwlimit") {
		bwlimitWB, err := units.ParseBase2Bytes(viper.GetString("write-bwlimit"))
		if err != nil {
			return errors.Wrapf(err, "unable to parse %s", "write-bwlimit")
		}

		bwlimitW = int(bwlimitWB)
	}

	blockingReaders := true
	blockingWriters := true

	var bww *bwlimit.Writer
	var r *bufio.Reader
	var fr, fw *os.File
	if args[0] == "-" {
		if bwlimitR > 0 {
			bwR := bwlimit.NewBwlimit(bwlimitR, blockingReaders)
			bwr := bwR.Reader(os.Stdin)
			r = bufio.NewReader(bwr)
		} else {
			r = bufio.NewReader(os.Stdin)
		}
	} else {
		var err error
		fr, err = os.Open(args[0])
		if err != nil {
			return errors.Wrapf(err, "unable to open %+q", args[0])
		}

		if bwlimitR > 0 {
			bwR := bwlimit.NewBwlimit(bwlimitR, blockingReaders)
			r = bufio.NewReader(bwR.Reader(fr))
		} else {
			r = bufio.NewReader(fr)
		}
	}

	var w io.Writer
	var bufioW *bufio.Writer
	// var bufioR *bufio.Reader
	if viper.GetBool("stdout") {
		if bwlimitW > 0 {
			bufioW = bufio.NewWriter(os.Stdout)
			bwW := bwlimit.NewBwlimit(bwlimitW, blockingWriters)
			bww = bwW.Writer(bufioW)
			w = bww
		} else {
			w = bufio.NewWriter(os.Stdout)
		}
	} else {
		if !viper.IsSet("output") || viper.GetString("output") == "" {
			return fmt.Errorf("no output file set")
		}

		filename := viper.GetString("output")
		if filename == "-" {
			fw = os.Stdout
		} else {
			fw, err = os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
			if err != nil {
				return errors.Wrapf(err, "unable to open %+q", filename)
			}
		}

		if bwlimitW > 0 {
			bwW := bwlimit.NewBwlimit(bwlimitW, blockingWriters)
			bufioW = bufio.NewWriter(fw)
			bww = bwW.Writer(bufioW)
			w = bww
		} else {
			w = bufio.NewWriter(fw)
		}
	}

	gzW := gzip.NewWriter(w)
	gzW.SetConcurrency(100000, 10)

	// Rushed implementation of io.Copy() that handles reading/writing from a
	// buffer because the writer can perform a short-write and return 0, nil if
	// we've exceeded the write Bandwidth.
	//
	// TODO(seanc@): I wish it would just block for real.  It looks like blocking
	// writers aren't behaving as expected atm.  In the interim, this'll work.
	//
	// 	if _, err := io.Copy(gzW, r); err != nil {
	// 	return errors.Wrap(err, "unable to write")
	// }

	// buf := make([]byte, int(1*units.MiB))
	for {
		nw, err := r.WriteTo(gzW)
		fmt.Fprintf(os.Stderr, "wrote %d bytes\n", nw)
		if err != nil {
			if err == io.EOF {
				break
			}

			return errors.Wrap(err, "unable to write buffer")
		}

		if nw == 0 {
			break
			// This should be a user configurable sleep timer.
			time.Sleep(10 * time.Millisecond)
			continue
		}
	}

	// 	nr, err := r.Read(buf)
	// 	fmt.Fprintf(os.Stderr, "read %d bytes\n", nr)
	// 	if err == io.EOF {
	// 		break
	// 	}

	// 	// Iterate attempting to write
	// 	off := 0
	// 	for {
	// 		nw, err := gzW.Write(buf[off:nr])
	// 		fmt.Fprintf(os.Stderr, "wrote %d bytes\n", nw)
	// 		if err != nil {
	// 			return errors.Wrap(err, "unable to write buffer")
	// 		}

	// 		// Short-write, wrap to continue
	// 		if nw == 0 {
	// 			time.Sleep(100 * time.Millisecond)
	// 			continue
	// 		}

	// 		off += nw
	// 		if off >= nr {
	// 			break
	// 		}

	// 	}
	// }

	// if bufioR != nil {
	// 	if err := bufioR.Flush(); err != nil {
	// 		return errors.Wrap(err, "unable to flush buffered input")
	// 	}
	// }

	if fr != nil {
		if err := fr.Close(); err != nil {
			return errors.Wrap(err, "unable to close input")
		}
	}

	// Close writers in reverse order.  None of these can be deferred in order to
	// perform error handling.

	{
		if err := gzW.Flush(); err != nil {
			return errors.Wrap(err, "error flushing gzip writer")
		}

		if err := gzW.Close(); err != nil {
			return errors.Wrap(err, "error closing gzip writer")
		}
	}

	if bufioW != nil {
		if err := bufioW.Flush(); err != nil {
			return errors.Wrap(err, "unable to flush output")
		}
	}

	if bww != nil {
		if err := bww.Close(); err != nil {
			return errors.Wrap(err, "error closing throttling writer")
		}
	}

	if fw != nil {
		if err := fw.Close(); err != nil {
			return errors.Wrap(err, "unable to close output")
		}
	}

	return nil
}

func decompress(cmd *cobra.Command, args []string) error {
	return fmt.Errorf("decompress not implemented")
}
