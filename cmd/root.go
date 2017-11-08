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
	"runtime"
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
	Long:         buildtime.PROGNAME + ` is an parallel compression utility with throttling`,
	Example: `  $ echo 'Hello World' | smallz -c -9 - > stdout.gz
  $ smallz -dc stdout.gz
  Hello World
  $ echo 'Hello World' | time smallz -i=1B -c -0 -
  $ smallz -dc stdout.gz
  Hello World`,

	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		// Re-initialize logging with user-supplied configuration parameters
		{
			// os.Stderr isn't guaranteed to be thread-safe, wrap in a sync writer.
			// Files are guaranteed to be safe, terminals are not.
			var logWriter io.Writer
			if isatty.IsTerminal(os.Stderr.Fd()) || isatty.IsCygwinTerminal(os.Stderr.Fd()) {
				logWriter = zerolog.SyncWriter(os.Stderr)
			} else {
				logWriter = os.Stderr
			}

			logFmt, err := config.LogLevelParse(viper.GetString(config.KeyAgentLogFormat))
			if err != nil {
				return errors.Wrap(err, "unable to parse log format")
			}

			if logFmt == config.LogFormatAuto {
				if isatty.IsTerminal(os.Stderr.Fd()) || isatty.IsCygwinTerminal(os.Stderr.Fd()) {
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
		case viper.IsSet("decompress") && viper.GetBool("decompress"):
			return decompress(cmd, args)
		case viper.GetBool("compress"):
			return compress(cmd, args)
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
			description  = "Change the log level being sent to stderr"
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
		if isatty.IsTerminal(os.Stderr.Fd()) || isatty.IsCygwinTerminal(os.Stderr.Fd()) {
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
			longName     = "io-limit"
			shortName    = "i"
			defaultValue = "0B"
			description  = "Specify the rate at which IO should be ingested, 0B to disable"
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

	{
		const (
			longName    = "num-threads"
			shortName   = "t"
			description = "Specify the output file to use"
		)
		defaultValue := int(runtime.NumCPU())

		RootCmd.PersistentFlags().IntP(longName, shortName, defaultValue, description)
		viper.BindPFlag(longName, RootCmd.PersistentFlags().Lookup(longName))
		viper.SetDefault(longName, defaultValue)
	}

	{
		const (
			longName     = "block-size"
			shortName    = "b"
			defaultValue = "1MiB"
			description  = "Specify the block-size to use"
		)

		RootCmd.PersistentFlags().StringP(longName, shortName, defaultValue, description)
		viper.BindPFlag(longName, RootCmd.PersistentFlags().Lookup(longName))
		viper.SetDefault(longName, defaultValue)
	}

	{
		{
			var (
				// FIXME(seanc@): "compress-%d" should be a constant
				longName     = "compress-0"
				shortName    = "0"
				defaultValue = false
				description  = "Skip compressing the input"
			)

			RootCmd.PersistentFlags().BoolP(longName, shortName, defaultValue, description)
			viper.BindPFlag(longName, RootCmd.PersistentFlags().Lookup(longName))
			viper.SetDefault(longName, defaultValue)
		}

		for i := 1; i <= 9; i++ {
			var (
				// FIXME(seanc@): "compress-%d" should be a constant
				longName     = fmt.Sprintf("compress-%d", i)
				shortName    = fmt.Sprintf("%d", i)
				defaultValue = false
				description  = "Compress the input using the specified compression level"
			)

			if i == gzip.DefaultCompression {
				defaultValue = true
			}

			RootCmd.PersistentFlags().BoolP(longName, shortName, defaultValue, description)
			viper.BindPFlag(longName, RootCmd.PersistentFlags().Lookup(longName))
			viper.SetDefault(longName, defaultValue)
		}
	}
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	viper.SetConfigName(buildtime.PROGNAME)
}

// compress reads from a source (or stdin) and compresses.
//
// TODO(seanc@): Decompose this function into something halfway sane.
// Shell-script in Go much?  This is a sadness and needs to be cleaned up.
func compress(cmd *cobra.Command, args []string) (err error) {
	if len(args) != 1 {
		return fmt.Errorf("must have only one argument")
	}
	const mode = "compress"

	blockSize, err := units.ParseBase2Bytes(viper.GetString("block-size"))
	switch {
	case err != nil:
		return errors.Wrapf(err, "unable to parse %s", "block-size")
	case blockSize == 0:
		blockSize = 1 * units.MiB
	case blockSize < 0:
		return fmt.Errorf("block-size can't be negative")
	case blockSize > 1*units.GiB:
		return fmt.Errorf("block-size can't be greater than 1GiB")
	}
	log.Debug().Int("block-size-bytes", int(blockSize)).Str("block-size", blockSize.String()).Str("mode", mode).Msg("block-size")

	var bwlimitR units.Base2Bytes
	{
		if viper.IsSet("io-limit") {
			bwlimitR, err = units.ParseBase2Bytes(viper.GetString("io-limit"))
			if err != nil {
				return errors.Wrapf(err, "unable to parse %s", "io-limit")
			}

			log.Debug().Int("io-limit-bytes", int(bwlimitR)).Str("io-limit", bwlimitR.String()).Str("mode", mode).Msg("io-limit")
		}

		if bwlimitR == 0 {
			log.Debug().Str("mode", mode).Msg("io-limit disabled")
		}
	}

	blockingReaders := true

	var r *bufio.Reader
	var fr, fw *os.File
	if args[0] == "-" {
		if bwlimitR > 0 {
			log.Debug().Str("io-limit", bwlimitR.String()).Str("block-size", blockSize.String()).Str("file", "stdin").Str("mode", mode).Msg("input")
			bwR := bwlimit.NewBwlimit(int(bwlimitR), blockingReaders)
			bwr := bwR.Reader(os.Stdin)
			r = bufio.NewReaderSize(bwr, int(blockSize))
		} else {
			log.Debug().Str("file", "stdin").Str("mode", mode).Msg("input")
			r = bufio.NewReader(os.Stdin)
		}
	} else {
		var err error
		filename := args[0]
		fr, err = os.Open(filename)
		if err != nil {
			return errors.Wrapf(err, "unable to open %+q", filename)
		}

		if bwlimitR > 0 {
			bwR := bwlimit.NewBwlimit(int(bwlimitR), blockingReaders)
			r = bufio.NewReaderSize(bwR.Reader(fr), int(blockSize))
			log.Debug().Str("io-limit", bwlimitR.String()).Str("block-size", blockSize.String()).Str("file", filename).Str("mode", mode).Msg("input")
		} else {
			r = bufio.NewReader(fr)
			log.Debug().Str("file", filename).Str("mode", mode).Msg("input")
		}
	}

	var w io.Writer
	var bufioW *bufio.Writer
	if viper.GetBool("stdout") {
		log.Debug().Str("mode", mode).Msg("sending output to stdout")
		bufioW = bufio.NewWriterSize(os.Stdout, int(blockSize))
		w = bufioW
	} else {
		if !viper.IsSet("output") || viper.GetString("output") == "" {
			return fmt.Errorf("no output file set")
		}

		filename := viper.GetString("output")
		log.Debug().Str("filename", filename).Str("block-size", blockSize.String()).Str("mode", mode).Msg("output")
		if filename == "-" {
			fw = os.Stdout
		} else {
			fw, err = os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
			if err != nil {
				return errors.Wrapf(err, "unable to open %+q", filename)
			}
		}

		bufioW = bufio.NewWriterSize(fw, int(blockSize))
		w = bufioW
	}

	compressionLevel := getCompressionLevel(cmd)
	var gzW *gzip.Writer
	if compressionLevel != 0 {
		gzW, err = gzip.NewWriterLevel(w, compressionLevel)
		if err != nil {
			return errors.Wrap(err, "multiple compression levels specified")
		}
		w = gzW

		{
			numThreads := viper.GetInt("num-threads")
			if numThreads < 1 {
				return fmt.Errorf("num threads can't be negative")
			}
			log.Debug().Int("level", compressionLevel).Int("num-threads", numThreads).Str("block-size", blockSize.String()).Str("mode", mode).Msg("compression enabled")

			gzW.SetConcurrency(int(blockSize), numThreads)
		}
	} else {
		log.Debug().Str("mode", mode).Msg("compression disabled")
	}

	// Rushed implementation of io.Copy() that handles reading from a bwlimit
	// reader that may return 0 bytes if the bandwidth threshold has been
	// exceeded.
	buf := make([]byte, int(4*units.MiB))
READ_LOOP:
	for {
		nr, err := r.Read(buf)
		if err == io.EOF {
			break
		}

		if nr == 0 {
			// FIXME(seanc@): this sleep constant should be exposed by the upstream
			// library and reused here.
			time.Sleep(10 * time.Millisecond)
			continue READ_LOOP
		}

		// Iterate attempting to write
		off := 0
	WRITE_LOOP:
		for {
			nw, err := w.Write(buf[off:nr])
			if err != nil {
				return errors.Wrap(err, "unable to write buffer")
			}

			// Short-write, try again
			if nw == 0 {
				// FIXME(seanc@): this sleep constant should be exposed by the upstream
				// library and reused here.
				time.Sleep(10 * time.Millisecond)
				continue WRITE_LOOP
			}

			off += nw
			if off == nr {
				break WRITE_LOOP
			}
		}
	}

	if fr != nil {
		log.Debug().Str("mode", mode).Msg("closing input")
		if err := fr.Close(); err != nil {
			return errors.Wrap(err, "unable to close input")
		}
	}

	// Close writers in reverse order.  None of these can be deferred in order to
	// perform error handling.

	if gzW != nil {
		log.Debug().Str("mode", mode).Msg("flushing output compression stream")
		if err := gzW.Flush(); err != nil {
			return errors.Wrap(err, "error flushing gzip writer")
		}

		log.Debug().Str("mode", mode).Msg("closing output compression stream")
		if err := gzW.Close(); err != nil {
			return errors.Wrap(err, "error closing gzip writer")
		}
	}

	if bufioW != nil {
		log.Debug().Str("mode", mode).Msg("flushing output buffers")
		if err := bufioW.Flush(); err != nil {
			return errors.Wrap(err, "unable to flush output")
		}
	}

	if fw != nil {
		log.Debug().Str("mode", mode).Msg("closing output file")
		if err := fw.Close(); err != nil {
			return errors.Wrap(err, "unable to close output")
		}
	}

	return nil
}

// decompress reads from a source (or stdin) and decompresses.
//
// TODO(seanc@): Decompose this function into something halfway sane.
// Shell-script in Go much?  This is a sadness and needs to be cleaned up.
func decompress(cmd *cobra.Command, args []string) (err error) {
	if len(args) != 1 {
		return fmt.Errorf("must have only one argument")
	}
	const mode = "decompress"

	blockSize, err := units.ParseBase2Bytes(viper.GetString("block-size"))
	switch {
	case err != nil:
		return errors.Wrapf(err, "unable to parse %s", "block-size")
	case blockSize == 0:
		blockSize = 1 * units.MiB
	case blockSize < 0:
		return fmt.Errorf("block-size can't be negative")
	case blockSize > 1*units.GiB:
		return fmt.Errorf("block-size can't be greater than 1GiB")
	}
	log.Debug().Int("block-size-bytes", int(blockSize)).Str("block-size", blockSize.String()).Str("mode", mode).Msg("block-size")

	var bwlimitR units.Base2Bytes
	{
		if viper.IsSet("io-limit") {
			bwlimitR, err = units.ParseBase2Bytes(viper.GetString("io-limit"))
			if err != nil {
				return errors.Wrapf(err, "unable to parse %s", "io-limit")
			}

			log.Debug().Int("io-limit-bytes", int(bwlimitR)).Str("io-limit", bwlimitR.String()).Str("mode", mode).Msg("io-limit")
		}

		if bwlimitR == 0 {
			log.Debug().Str("mode", mode).Msg("io-limit disabled")
		}
	}

	blockingReaders := true

	var gzr *gzip.Reader
	var r io.Reader
	var fr, fw *os.File
	if args[0] == "-" {
		if bwlimitR > 0 {
			log.Debug().Str("io-limit", bwlimitR.String()).Str("block-size", blockSize.String()).Str("file", "stdin").Str("mode", mode).Msg("input")
			bwR := bwlimit.NewBwlimit(int(bwlimitR), blockingReaders)
			bwr := bwR.Reader(os.Stdin)
			gzr, err = gzip.NewReader(bufio.NewReaderSize(bwr, int(blockSize)))
			if err != nil {
				return errors.Wrap(err, "unable to decompress")
			}
		} else {
			log.Debug().Str("file", "stdin").Str("mode", mode).Msg("input")
			gzr, err = gzip.NewReader(bufio.NewReader(os.Stdin))
			if err != nil {
				return errors.Wrap(err, "unable to decompress")
			}
		}
		r = gzr
	} else {
		var err error
		filename := args[0]
		fr, err = os.Open(filename)
		if err != nil {
			return errors.Wrapf(err, "unable to open %+q", filename)
		}

		if bwlimitR > 0 {
			bwR := bwlimit.NewBwlimit(int(bwlimitR), blockingReaders)
			gzr, err = gzip.NewReader(bufio.NewReaderSize(bwR.Reader(fr), int(blockSize)))
			if err != nil {
				return errors.Wrap(err, "unable to decompress")
			}
			log.Debug().Str("io-limit", bwlimitR.String()).Str("block-size", blockSize.String()).Str("file", filename).Str("mode", mode).Msg("input")
		} else {
			gzr, err = gzip.NewReader(bufio.NewReader(fr))
			if err != nil {
				return errors.Wrap(err, "unable to decompress")
			}
			log.Debug().Str("file", filename).Str("mode", mode).Msg("input")
		}
		r = gzr
	}

	var w io.Writer
	var bufioW *bufio.Writer
	if viper.GetBool("stdout") {
		log.Debug().Str("mode", mode).Msg("sending output to stdout")
		bufioW = bufio.NewWriterSize(os.Stdout, int(blockSize))
		w = bufioW
	} else {
		if !viper.IsSet("output") || viper.GetString("output") == "" {
			return fmt.Errorf("no output file set")
		}

		filename := viper.GetString("output")
		log.Debug().Str("filename", filename).Str("block-size", blockSize.String()).Str("mode", mode).Msg("output")
		if filename == "-" {
			fw = os.Stdout
		} else {
			fw, err = os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
			if err != nil {
				return errors.Wrapf(err, "unable to open %+q", filename)
			}
		}

		bufioW = bufio.NewWriterSize(fw, int(blockSize))
		w = bufioW
	}

	// Rushed implementation of io.Copy() that handles reading from a bwlimit
	// reader that may return 0 bytes if the bandwidth threshold has been
	// exceeded.
	buf := make([]byte, int(4*units.MiB))
READ_LOOP:
	for {
		nr, err := r.Read(buf)
		if err == io.EOF {
			break
		}

		if nr == 0 {
			// FIXME(seanc@): this sleep constant should be exposed by the upstream
			// library and reused here.
			time.Sleep(10 * time.Millisecond)
			continue READ_LOOP
		}

		// Iterate attempting to write
		off := 0
	WRITE_LOOP:
		for {
			nw, err := w.Write(buf[off:nr])
			if err != nil {
				return errors.Wrap(err, "unable to write buffer")
			}

			// Short-write, try again
			if nw == 0 {
				// FIXME(seanc@): this sleep constant should be exposed by the upstream
				// library and reused here.
				time.Sleep(10 * time.Millisecond)
				continue WRITE_LOOP
			}

			off += nw
			if off == nr {
				break WRITE_LOOP
			}
		}
	}

	if fr != nil {
		log.Debug().Str("mode", mode).Msg("closing input")
		if err := fr.Close(); err != nil {
			return errors.Wrap(err, "unable to close input")
		}
	}

	// Close writers in reverse order.  None of these can be deferred in order to
	// perform error handling.

	if gzr != nil {
		log.Debug().Str("mode", mode).Msg("flushing output buffers")
		if err := gzr.Close(); err != nil {
			return errors.Wrap(err, "unable to close the decompression decoder")
		}
	}

	if bufioW != nil {
		log.Debug().Str("mode", mode).Msg("flushing output buffers")
		if err := bufioW.Flush(); err != nil {
			return errors.Wrap(err, "unable to flush output")
		}
	}

	if fw != nil {
		log.Debug().Str("mode", mode).Msg("closing output file")
		if err := fw.Close(); err != nil {
			return errors.Wrap(err, "unable to close output")
		}
	}

	return nil
}

// Return the first user selected compression level.  If the user selected
// multiple conflicting values, the value -1 will be returned.  If the user did
// not select any values, the default compression level (6) will be returned.  A
// value of 0 will pass the data through uncompressed (i.e. emulate pv(1)'s
// functionality).
func getCompressionLevel(cmd *cobra.Command) int {
	const initialValue = -2

	var lastValue int = initialValue
	for i := 9; i >= 0; i-- {
		// FIXME(seanc@): "compress-%d" should be a constant
		longName := fmt.Sprintf("compress-%d", i)
		if viper.IsSet(longName) && viper.GetBool(longName) {
			if lastValue != initialValue {
				lastValue = -1
				break
			}

			lastValue = i
		}
	}

	if lastValue == initialValue {
		return gzip.DefaultCompression
	}

	return lastValue
}
