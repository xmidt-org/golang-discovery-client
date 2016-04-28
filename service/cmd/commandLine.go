// Package cmd provides a standard way to extract a Discovery from a command-line
package cmd

import (
	"flag"
	"github.com/Comcast/golang-discovery-client/service"
	"strings"
)

const (
	// ConnectionFlag is the command line flag used to specify the Curator connection string
	ConnectionFlag string = "connection"

	// DefaultConnection is the default connection string
	DefaultConnection string = "127.0.0.1:2181"

	// ConnectionUsage is the usage text for ConnectionFlag
	ConnectionUsage string = "the Curator connection string (comma-delimited list of host:port entries without spaces)"

	// BasePathFlag is the command line flag used to specify the base service path
	BasePathFlag string = "basePath"

	// BasePathUsage is the usage text for BasePathFlag
	BasePathUsage string = "the parent znode path for services"

	// WatchFlag is the command line flag used to specify the names of one or more services to watch
	WatchFlag string = "watch"

	// BasePathUsage is the usage text for WatchFlag
	WatchUsage string = "comma-delimited list of service names to watch (no spaces)"
)

// ConfigureFlagSet adds the standard set of command-line options to a
// given FlagSet
func ConfigureFlagSet(flagSet *flag.FlagSet) {
	flagSet.String(ConnectionFlag, DefaultConnection, ConnectionUsage)
	flagSet.String(BasePathFlag, "", BasePathUsage)
	flagSet.String(WatchFlag, "", WatchUsage)
}

// ConfigureCommandLine configures the default, internal flag.CommandLine
// with the standard set of command-line options
func ConfigureCommandLine() {
	ConfigureFlagSet(flag.CommandLine)
}

// ApplyFlagSet applies the set flags to a discovery builder
func ApplyFlagSet(discoveryBuilder *service.DiscoveryBuilder, flagSet *flag.FlagSet) {
	flagSet.VisitAll(func(flag *flag.Flag) {
		switch flag.Name {
		case ConnectionFlag:
			discoveryBuilder.Connection = flag.Value.String()

		case BasePathFlag:
			discoveryBuilder.BasePath = flag.Value.String()

		case WatchFlag:
			discoveryBuilder.Watches = strings.Split(flag.Value.String(), ",")
		}
	})
}

// ApplyCommandLine applies the default, internal flag.CommandLine to the given
// discovery builder
func ApplyCommandLine(discoveryBuilder *service.DiscoveryBuilder) {
	ApplyFlagSet(discoveryBuilder, flag.CommandLine)
}
