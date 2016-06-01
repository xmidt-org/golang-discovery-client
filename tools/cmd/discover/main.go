package main

import (
	"flag"
	"fmt"
	"github.com/Comcast/golang-discovery-client/service"
	"github.com/samuel/go-zookeeper/zk"
	"os"
	"os/signal"
	"strings"
	"sync"
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

	// WatchUsage is the usage text for WatchFlag
	WatchUsage string = "comma-delimited list of service names to watch (no spaces)"
)

// newDiscoveryBuilder extracts a DiscoveryBuilder from the command line
func newDiscoveryBuilder() *service.DiscoveryBuilder {
	discoveryBuilder := &service.DiscoveryBuilder{}
	flag.StringVar(&discoveryBuilder.Connection, ConnectionFlag, DefaultConnection, ConnectionUsage)
	flag.StringVar(&discoveryBuilder.BasePath, BasePathFlag, "", BasePathUsage)

	var watchList string
	flag.StringVar(&watchList, WatchFlag, "", WatchUsage)

	flag.Parse()
	if len(watchList) == 0 {
		fmt.Fprintln(os.Stderr, "At least (1) watch is required")
		os.Exit(1)
	}

	discoveryBuilder.Watches = strings.Split(watchList, ",")
	return discoveryBuilder
}

func exec() int {
	waitGroup := &sync.WaitGroup{}
	shutdown := make(chan struct{})
	defer waitGroup.Wait()
	defer close(shutdown)

	logger := zk.DefaultLogger
	discoveryBuilder := newDiscoveryBuilder()
	discovery := discoveryBuilder.NewDiscovery(logger)
	monitor := NewMonitor(logger)

	if err := monitor.Run(waitGroup, shutdown); err != nil {
		logger.Printf("Unable to start monitor: %v", err)
		return 1
	}

	if err := discovery.Run(waitGroup, shutdown); err != nil {
		logger.Printf("Unable to start discovery client: %v", err)
		return 1
	}

	for _, serviceName := range discovery.ServiceNames() {
		discovery.AddListener(serviceName, monitor)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	<-signals
	return 0
}

func main() {
	os.Exit(exec())
}
