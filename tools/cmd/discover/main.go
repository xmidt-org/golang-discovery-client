package main

import (
	"flag"
	"fmt"
	"github.com/Comcast/golang-discovery-client/service"
	"github.com/Comcast/golang-discovery-client/service/cmd"
	"os"
	"os/signal"
)

func ServiceListener(logger service.Logger) service.Listener {
	return service.ListenerFunc(func(serviceName string, instances service.Instances) {
		logger.Info("Updated [%s] services: %s", serviceName, instances)
	})
}

func main() {
	cmd.ConfigureCommandLine()
	flag.Parse()
	discoveryBuilder := &service.DiscoveryBuilder{}
	cmd.ApplyCommandLine(discoveryBuilder)

	logger := &service.DefaultLogger{os.Stdout}
	discovery, err := discoveryBuilder.NewDiscovery(logger, true)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to start discovery client: %v\n", err)
		os.Exit(1)
	}

	for _, serviceName := range discovery.ServiceNames() {
		if instances, err := discovery.FetchServices(serviceName); err != nil {
			logger.Error("Unable to fetch initial [%s] services: %v", serviceName, err)
		} else {
			logger.Info("Initial [%s] services: %s", serviceName, instances)
		}
	}

	discovery.AddListenerForAll(ServiceListener(logger))

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	<-signals
	discovery.Close()
}
