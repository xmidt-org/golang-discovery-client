package main

import (
	"flag"
	"github.com/Comcast/golang-discovery-client/service"
	"github.com/Comcast/golang-discovery-client/service/cmd"
	"os"
	"os/signal"
)

func printServices(logger service.Logger, serviceName string, oldInstances, newInstances service.Instances) {
	logger.Info("\t")
	logger.Info("\tService: %s", serviceName)
	logger.Info("\t")
}

func initialServices(logger service.Logger, discovery service.Discovery) map[string]service.Instances {
	services := make(map[string]service.Instances, discovery.ServiceCount())
	for _, serviceName := range discovery.ServiceNames() {
		if instances, err := discovery.FetchServices(serviceName); err != nil {
			logger.Error("Unable to fetch initial [%s] services: %v", serviceName, err)
			services[serviceName] = make(service.Instances, 0)
		} else {
			services[serviceName] = instances
		}
	}

	return services
}

func monitorServices(logger service.Logger, discovery service.Discovery, services map[string]service.Instances) {
	discovery.AddListenerForAll(
		service.ListenerFunc(func(serviceName string, newInstances service.Instances) {
			if oldInstances, ok := services[serviceName]; ok {
				printServices(logger, serviceName, oldInstances, newInstances)
				services[serviceName] = newInstances
			} else {
				logger.Error("Unknown service: [%s]", serviceName)
			}
		}),
	)
}

func main() {
	cmd.ConfigureCommandLine()
	flag.Parse()
	discoveryBuilder := &service.DiscoveryBuilder{}
	cmd.ApplyCommandLine(discoveryBuilder)

	logger := &service.DefaultLogger{os.Stdout}
	discovery, err := discoveryBuilder.NewDiscovery(logger, true)
	if err != nil {
		logger.Error("Unable to start discovery client: %v", err)
		os.Exit(1)
	}

	services := initialServices(logger, discovery)
	for serviceName, instances := range services {
		printServices(logger, serviceName, instances, instances)
	}

	monitorServices(logger, discovery, services)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	<-signals
	discovery.Close()
}
