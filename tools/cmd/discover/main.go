package main

import (
	"flag"
	"github.com/Comcast/golang-discovery-client/service"
	"github.com/Comcast/golang-discovery-client/service/cmd"
	"github.com/foursquare/fsgo/net/discovery"
	"os"
	"os/signal"
	"strconv"
)

func printService(logger service.Logger, status string, instance *discovery.ServiceInstance) {
	port := "?"
	portType := "?"
	if instance.Port != nil {
		port = strconv.Itoa(*instance.Port)
		portType = "http"
	} else if instance.SslPort != nil {
		port = strconv.Itoa(*instance.SslPort)
		portType = "https"
	}

	logger.Info(
		"\t%-8.8s | %-37.37s | %-5.5s | %-32.32s | %-5.5s",
		status,
		instance.Id,
		portType,
		instance.Address,
		port,
	)
}

func printServices(logger service.Logger, serviceName string, oldInstances, newInstances service.Instances) {
	logger.Info("\t##### %s #####", serviceName)

	oldInstancesById := make(map[string]*discovery.ServiceInstance, len(oldInstances))
	newInstancesById := make(map[string]*discovery.ServiceInstance, len(newInstances))

	for _, instance := range oldInstances {
		oldInstancesById[instance.Id] = instance
	}

	for _, instance := range newInstances {
		newInstancesById[instance.Id] = instance
	}

	for id, instance := range oldInstancesById {
		if _, ok := newInstancesById[id]; ok {
			// this is an existing instance
			delete(newInstancesById, id)
			printService(logger, "EXISTING", instance)
		} else {
			// this instance was removed
			printService(logger, "REMOVED", instance)
		}
	}

	for _, instance := range newInstancesById {
		// each of the remaining entries is a new instance
		printService(logger, "NEW", instance)
	}

	logger.Info(
		"\tSummary:  There were %d %s service(s).  There are now %d service(s).",
		len(oldInstances),
		serviceName,
		len(newInstances),
	)
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
