package main

import (
	"flag"
	"os"
	"os/signal"

	"github.com/Comcast/golang-discovery-client/service"
	"github.com/Comcast/golang-discovery-client/service/cmd"
	"github.com/foursquare/fsgo/net/discovery"
)

const (
	// CategoryInitial is the category assigned to all services when the discovery client is first queried
	CategoryInitial string = "INITIAL"

	// CategoryExisting is the category assigned to services which didn't change from one watch event to the next
	CategoryExisting string = "EXISTING"

	// CategoryNew is the category assigned to brand new services
	CategoryNew string = "NEW"

	// CategoryRemoved is the category assigned to services which are no longer in the watched list
	CategoryRemoved string = "REMOVED"
)

func categorizeServices(oldServices, newServices service.Instances) (categories map[string]service.Instances) {
	categories = make(map[string]service.Instances, 3)

	maxServiceCount := len(oldServices)
	if maxServiceCount < len(newServices) {
		maxServiceCount = len(newServices)
	}

	categories[CategoryExisting] = make(service.Instances, 0, maxServiceCount)
	categories[CategoryNew] = make(service.Instances, 0, maxServiceCount)
	categories[CategoryRemoved] = make(service.Instances, 0, maxServiceCount)

	newServicesByID := make(service.KeyMap, len(newServices))
	newServices.ToKeyMap(service.InstanceId, newServicesByID)

	for _, oldService := range oldServices {
		id := oldService.Id
		if existingService, ok := newServicesByID[id]; ok {
			delete(newServicesByID, id)
			categories[CategoryExisting] = append(categories[CategoryExisting], existingService)
		} else {
			categories[CategoryRemoved] = append(categories[CategoryRemoved], existingService)
		}
	}

	for _, newService := range newServicesByID {
		categories[CategoryNew] = append(categories[CategoryNew], newService)
	}

	return
}

func printService(logger service.Logger, category string, service *discovery.ServiceInstance) {
	logger.Info(
		"\t%-8.8s | %s | %s",
		category,
		service.Id,
		service.HttpAddress(service),
	)
}

func printUpdates(logger service.Logger, serviceName string, categories map[string]service.Instances) {
	logger.Info("\t##### %s #####", serviceName)

	totalServiceCount := 0
	for category, services := range categories {
		totalServiceCount += len(services)
		for _, service := range services {
			printService(logger, category, service)
		}
	}

	logger.Info(
		"[%d] total, [%d] existing (unchanged), [%d] new, [%d] removed",
		totalServiceCount,
		len(categories[CategoryExisting]),
		len(categories[CategoryNew]),
		len(categories[CategoryRemoved]),
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

func printInitial(logger service.Logger, initialServices map[string]service.Instances) {
	for serviceName, services := range initialServices {
		logger.Info("\t##### %s #####", serviceName)

		for _, service := range services {
			printService(logger, CategoryInitial, service)
		}

		logger.Info("\tInitial count of [%s] services: %d", serviceName, len(services))
	}
}

func monitorServices(logger service.Logger, discovery service.Discovery, serviceMap map[string]service.Instances) {
	discovery.AddListenerForAll(
		service.ListenerFunc(func(serviceName string, newServices service.Instances) {
			if oldServices, ok := serviceMap[serviceName]; ok {
				printUpdates(logger, serviceName, categorizeServices(oldServices, newServices))
				services[serviceName] = newServices
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

	serviceMap := initialServices(logger, discovery)
	printInitial(logger, serviceMap)
	monitorServices(logger, discovery, serviceMap)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	<-signals
	discovery.Close()
}
