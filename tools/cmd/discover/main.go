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
	// CategoryInitial is the category assigned to all instances when the discovery client is first queried
	CategoryInitial string = "INITIAL"

	// CategoryExisting is the category assigned to instances which didn't change from one watch event to the next
	CategoryExisting string = "EXISTING"

	// CategoryNew is the category assigned to brand new instances
	CategoryNew string = "NEW"

	// CategoryRemoved is the category assigned to instances which are no longer in the watched list
	CategoryRemoved string = "REMOVED"
)

func categorizeinstances(oldInstances, newInstances service.Instances) (categories map[string]service.Instances) {
	categories = make(map[string]service.Instances, 3)

	maxServiceCount := len(oldInstances)
	if maxServiceCount < len(newInstances) {
		maxServiceCount = len(newInstances)
	}

	categories[CategoryExisting] = make(service.Instances, 0, maxServiceCount)
	categories[CategoryNew] = make(service.Instances, 0, maxServiceCount)
	categories[CategoryRemoved] = make(service.Instances, 0, maxServiceCount)

	newInstancesByID := make(service.KeyMap, len(newInstances))
	newInstances.ToKeyMap(service.InstanceId, newInstancesByID)

	for _, oldInstance := range oldInstances {
		id := oldInstance.Id
		if existingInstance, ok := newInstancesByID[id]; ok {
			delete(newInstancesByID, id)
			categories[CategoryExisting] = append(categories[CategoryExisting], existingInstance)
		} else {
			categories[CategoryRemoved] = append(categories[CategoryRemoved], existingInstance)
		}
	}

	for _, newInstance := range newInstancesByID {
		categories[CategoryNew] = append(categories[CategoryNew], newInstance)
	}

	return
}

func printService(logger service.Logger, category string, instance *discovery.ServiceInstance) {
	logger.Info(
		"\t%-8.8s | %s | %s",
		category,
		instance.Id,
		service.HttpAddress(service),
	)
}

func printUpdates(logger service.Logger, serviceName string, categories map[string]service.Instances) {
	logger.Info("\t##### %s #####", serviceName)

	totalServiceCount := 0
	for category, instances := range categories {
		totalServiceCount += len(instances)
		for _, service := range instances {
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

func initialinstances(logger service.Logger, discovery service.Discovery) map[string]service.Instances {
	instances := make(map[string]service.Instances, discovery.ServiceCount())
	for _, serviceName := range discovery.ServiceNames() {
		if instances, err := discovery.Fetchinstances(serviceName); err != nil {
			logger.Error("Unable to fetch initial [%s] instances: %v", serviceName, err)
			instances[serviceName] = make(service.Instances, 0)
		} else {
			instances[serviceName] = instances
		}
	}

	return instances
}

func printInitial(logger service.Logger, initialinstances map[string]service.Instances) {
	for serviceName, instances := range initialinstances {
		logger.Info("\t##### %s #####", serviceName)

		for _, service := range instances {
			printService(logger, CategoryInitial, service)
		}

		logger.Info("\tInitial count of [%s] instances: %d", serviceName, len(instances))
	}
}

func monitorinstances(logger service.Logger, discovery service.Discovery, serviceMap map[string]service.Instances) {
	discovery.AddListenerForAll(
		service.ListenerFunc(func(serviceName string, newInstances service.Instances) {
			if oldInstances, ok := serviceMap[serviceName]; ok {
				printUpdates(logger, serviceName, categorizeinstances(oldInstances, newInstances))
				instances[serviceName] = newInstances
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

	serviceMap := initialinstances(logger, discovery)
	printInitial(logger, serviceMap)
	monitorinstances(logger, discovery, serviceMap)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	<-signals
	discovery.Close()
}
