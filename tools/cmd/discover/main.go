package main

import (
	"flag"
	"os"
	"os/signal"

	"github.com/Comcast/golang-discovery-client/service"
	"github.com/Comcast/golang-discovery-client/service/cmd"
	"github.com/foursquare/fsgo/net/discovery"
)

type Category string
type Categories map[Category]service.Instances
type ServiceMap map[string]service.Instances

const (
	// CategoryInitial is the category assigned to all instances when the discovery client is first queried
	CategoryInitial Category = "INITIAL"

	// CategoryExisting is the category assigned to instances which didn't change from one watch event to the next
	CategoryExisting Category = "EXISTING"

	// CategoryNew is the category assigned to brand new instances
	CategoryNew Category = "NEW"

	// CategoryRemoved is the category assigned to instances which are no longer in the watched list
	CategoryRemoved Category = "REMOVED"
)

func categorizeinstances(oldInstances, newInstances service.Instances) Categories {
	categories := make(Categories)
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
		service.HttpAddress(instance),
	)
}

func printUpdates(logger service.Logger, serviceName string, categories Categories) {
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

func initialinstances(logger service.Logger, discovery service.Discovery) ServiceMap {
	serviceMap := make(ServiceMap, discovery.ServiceCount())
	for _, serviceName := range discovery.ServiceNames() {
		if instances, err := discovery.FetchServices(serviceName); err != nil {
			logger.Error("Unable to fetch initial [%s] instances: %v", serviceName, err)
			serviceMap[serviceName] = make(service.Instances)
		} else {
			serviceMap[serviceName] = instances
		}
	}

	return
}

func printInitial(logger service.Logger, initial ServiceMap) {
	for serviceName, instances := range initial {
		logger.Info("\t##### %s #####", serviceName)

		for _, service := range instances {
			printService(logger, CategoryInitial, service)
		}

		logger.Info("\tInitial count of [%s] instances: %d", serviceName, len(instances))
	}
}

func monitorinstances(logger service.Logger, discovery service.Discovery, serviceMap ServiceMap) {
	discovery.AddListenerForAll(
		service.ListenerFunc(func(serviceName string, newInstances service.Instances) {
			if oldInstances, ok := serviceMap[serviceName]; ok {
				printUpdates(logger, serviceName, categorizeinstances(oldInstances, newInstances))
				serviceMap[serviceName] = newInstances
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
