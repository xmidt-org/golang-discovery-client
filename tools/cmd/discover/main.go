package main

import (
	"flag"
	"fmt"
	"github.com/Comcast/golang-discovery-client/service"
	"github.com/foursquare/fsgo/net/discovery"
	"os"
	"os/signal"
	"strings"
	"sync"
)

type Category string
type Categories map[Category]service.Instances
type ServiceMap map[string]service.Instances

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
			categories[CategoryRemoved] = append(categories[CategoryRemoved], oldInstance)
		}
	}

	for _, newInstance := range newInstancesByID {
		categories[CategoryNew] = append(categories[CategoryNew], newInstance)
	}

	return categories
}

func printService(logger service.Logger, category Category, instance *discovery.ServiceInstance) {
	escapeStart := ""
	escapeStop := ""

	switch category {
	case CategoryNew:
		// green, bold
		escapeStart = "\033[32;1m"
		escapeStop = "\033[0m"

	case CategoryRemoved:
		// red
		escapeStart = "\033[31m"
		escapeStop = "\033[0m"
	}

	logger.Info(
		"\t%s%-8.8s | %s | %s%s",
		escapeStart,
		category,
		instance.Id,
		service.HttpAddress(instance),
		escapeStop,
	)
}

func printUpdates(logger service.Logger, serviceName string, categories Categories) {
	logger.Info("\t##### %s #####", serviceName)

	totalServiceCount := 0
	for category, instances := range categories {
		totalServiceCount += len(instances)
		for _, instance := range instances {
			printService(logger, category, instance)
		}
	}

	logger.Info(
		"\t##### [%d] total, [%d] existing (unchanged), [%d] new, [%d] removed #####",
		totalServiceCount,
		len(categories[CategoryExisting]),
		len(categories[CategoryNew]),
		len(categories[CategoryRemoved]),
	)
}

func initialInstances(logger service.Logger, discovery service.Discovery) ServiceMap {
	serviceMap := make(ServiceMap, discovery.ServiceCount())
	for _, serviceName := range discovery.ServiceNames() {
		if instances, err := discovery.FetchServices(serviceName); err != nil {
			logger.Error("Unable to fetch initial [%s] instances: %v", serviceName, err)
			serviceMap[serviceName] = make(service.Instances, 0)
		} else {
			serviceMap[serviceName] = instances
		}
	}

	return serviceMap
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

func monitorInstances(logger service.Logger, discovery service.Discovery, serviceMap ServiceMap) {
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

func main() {
	discoveryBuilder := newDiscoveryBuilder()
	logger := &service.DefaultLogger{os.Stdout}
	discovery := discoveryBuilder.NewDiscovery(logger)

	waitGroup := &sync.WaitGroup{}
	shutdown := make(chan struct{})

	err := discovery.Run(waitGroup, shutdown)
	if err != nil {
		logger.Error("Unable to start discovery client: %v", err)
		os.Exit(1)
	}

	serviceMap := initialInstances(logger, discovery)
	printInitial(logger, serviceMap)
	monitorInstances(logger, discovery, serviceMap)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	<-signals
	close(shutdown)
	waitGroup.Wait()
}
