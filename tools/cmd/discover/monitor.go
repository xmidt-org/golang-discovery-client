package main

import (
	"github.com/Comcast/golang-discovery-client/service"
	"github.com/foursquare/fsgo/net/discovery"
	"github.com/samuel/go-zookeeper/zk"
	"sync"
)

type Category string
type Categories map[Category]service.Instances

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

type MonitorEvent struct {
	serviceName string
	instances   service.Instances
}

type Monitor struct {
	logger   zk.Logger
	services map[string]service.Instances
	inbound  chan MonitorEvent
}

func NewMonitor(logger zk.Logger) *Monitor {
	return &Monitor{
		logger: logger,
	}
}

func (m *Monitor) update(serviceName string, newInstances service.Instances) Categories {
	categories := make(Categories)
	if oldInstances, ok := m.services[serviceName]; ok {
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
	} else {
		categories[CategoryInitial] = newInstances
	}

	m.services[serviceName] = newInstances
	return categories
}

func (m *Monitor) printService(category Category, instance *discovery.ServiceInstance) {
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

	m.logger.Printf(
		"\t%s%-8.8s | %s | %s%s",
		escapeStart,
		category,
		instance.Id,
		service.HttpAddress(instance),
		escapeStop,
	)
}

func (m *Monitor) printServices(serviceName string, categories Categories) {
	m.logger.Printf("\t##### %s #####", serviceName)

	totalCount := 0
	for category, instances := range categories {
		for _, instance := range instances {
			m.printService(category, instance)
			totalCount++
		}
	}

	m.logger.Printf("\tCount of [%s] instances: %d", serviceName, totalCount)
}

func (m *Monitor) Run(waitGroup *sync.WaitGroup, shutdown <-chan struct{}) error {
	m.services = make(map[string]service.Instances)
	m.inbound = make(chan MonitorEvent, 10)
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		defer close(m.inbound)

		for {
			select {
			case <-shutdown:
				return
			case event := <-m.inbound:
				categories := m.update(event.serviceName, event.instances)
				m.printServices(event.serviceName, categories)
			}
		}
	}()

	return nil
}

func (m *Monitor) ServicesChanged(serviceName string, instances service.Instances) {
	m.inbound <- MonitorEvent{serviceName, instances}
}
