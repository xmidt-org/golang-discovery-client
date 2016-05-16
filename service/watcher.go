package service

import (
	"errors"
	"fmt"
	"github.com/foursquare/curator.go"
	"github.com/foursquare/fsgo/net/discovery"
	"github.com/samuel/go-zookeeper/zk"
	"sync"
)

// serviceWatcher holds meta data about one particular service that's being
// observed for changes.  This type also implements a simple API for interacting
// with Zookeeper.
type serviceWatcher struct {
	curatorConnection  discovery.Conn
	instanceSerializer discovery.InstanceSerializer
	servicePath        string
	serviceName        string

	listenerMutex sync.Mutex
	listeners     []Listener
}

// addListener appends a listener to this watcher
func (this *serviceWatcher) addListener(listener Listener) {
	this.listenerMutex.Lock()
	defer this.listenerMutex.Unlock()
	this.listeners = append(this.listeners, listener)
}

// removeListener removes a listener to this watcher
func (this *serviceWatcher) removeListener(listener Listener) bool {
	this.listenerMutex.Lock()
	defer this.listenerMutex.Unlock()
	for index, candidate := range this.listeners {
		if candidate == listener {
			this.listeners = append(this.listeners[:index], this.listeners[index+1:]...)
			return true
		}
	}

	return false
}

// dispatch broadcasts the given service Instances to all listeners associated
// with this watcher
func (this *serviceWatcher) dispatch(instances Instances) {
	this.listenerMutex.Lock()
	defer this.listenerMutex.Unlock()
	for _, listener := range this.listeners {
		listener.ServicesChanged(this.serviceName, instances)
	}
}

// fetchServices obtains the ServiceInstance objects from the given slice
// of child nodes.  This method is tolerant of zookeeper and parsing errors,
// since during network flapping it's possible that the slice of child ids
// is no longer valid.  This will be reflected in a partially filled or empty
// Instances result.
func (this *serviceWatcher) fetchServices(childIds []string) Instances {
	instances := make(Instances, 0, len(childIds))

	for _, childId := range childIds {
		instancePath := this.servicePath + "/" + childId
		data, err := this.curatorConnection.GetData().ForPath(instancePath)
		if err != nil {
			// ignore errors when obtaining the child data, as its possible for the
			// current set of children to have changed before this method was called
			continue
		}

		serviceInstance, err := this.instanceSerializer.Deserialize(data)
		if err != nil {
			// ignore deserialization errors, as it's possible when doing upgrades
			// for multiple versions of the discovery client to run simultaneously
			continue
		}

		serviceInstance.Id = childId
		instances = append(instances, serviceInstance)
	}

	return instances
}

// readServices obtains the current child nodes, then invokes readServices
func (this *serviceWatcher) readServices() (Instances, error) {
	childIds, err := this.curatorConnection.GetChildren().ForPath(this.servicePath)
	if err != nil {
		return nil, errors.New(
			fmt.Sprintf("Error while fetching children for path %s: %v", this.servicePath, err),
		)
	}

	return this.fetchServices(childIds), nil
}

// readServicesAndWatch is like readServices, except that it also sets a watch
// on the watched service path
func (this *serviceWatcher) readServicesAndWatch() (Instances, error) {
	childIds, err := this.curatorConnection.GetChildren().Watched().ForPath(this.servicePath)
	if err != nil {
		return nil, errors.New(
			fmt.Sprintf("Error while getting children with watch for path %s: %v", this.servicePath, err),
		)
	}

	return this.fetchServices(childIds), nil
}

// initialize sets up this watcher with a curator connection and ensures that any necessary
// znode paths exist
func (this *serviceWatcher) initialize(curatorConnection discovery.Conn) error {
	this.curatorConnection = curatorConnection
	err := curator.NewEnsurePath(this.servicePath).Ensure(this.curatorConnection.ZookeeperClient())
	if err != nil && err != zk.ErrNodeExists {
		return errors.New(
			fmt.Sprintf("Error during initialization while ensuring path %s: %v", this.servicePath, err),
		)
	}

	_, err = this.curatorConnection.GetChildren().Watched().ForPath(this.servicePath)
	if err != nil {
		return errors.New(
			fmt.Sprintf("Error during initialization setting watch on path %s: %v", this.servicePath, err),
		)
	}

	return nil
}

// serviceWatcherSet is an internal collection type that maps serviceWatches by name and path
type serviceWatcherSet struct {
	serviceNames []string
	byName       map[string]*serviceWatcher
	byPath       map[string]*serviceWatcher
}

// newServiceWatcherSet is an internal Factory Method that creates one serviceWatcher
// for each service name, then returns a serviceWatcherSet with the services mapped.
func newServiceWatcherSet(serviceNames []string, basePath string) *serviceWatcherSet {
	watcherCount := len(serviceNames)
	byName := make(map[string]*serviceWatcher, watcherCount)
	byPath := make(map[string]*serviceWatcher, watcherCount)
	instanceSerializer := &discovery.JsonInstanceSerializer{}

	for _, serviceName := range serviceNames {
		// ignore duplicate service names
		if _, ok := byName[serviceName]; ok {
			continue
		}

		servicePath := basePath + "/" + serviceName
		serviceWatcher := &serviceWatcher{
			instanceSerializer: instanceSerializer,
			servicePath:        servicePath,
			serviceName:        serviceName,
		}

		byName[serviceWatcher.serviceName] = serviceWatcher
		byPath[serviceWatcher.servicePath] = serviceWatcher
	}

	serviceWatcherSet := &serviceWatcherSet{
		serviceNames: make([]string, len(byName)),
		byName:       byName,
		byPath:       byPath,
	}

	// copying the keys ensures that the service names have been deduped
	index := 0
	for serviceName := range serviceWatcherSet.byName {
		serviceWatcherSet.serviceNames[index] = serviceName
		index++
	}

	return serviceWatcherSet
}

func (this *serviceWatcherSet) serviceCount() int {
	return len(this.serviceNames)
}

func (this *serviceWatcherSet) cloneServiceNames() []string {
	serviceNames := make([]string, len(this.serviceNames))
	copy(serviceNames, this.serviceNames)
	return serviceNames
}

func (this *serviceWatcherSet) findByName(serviceName string) (*serviceWatcher, bool) {
	value, ok := this.byName[serviceName]
	return value, ok
}

func (this *serviceWatcherSet) findByPath(path string) (*serviceWatcher, bool) {
	value, ok := this.byPath[path]
	return value, ok
}

func (this *serviceWatcherSet) visit(visitor func(*serviceWatcher)) {
	for _, serviceWatcher := range this.byName {
		visitor(serviceWatcher)
	}
}

func (this *serviceWatcherSet) initialize(curatorConnection discovery.Conn) error {
	for _, serviceWatcher := range this.byName {
		err := serviceWatcher.initialize(curatorConnection)
		if err != nil {
			return err
		}
	}

	return nil
}
