package service

import (
	"errors"
	"fmt"
	"github.com/foursquare/curator.go"
	"github.com/foursquare/fsgo/net/discovery"
	"github.com/samuel/go-zookeeper/zk"
	"io"
	"net/http"
	"time"
)

const (
	DefaultWatchCooldown = time.Duration(5 * time.Second)
)

// Discovery represents a service discovery endpoint.  Instances are
// created using a DiscoveryBuilder.
type Discovery interface {
	io.Closer

	// Connected indicates whether this discovery is actually connected to a zookeeper ensemble
	Connected() bool

	// ServiceCount returns the number of watched services in associated with this Discovery
	ServiceCount() int

	// ServiceNames returns an independent slice containing the names of the watched services
	// available in this Discovery
	ServiceNames() []string

	// FetchServices returns an Instances containing the set of services with the given name.
	// If no services by that name are watched, this method returns an error.
	FetchServices(serviceName string) (Instances, error)

	// AddListenerForAll adds the given listener for all watched services
	AddListenerForAll(listener Listener)

	// RemoveListenerFromAll removes the given listener from all watched services
	RemoveListenerFromAll(listener Listener)

	// AddListener registers a listener for the given service name
	AddListener(serviceName string, listener Listener)

	// RemoveListener deregisters a listener for the given service name
	RemoveListener(serviceName string, listener Listener)

	// BlockUntilConnected blocks until the underlying Curator implementation
	// is in a connected state with Zookeeper
	BlockUntilConnected() error

	// BlockUntilConnectedTimeout is like BlockUntilConnected, except that it will
	// abort with an error if the specified time elapses without the underlying
	// Curator implementation transitioning into a connected state.
	BlockUntilConnectedTimeout(maxWaitTime time.Duration) error
}

// curatorDiscovery is the default, Curator-based Service Discovery subsystem.
type curatorDiscovery struct {
	basePath          string
	curatorConnection discovery.Conn
	logger            Logger
	serviceDiscovery  *discovery.ServiceDiscovery
	serviceWatcherSet *serviceWatcherSet

	connectionStates chan curator.ConnectionState
	curatorEvents    chan curator.CuratorEvent
	operations       chan func()
}

// StateChanged monitors the Curator connection for changes, and updates this Discovery
// instance.  Client code can use Connected() to determine if the underlying connection
// to Zookeeper is actually connected.
func (this *curatorDiscovery) StateChanged(client curator.CuratorFramework, newState curator.ConnectionState) {
	this.connectionStates <- newState
}

// EventReceived provides multiplexing for the various events that this discovery can receive
func (this *curatorDiscovery) EventReceived(client curator.CuratorFramework, event curator.CuratorEvent) error {
	this.curatorEvents <- event
	return nil
}

// initialize creates the various data structures necessary for the curatorDiscovery
// to do its job.  Concurrency stuff like channels is handled in monitor().
func (this *curatorDiscovery) initialize(
	registrations Instances,
	watches []string) error {

	this.logger.Debug("initialize(registrations=%s, watches=%s)", registrations, watches)
	var err error

	if len(registrations) > 0 {
		this.serviceDiscovery = discovery.NewServiceDiscovery(this.curatorConnection, this.basePath)
		err = this.serviceDiscovery.MaintainRegistrations()
		if err != nil {
			return errors.New(
				fmt.Sprintf("Error while starting up service discovery: %v", err),
			)
		}

		err = registrations.RegisterWith(this.serviceDiscovery)
		if err != nil {
			return errors.New(
				fmt.Sprintf("Error while registering services: %v", err),
			)
		}
	}

	this.serviceWatcherSet, err = newServiceWatcherSet(
		watches,
		this.curatorConnection,
		this.basePath,
	)

	if err != nil {
		return errors.New(
			fmt.Sprintf("Error while starting service watchers: %v", err),
		)
	}

	return nil
}

// handleWatchEvent implements the logic executed in response to Curator WATCH events
func (this *curatorDiscovery) handleWatchEvent(event curator.CuratorEvent) {
	this.logger.Debug("handleWatchEvent(%v)", event)
	watchedEvent := event.WatchedEvent()
	if watchedEvent != nil && watchedEvent.State == zk.StateHasSession {
		this.logger.Warn("Recovering from zookeeper connection disruption")
		this.serviceWatcherSet.visit(func(serviceWatcher *serviceWatcher) {
			this.operations <- func() {
				instances, err := serviceWatcher.readServices()
				if err != nil {
					this.logger.Warn("Error while attempting to read %s services after connection disruption: %v", serviceWatcher.serviceName, err)
				} else {
					serviceWatcher.dispatch(instances)
				}
			}
		})
	} else if serviceWatcher, ok := this.serviceWatcherSet.findByPath(event.Path()); ok {
		this.operations <- func() {
			instances, err := serviceWatcher.readServicesAndWatch()
			if err != nil {
				this.logger.Warn("Error while reading services: %v\n", err)
			} else {
				serviceWatcher.dispatch(instances)
			}
		}
	}
}

// monitor manages channel lifecycle and concurrently observes and reacts
// to discovery events
func (this *curatorDiscovery) monitor() {
	this.logger.Debug("monitor()")
	this.connectionStates = make(chan curator.ConnectionState, 10)
	this.curatorEvents = make(chan curator.CuratorEvent, 10)
	this.operations = make(chan func(), 100)
	this.curatorConnection.ConnectionStateListenable().AddListener(this)
	this.curatorConnection.CuratorListenable().AddListener(this)

	go func() {
		// handle operations in a separate goroutine to allow
		// the select code below to concurrently dispatch operations
		for operation := range this.operations {
			operation()
		}
	}()

	go func() {
		defer func() {
			this.curatorConnection.ConnectionStateListenable().RemoveListener(this)
			this.curatorConnection.CuratorListenable().RemoveListener(this)
			close(this.connectionStates)
			close(this.curatorEvents)
			close(this.operations)
		}()

		for {
			select {
			case newState := <-this.connectionStates:
				this.logger.Debug("connection state: %v\n", newState)
			case event := <-this.curatorEvents:
				this.logger.Debug("curator event: type=%s, path=%s, error=%v, children=%v\n", event.Type(), event.Path(), event.Err(), event.Children())

				switch event.Type() {
				case curator.CLOSING:
					this.logger.Info("Curator closing.  Service Discovery shutting down.")
					return
				case curator.WATCHED:
					this.handleWatchEvent(event)
				}
			}
		}
	}()
}

func (this *curatorDiscovery) Connected() bool {
	this.logger.Debug("Connected()")
	return this.curatorConnection.ZookeeperClient().Connected()
}

func (this *curatorDiscovery) ServiceCount() int {
	return this.serviceWatcherSet.serviceCount()
}

func (this *curatorDiscovery) ServiceNames() []string {
	return this.serviceWatcherSet.cloneServiceNames()
}

func (this *curatorDiscovery) FetchServices(serviceName string) (Instances, error) {
	this.logger.Debug("FetchServices(%s)", serviceName)
	serviceWatcher, ok := this.serviceWatcherSet.findByName(serviceName)
	if ok {
		return serviceWatcher.readServices()
	}

	return nil, errors.New(fmt.Sprintf("No such service: %s", serviceName))
}

func (this *curatorDiscovery) AddListenerForAll(listener Listener) {
	this.logger.Debug("AddListenerForAll(%v)", listener)
	this.operations <- func() {
		this.serviceWatcherSet.visit(func(serviceWatcher *serviceWatcher) {
			serviceWatcher.addListener(listener)
		})
	}
}

func (this *curatorDiscovery) RemoveListenerFromAll(listener Listener) {
	this.logger.Debug("RemoveListenerFromAll(%v)", listener)
	this.operations <- func() {
		this.serviceWatcherSet.visit(func(serviceWatcher *serviceWatcher) {
			serviceWatcher.removeListener(listener)
		})
	}
}

func (this *curatorDiscovery) AddListener(serviceName string, listener Listener) {
	this.logger.Debug("AddListener(%s, %v)", serviceName, listener)
	serviceWatcher, ok := this.serviceWatcherSet.findByName(serviceName)
	if ok {
		this.operations <- func() {
			serviceWatcher.addListener(listener)
		}
	}
}

func (this *curatorDiscovery) RemoveListener(serviceName string, listener Listener) {
	this.logger.Debug("RemoveListener(%s, %v)", serviceName, listener)
	serviceWatcher, ok := this.serviceWatcherSet.findByName(serviceName)
	if ok {
		this.operations <- func() {
			serviceWatcher.removeListener(listener)
		}
	}
}

func (this *curatorDiscovery) Close() error {
	this.logger.Debug("Close()")
	return this.curatorConnection.Close()
}

func (this *curatorDiscovery) BlockUntilConnected() error {
	return this.curatorConnection.BlockUntilConnected()
}

func (this *curatorDiscovery) BlockUntilConnectedTimeout(maxWaitTime time.Duration) error {
	return this.curatorConnection.BlockUntilConnectedTimeout(maxWaitTime)
}

// RequireConnected returns a function which can test whether an HTTP request
// should proceed.  Requests are allowed only if the underlying Zookeeper connection
// is active.
func RequireConnected(discovery Discovery) func(*http.Request) (error, bool) {
	return func(request *http.Request) (error, bool) {
		return nil, discovery.Connected()
	}
}

// DiscoveryBuilder provides a configurable DiscoveryFactory implementation.  This type
// also implements a standard JSON configuration.
type DiscoveryBuilder struct {
	// Connection is the Curator connection string.  It's a comma-delimited
	// list of zookeeper server nodes
	Connection string `json:"connection"`

	// BasePath is the parent znode path for all registrations and watches
	// for Discovery instances produced by this builder
	BasePath string `json:"basePath"`

	// Registrations holds any service instances that are maintained in zookeeper
	// under the BasePath.
	Registrations Instances `json:"registrations"`

	// Watches contains the names of services, registered under the BasePath,
	// to listen for changes
	Watches []string `json:"watches"`

	// WatchCooldown is the interval between noticing a service change and the
	// time services are actually reread from zookeeper
	WatchCooldown *time.Duration `json:"watchCooldown"`
}

// NewDiscovery creates a distinct Discovery instance from this DiscoveryBuilder.  Changes
// to this builder will not affect the newly created Discovery instance, and vice versa.
// The returned Discovery object, along with the underlying Curator infrastructure, will have
// been started.  Errors returned by this method most commonly refer to errors during
// startup.
func (this *DiscoveryBuilder) NewDiscovery(logger Logger, blockUntilConnected bool) (product Discovery, err error) {
	logger.Debug("NewDiscovery()")
	curatorConnection, err := discovery.DefaultConn(this.Connection)
	if err != nil {
		logger.Error("Unable to start curator using connection %s: %v", this.Connection, err)
		return
	}

	defer func() {
		if err != nil {
			logger.Error("Shutting down curator due to error: %v", err)
			curatorConnection.Close()
		}
	}()

	if blockUntilConnected {
		err = curatorConnection.BlockUntilConnected()
		if err != nil {
			return
		}
	}

	implementation := &curatorDiscovery{
		basePath:          this.BasePath,
		curatorConnection: curatorConnection,
		logger:            logger,
	}

	err = implementation.initialize(this.Registrations, this.Watches)
	if err != nil {
		return
	}

	implementation.monitor()
	product = implementation
	return
}
