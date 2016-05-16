package service

import (
	"errors"
	"fmt"
	"github.com/foursquare/curator.go"
	"github.com/foursquare/fsgo/net/discovery"
	"github.com/samuel/go-zookeeper/zk"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DefaultWatchCooldown = time.Duration(5 * time.Second)
)

const (
	discoveryStateNotStarted = uint32(iota)
	discoveryStateRunning
	discoveryStateStopped
)

var (
	ErrorNotRunning error = errors.New("Discovery client not running")
)

// Discovery represents a service discovery endpoint.  Instances are
// created using a DiscoveryBuilder.
type Discovery interface {
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

	// Run starts this Discovery instance.  It is idempotent.
	Run(waitGroup *sync.WaitGroup, shutdown <-chan struct{}) error
}

// curatorDiscovery is the default, Curator-based Service Discovery subsystem.
type curatorDiscovery struct {
	state             uint32
	connection        string
	basePath          string
	registrations     Instances
	serviceWatcherSet *serviceWatcherSet
	curatorConnection discovery.Conn
	logger            Logger
	serviceDiscovery  *discovery.ServiceDiscovery

	connectionStates chan curator.ConnectionState
	curatorEvents    chan curator.CuratorEvent
	operations       chan func()
	once             sync.Once
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

func (this *curatorDiscovery) running() bool {
	return atomic.LoadUint32(&this.state) == discoveryStateRunning
}

// handleWatchEvent implements the logic executed in response to Curator WATCH events.
// This method MUST be invoked on the goroutine that processes operations.
func (this *curatorDiscovery) handleWatchEvent(event curator.CuratorEvent) {
	this.logger.Debug("handleWatchEvent(%v)", event)
	watchedEvent := event.WatchedEvent()
	if watchedEvent != nil && watchedEvent.State == zk.StateHasSession {
		this.logger.Warn("Recovering from zookeeper connection disruption")
		this.serviceWatcherSet.visit(func(serviceWatcher *serviceWatcher) {
			instances, err := serviceWatcher.readServices()
			if err != nil {
				this.logger.Warn("Error while attempting to read %s services after connection disruption: %v", serviceWatcher.serviceName, err)
			} else {
				serviceWatcher.dispatch(instances)
			}
		})
	} else if serviceWatcher, ok := this.serviceWatcherSet.findByPath(event.Path()); ok {
		instances, err := serviceWatcher.readServicesAndWatch()
		if err != nil {
			this.logger.Warn("Error while reading services: %v\n", err)
		} else {
			serviceWatcher.dispatch(instances)
		}
	}
}

func (this *curatorDiscovery) Connected() bool {
	if this.running() {
		return this.curatorConnection.ZookeeperClient().Connected()
	}

	return false
}

func (this *curatorDiscovery) ServiceCount() int {
	return this.serviceWatcherSet.serviceCount()
}

func (this *curatorDiscovery) ServiceNames() []string {
	return this.serviceWatcherSet.cloneServiceNames()
}

func (this *curatorDiscovery) FetchServices(serviceName string) (Instances, error) {
	if this.running() {
		if serviceWatcher, ok := this.serviceWatcherSet.findByName(serviceName); ok {
			return serviceWatcher.readServices()
		}
	} else {
		return nil, ErrorNotRunning
	}

	return nil, errors.New(fmt.Sprintf("No such service: %s", serviceName))
}

func (this *curatorDiscovery) AddListenerForAll(listener Listener) {
	this.serviceWatcherSet.visit(func(serviceWatcher *serviceWatcher) {
		serviceWatcher.addListener(listener)
	})
}

func (this *curatorDiscovery) RemoveListenerFromAll(listener Listener) {
	this.serviceWatcherSet.visit(func(serviceWatcher *serviceWatcher) {
		serviceWatcher.removeListener(listener)
	})
}

func (this *curatorDiscovery) AddListener(serviceName string, listener Listener) {
	if serviceWatcher, ok := this.serviceWatcherSet.findByName(serviceName); ok {
		serviceWatcher.addListener(listener)
	}
}

func (this *curatorDiscovery) RemoveListener(serviceName string, listener Listener) {
	if serviceWatcher, ok := this.serviceWatcherSet.findByName(serviceName); ok {
		serviceWatcher.removeListener(listener)
	}
}

func (this *curatorDiscovery) BlockUntilConnected() error {
	if this.running() {
		return this.curatorConnection.BlockUntilConnected()
	}

	return ErrorNotRunning
}

func (this *curatorDiscovery) BlockUntilConnectedTimeout(maxWaitTime time.Duration) error {
	if this.running() {
		return this.curatorConnection.BlockUntilConnectedTimeout(maxWaitTime)
	}

	return ErrorNotRunning
}

func (this *curatorDiscovery) Run(waitGroup *sync.WaitGroup, shutdown <-chan struct{}) (err error) {
	this.logger.Debug("Run()")
	this.once.Do(func() {
		this.logger.Info("Discovery client starting")
		this.curatorConnection, err = discovery.DefaultConn(this.connection)
		if err != nil {
			return
		}

		defer func() {
			if err != nil {
				this.curatorConnection.Close()
			}
		}()

		if len(this.registrations) > 0 {
			this.logger.Info("Maintaining registrations: %s", this.registrations)
			this.serviceDiscovery = discovery.NewServiceDiscovery(this.curatorConnection, this.basePath)
			err = this.serviceDiscovery.MaintainRegistrations()
			if err != nil {
				return
			}

			err = this.registrations.RegisterWith(this.serviceDiscovery)
			if err != nil {
				return
			}
		}

		this.logger.Info("Watching services: %s", this.serviceWatcherSet.serviceNames)
		this.serviceWatcherSet.initialize(this.curatorConnection)

		this.connectionStates = make(chan curator.ConnectionState, 10)
		this.curatorEvents = make(chan curator.CuratorEvent, 10)
		this.operations = make(chan func(), 100)
		this.curatorConnection.ConnectionStateListenable().AddListener(this)
		this.curatorConnection.CuratorListenable().AddListener(this)

		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			defer close(this.connectionStates)
			defer close(this.curatorEvents)
			defer close(this.operations)
			defer func() {
				this.logger.Info("Discovery client shutting down")
				atomic.StoreUint32(&this.state, discoveryStateStopped)
				this.curatorConnection.ConnectionStateListenable().RemoveListener(this)
				this.curatorConnection.CuratorListenable().RemoveListener(this)
			}()

			atomic.StoreUint32(&this.state, discoveryStateRunning)

			for {
				select {
				case <-shutdown:
					if err := this.curatorConnection.Close(); err != nil {
						this.logger.Warn("Error while closing Curator: %v", err)
					}

					return
				case newState := <-this.connectionStates:
					this.logger.Debug("connection state: %v\n", newState)
				case operation := <-this.operations:
					operation()
				case event := <-this.curatorEvents:
					this.logger.Debug("curator event: type=%s, path=%s, error=%v, children=%v\n", event.Type(), event.Path(), event.Err(), event.Children())

					switch event.Type() {
					case curator.CLOSING:
						// no need to close the curator here, this should be an edge case
						// since clients will normally close(shutdown) to shut this discovery instance down
						this.logger.Info("Curator closing.  Service Discovery shutting down.")
						return
					case curator.WATCHED:
						this.handleWatchEvent(event)
					}
				}
			}
		}()
	})

	return
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
func (this *DiscoveryBuilder) NewDiscovery(logger Logger) Discovery {
	logger.Debug("NewDiscovery()")

	registrations := make(Instances, len(this.Registrations))
	for index := 0; index < len(registrations); index++ {
		clone := *this.Registrations[index]
		registrations[index] = &clone
	}

	watches := make([]string, len(this.Watches))
	copy(watches, this.Watches)

	return &curatorDiscovery{
		connection:        this.Connection,
		basePath:          this.BasePath,
		registrations:     registrations,
		serviceWatcherSet: newServiceWatcherSet(this.Watches, this.BasePath),
		logger:            logger,
	}
}
