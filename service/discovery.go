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

	// AddListener registers a listener for the given service name.  If fetchInitial is true,
	// the supplied listener will receive the initial set of services.  The returned error
	// will indicate any problems with that initial fetch.
	AddListener(serviceName string, fetchInitial bool, listener Listener) error

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
	state         uint32
	connection    string
	basePath      string
	registrations Instances

	serviceWatcherSet *serviceWatcherSet
	curatorConnection discovery.Conn
	logger            zk.Logger
	serviceDiscovery  *discovery.ServiceDiscovery

	curatorEvents chan curator.CuratorEvent
	once          sync.Once
}

// EventReceived provides multiplexing for the various events that this discovery can receive
func (this *curatorDiscovery) EventReceived(client curator.CuratorFramework, event curator.CuratorEvent) error {
	this.curatorEvents <- event
	return nil
}

func (this *curatorDiscovery) running() bool {
	return atomic.LoadUint32(&this.state) == discoveryStateRunning
}

// handleReconnect() should be called only inside the main event loop when
// a zookeeper reconnection has been detected.
func (this *curatorDiscovery) handleReconnect() {
	this.logger.Printf("Recovering from zookeeper connection disruption")
	for _, serviceWatcher := range this.serviceWatcherSet.byName {
		instances, err := serviceWatcher.readServices()
		if err != nil {
			this.logger.Printf("Error while attempting to read [%s] service instances after connection disruption: %v", serviceWatcher.serviceName, err)
		} else {
			serviceWatcher.dispatch(instances)
		}
	}
}

// updateServices dispatches an update event for services on a given path, if and only
// if the path is recognized.
func (this *curatorDiscovery) updateServices(path string) {
	if serviceWatcher, ok := this.serviceWatcherSet.findByPath(path); ok {
		instances, err := serviceWatcher.readServicesAndWatch()
		if err != nil {
			this.logger.Printf("Error while updating services: %v", err)
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

func (this *curatorDiscovery) AddListener(serviceName string, fetchInitial bool, listener Listener) error {
	if serviceWatcher, ok := this.serviceWatcherSet.findByName(serviceName); ok {
		return serviceWatcher.addListener(fetchInitial, listener)
	}

	return nil
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
	this.once.Do(func() {
		this.logger.Printf("Discovery client starting")
		this.curatorConnection, err = discovery.DefaultConn(this.connection)
		if err != nil {
			return
		}

		defer func() {
			if err != nil {
				this.curatorConnection.Close()
			}
		}()
		
		if err = this.curatorConnection.BlockUntilConnected(); err != nil {
			return
		}

		if len(this.registrations) > 0 {
			this.logger.Printf("Maintaining registrations: %s", this.registrations)
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

		if this.serviceWatcherSet.serviceCount() > 0 {
			this.logger.Printf("Watching services: %v", this.serviceWatcherSet.serviceNames)
			err = this.serviceWatcherSet.initialize(this.curatorConnection)
			if err != nil {
				return
			}
		}

		this.curatorEvents = make(chan curator.CuratorEvent, 10)
		this.curatorConnection.CuratorListenable().AddListener(this)
		atomic.StoreUint32(&this.state, discoveryStateRunning)

		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()

			defer func() {
				this.logger.Printf("Discovery client shutting down")
				atomic.StoreUint32(&this.state, discoveryStateStopped)
				this.curatorConnection.CuratorListenable().RemoveListener(this)

				close(this.curatorEvents)
			}()

			for {
				select {
				case <-shutdown:
					if err := this.curatorConnection.Close(); err != nil {
						this.logger.Printf("Error while closing Curator: %v", err)
					}

					return

				case curatorEvent := <-this.curatorEvents:
					switch curatorEvent.Type() {
					case curator.CLOSING:
						this.logger.Printf("Curator closing.  Service Discovery shutting down.")
						return
					case curator.WATCHED:
						if watchedEvent := curatorEvent.WatchedEvent(); watchedEvent == nil {
							this.logger.Printf("Nil watched event from Curator")
						} else if watchedEvent.Type == zk.EventSession && watchedEvent.State == zk.StateHasSession {
							this.handleReconnect()
						} else if watchedEvent.Type == zk.EventNodeChildrenChanged && len(watchedEvent.Path) > 0 {
							this.updateServices(watchedEvent.Path)
						}
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
}

// NewDiscovery creates a distinct Discovery instance from this DiscoveryBuilder.  Changes
// to this builder will not affect the newly created Discovery instance, and vice versa.
func (this *DiscoveryBuilder) NewDiscovery(logger zk.Logger) Discovery {
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
