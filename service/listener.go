package service

// Listener receives notifications when the set of watched services has changed.
type Listener interface {
	// ServicesChanged is invoked anytime a Watcher notices that the set of services
	// with a given name has changed.
	ServicesChanged(serviceName string, instances Instances)
}

// ListenerFunc is the function type that corresponds to Listener
type ListenerFunc func(string, Instances)

var _ Listener = (ListenerFunc)(nil)

func (f ListenerFunc) ServicesChanged(serviceName string, instances Instances) {
	f(serviceName, instances)
}
