package service

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

const (
	testBasePath    = "/test/region/flavor"
	testServiceName = "myService"
	testAddress     = "fabric-cd.webpa.comcast.net"
	testPort        = 8080
	testHttpAddress = "http://fabric-cd.webpa.comcast.net:8080"
	testDeviceId    = "mac:112233445566"
)

func _TestWatchAndAdvertise(t *testing.T) {
	clusterTest := StartClusterTest(t, 1)
	defer clusterTest.Stop()
	waitGroup := &sync.WaitGroup{}
	shutdown := make(chan struct{})
	defer close(shutdown)

	t.Log("Starting watch discovery")
	watchDiscovery := clusterTest.NewDiscovery(fmt.Sprintf(`{"basePath": "%s", "watches": ["%s"]}`, testBasePath, testServiceName))
	if err := watchDiscovery.Run(waitGroup, shutdown); err != nil {
		t.Fatalf("Unable to start watch Discovery: %v", err)
	}

	if err := watchDiscovery.BlockUntilConnected(); err != nil {
		t.Fatalf("Watch Discovery unable to connect: %v", err)
	}

	if count := watchDiscovery.ServiceCount(); count != 1 {
		t.Errorf("ServiceCount() should have returned 1, instead returned %d", count)
	}

	if serviceNames := watchDiscovery.ServiceNames(); len(serviceNames) != 1 {
		t.Errorf("ServiceNames() should have returned one service, instead returned %v", serviceNames)
	} else if serviceNames[0] != testServiceName {
		t.Errorf("ServiceNames() should return the slice of service names")
	}

	if _, err := watchDiscovery.FetchServices("nosuch"); err == nil {
		t.Errorf("FetchServices() should return an error for a nonexistent service")
	}

	initialInstances, err := watchDiscovery.FetchServices(testServiceName)
	if initialInstances == nil || err != nil {
		t.Fatalf("Unable to fetch initial services: %v", err)
	}

	if initialInstances.Len() > 0 {
		t.Errorf("The set of initial services should be empty")
	}

	updates := make(chan Instances, 1)
	watchDiscovery.AddListener(
		testServiceName,
		ListenerFunc(func(serviceName string, instances Instances) {
			t.Logf("New %s services: %v", serviceName, instances)
			updates <- instances
		}),
	)

	t.Log("Starting advertise discovery")
	advertiseDiscovery := clusterTest.NewDiscovery(
		fmt.Sprintf(
			`{"basePath": "%s", "registrations": [{"name": "%s", "address": "%s", "port": %d}]}`,
			testBasePath,
			testServiceName,
			testAddress,
			testPort,
		),
	)

	if err := advertiseDiscovery.Run(waitGroup, shutdown); err != nil {
		t.Fatalf("Unable to start advertise Discovery: %v", err)
	}

	if err := advertiseDiscovery.BlockUntilConnected(); err != nil {
		t.Fatalf("Advertise Discovery unable to connect: %v", err)
	}

	time.AfterFunc(3*DefaultWatchCooldown, func() {
		t.Log("Timeout has elapsed")
		close(updates)
	})

	t.Log("Waiting for advertised service")
	update, ok := <-updates
	if !ok {
		t.Fatalf("No new services arrived within the timeout")
	}

	if update.Len() != 1 {
		t.Fatalf("Unexpected count of new services: %d", update.Len())
	}

	updatedInstance := update[0]
	if updatedInstance == nil {
		t.Fatalf("Updated instances contained a nil element")
	}

	if updatedInstance.Name != testServiceName ||
		updatedInstance.Address != testAddress ||
		updatedInstance.Port == nil ||
		*updatedInstance.Port != testPort {
		t.Fatalf("Unexpected service instance from discovery listener")
	}
}

func TestTolerateDisconnection(t *testing.T) {
	clusterTest := StartClusterTest(t, 1)
	defer clusterTest.Stop()
	waitGroup := &sync.WaitGroup{}
	shutdown := make(chan struct{})
	defer close(shutdown)

	t.Log("Starting watch discovery")
	watchDiscovery := clusterTest.NewDiscovery(fmt.Sprintf(`{"basePath": "%s", "watches": ["%s"]}`, testBasePath, testServiceName))
	if err := watchDiscovery.Run(waitGroup, shutdown); err != nil {
		t.Fatalf("Unable to start watch Discovery: %v", err)
	}

	if err := watchDiscovery.BlockUntilConnected(); err != nil {
		t.Fatalf("Watch Discovery unable to connect: %v", err)
	}

	updates := make(chan Instances, 1)
	watchDiscovery.AddListener(
		testServiceName,
		ListenerFunc(func(serviceName string, instances Instances) {
			t.Logf("Watch discovery notified of [%s] services: %#v", serviceName, instances)
			updates <- instances
		}),
	)

	clusterTest.StopAllServers()
	time.Sleep(2 * time.Second)
	clusterTest.StartAllServers()

	if err := watchDiscovery.BlockUntilConnected(); err != nil {
		t.Fatalf("Watch Discovery unable to reconnect: %v", err)
	}

	t.Log("Starting advertise discovery")
	advertiseDiscovery := clusterTest.NewDiscovery(
		fmt.Sprintf(
			`{"basePath": "%s", "registrations": [{"name": "%s", "address": "%s", "port": %d}]}`,
			testBasePath,
			testServiceName,
			testAddress,
			testPort,
		),
	)

	if err := advertiseDiscovery.Run(waitGroup, shutdown); err != nil {
		t.Fatalf("Unable to start advertise Discovery: %v", err)
	}

	if err := advertiseDiscovery.BlockUntilConnected(); err != nil {
		t.Fatalf("Advertise Discovery unable to connect: %v", err)
	}

	var update Instances
	timer := time.NewTimer(time.Second * 15)
	defer timer.Stop()

	for len(update) != 1 {
		select {
		case update = <-updates:
		case <-timer.C:
			t.Fatal("Did not receive updated services within the timeout")
		}
	}

	updatedInstance := update[0]
	if updatedInstance == nil {
		t.Fatalf("Updated instances contained a nil element")
	}

	if updatedInstance.Name != testServiceName ||
		updatedInstance.Address != testAddress ||
		updatedInstance.Port == nil ||
		*updatedInstance.Port != testPort {
		t.Fatalf("Unexpected service instance from discovery listener")
	}
}
