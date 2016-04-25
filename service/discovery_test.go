package service

import (
	"fmt"
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

func TestWatchAndAdvertise(t *testing.T) {
	clusterTest := StartClusterTest(t, 1)
	defer clusterTest.Stop()

	watchDiscovery := clusterTest.NewDiscovery(fmt.Sprintf(`{"basePath": "%s", "watches": ["%s"]}`, testBasePath, testServiceName))
	defer watchDiscovery.Close()

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

	advertiseDiscovery := clusterTest.NewDiscovery(
		fmt.Sprintf(
			`{"basePath": "%s", "registrations": [{"name": "%s", "address": "%s", "port": %d}]}`,
			testBasePath,
			testServiceName,
			testAddress,
			testPort,
		),
	)
	defer advertiseDiscovery.Close()

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

	watchDiscovery := clusterTest.NewDiscovery(fmt.Sprintf(`{"basePath": "%s", "watches": ["%s"]}`, testBasePath, testServiceName))
	defer watchDiscovery.Close()

	updates := make(chan Instances, 1)
	watchDiscovery.AddListener(
		testServiceName,
		ListenerFunc(func(serviceName string, instances Instances) {
			updates <- instances
		}),
	)

	clusterTest.StopAllServers()
	time.Sleep(2 * time.Second)
	clusterTest.StartAllServers()

	advertiseDiscovery := clusterTest.NewDiscovery(
		fmt.Sprintf(
			`{"basePath": "%s", "registrations": [{"name": "%s", "address": "%s", "port": %d}]}`,
			testBasePath,
			testServiceName,
			testAddress,
			testPort,
		),
	)
	defer advertiseDiscovery.Close()

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
