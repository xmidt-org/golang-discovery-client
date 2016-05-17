package service

import (
	"bytes"
	"encoding/json"
	"github.com/samuel/go-zookeeper/zk"
	"os"
	"strconv"
	"testing"
)

var (
	zout bool
	zerr bool
)

// ConnectionString returns the Curator connection string to the test cluster
func ConnectionString(testCluster *zk.TestCluster) string {
	var buffer bytes.Buffer
	for _, server := range testCluster.Servers {
		if buffer.Len() > 0 {
			buffer.WriteRune(',')
		}

		buffer.WriteString("127.0.0.1:")
		buffer.WriteString(strconv.Itoa(server.Port))
	}

	return buffer.String()
}

type testLogger struct {
	t *testing.T
}

func (l *testLogger) Printf(format string, parameters ...interface{}) {
	l.t.Logf(format, parameters...)
}

// ClusterTest represents a test which uses a zookeeper test cluster in isolation.
type ClusterTest struct {
	t           *testing.T
	testCluster *zk.TestCluster
}

// StartClusterTest builds and starts a zookeeper test cluster.  Stdout and stderr are redirected
// to the test logging, which means they are available when -v is specified.
func StartClusterTest(t *testing.T, nodeCount int) *ClusterTest {
	testCluster, err := zk.StartTestCluster(nodeCount, os.Stdout, os.Stderr)
	if err != nil {
		t.Fatalf("Could not start test cluster: %v", err)
	}

	return &ClusterTest{t, testCluster}
}

// StartAllServers starts each server node in the test cluster.
// When a ClusterTest is first created, all server nodes are started.
// Thus, this method is only necessary if StopAllServers() is called.
func (this *ClusterTest) StartAllServers() {
	for _, server := range this.testCluster.Servers {
		server.Srv.Start()
	}
}

// StopAllServers stops each server node.  The ClusterTest instance may
// still be used after invoking StartAllServers()
func (this *ClusterTest) StopAllServers() {
	for _, server := range this.testCluster.Servers {
		server.Srv.Stop()
	}
}

// Stop completely ends the ClusterTest.  No further testing with this instance
// is possible.
func (this *ClusterTest) Stop() {
	this.testCluster.Stop()
}

// NewDiscoveryBuilder creates a DiscoveryBuilder using the supplied configuration.  The connection string
// is set (or replaced) with the connection string for the test cluster.
func (this *ClusterTest) NewDiscoveryBuilder(configuration string) *DiscoveryBuilder {
	discoveryBuilder := &DiscoveryBuilder{}
	err := json.Unmarshal([]byte(configuration), discoveryBuilder)
	if err != nil {
		this.t.Fatalf("Unable to unmarshal discovery configuration %s: %v", configuration, err)
	}

	discoveryBuilder.Connection = ConnectionString(this.testCluster)
	return discoveryBuilder
}

// NewDiscovery creates a new Discovery instance using the supplied configuration.  See NewDiscoveryBuilder.
func (this *ClusterTest) NewDiscovery(t *testing.T, configuration string) Discovery {
	return this.NewDiscoveryBuilder(configuration).NewDiscovery(&testLogger{t})
}
