/*
 * Copyright 2020 Balaji Jinnah and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package membership

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/pathivu/arivu/etcd"

	"github.com/stretchr/testify/require"

	"github.com/pathivu/arivu/config"
)

func bootArivuCluster(t *testing.T) (map[string]*etcd.EtcdServer, map[string]*Manager) {
	servers := make(map[string]*etcd.EtcdServer, 0)
	managers := make(map[string]*Manager, 0)

	dir, err := ioutil.TempDir("", "pathivu-test")
	require.NoError(t, err)
	servers["1"] = etcd.StartEtcd(&config.Config{
		PeerAddr:      "http://127.0.0.1:1234",
		KvClientAddr:  "http://127.0.0.1:8888",
		Name:          "node1",
		Dir:           dir,
		IntialCluster: "node1=http://127.0.0.1:1234,node2=http://127.0.0.1:2234,node3=http://127.0.0.1:3234",
	})
	m1, err := NewManager(&config.Config{
		PeerAddr:      "http://127.0.0.1:1234",
		KvClientAddr:  "http://127.0.0.1:8888",
		Name:          "node1",
		Dir:           dir,
		IntialCluster: "node1=http://127.0.0.1:1234,node2=http://127.0.0.1:2234,node3=http://127.0.0.1:3234",
	})
	require.NoError(t, err)

	managers["1"] = m1
	dir, err = ioutil.TempDir("", "pathivu-test")
	require.NoError(t, err)
	servers["2"] = etcd.StartEtcd(&config.Config{
		PeerAddr:      "http://127.0.0.1:2234",
		KvClientAddr:  "http://127.0.0.1:8889",
		Name:          "node2",
		Dir:           dir,
		IntialCluster: "node1=http://127.0.0.1:1234,node2=http://127.0.0.1:2234,node3=http://127.0.0.1:3234",
	})
	m2, err := NewManager(&config.Config{
		PeerAddr:      "http://127.0.0.1:2234",
		KvClientAddr:  "http://127.0.0.1:8889",
		Name:          "node2",
		Dir:           dir,
		IntialCluster: "node1=http://127.0.0.1:1234,node2=http://127.0.0.1:2234,node3=http://127.0.0.1:3234",
	})
	require.NoError(t, err)
	managers["2"] = m2
	dir, err = ioutil.TempDir("", "pathivu-test")
	require.NoError(t, err)
	servers["3"] = etcd.StartEtcd(&config.Config{
		PeerAddr:      "http://127.0.0.1:3234",
		KvClientAddr:  "http://127.0.0.1:9999",
		Name:          "node3",
		Dir:           dir,
		IntialCluster: "node1=http://127.0.0.1:1234,node2=http://127.0.0.1:2234,node3=http://127.0.0.1:3234",
	})
	m3, err := NewManager(&config.Config{
		PeerAddr:      "http://127.0.0.1:3234",
		KvClientAddr:  "http://127.0.0.1:9999",
		Name:          "node3",
		Dir:           dir,
		IntialCluster: "node1=http://127.0.0.1:1234,node2=http://127.0.0.1:2234,node3=http://127.0.0.1:3234",
	})
	require.NoError(t, err)
	managers["3"] = m3
	m1.StartManager()
	m2.StartManager()
	m3.StartManager()
	time.Sleep(1 * time.Second)
	return servers, managers
}

func TestMembership(t *testing.T) {
	servers, managers := bootArivuCluster(t)

	defer func() {
		for _, s := range servers {
			s.Stop()
		}
		for _, m := range managers {
			require.NoError(t, os.RemoveAll(m.cfg.Dir))
		}
	}()

	// Test for join node.
	node := &PathivuMember{
		ID:       "id-123",
		HostAddr: "127.0.0.1:3131",
		PushAddr: "127.0.0.1:1313",
		state:    alive,
	}
	err := managers["1"].JoinNode(node)
	require.NoError(t, err)

	time.Sleep(time.Second * 1)
	require.Equal(t, len(managers["1"].members), 1)

	// Check other node picked the config or not.
	require.Equal(t, len(managers["2"].members), 1)
	require.Equal(t, len(managers["3"].members), 1)

	// check whether the config is right or not.
	require.Equal(t, managers["2"].members[node.ID].HostAddr, node.HostAddr)

	// Test assign ingester
	dest, err := managers["1"].AssignPathivuIngester("source1")
	require.NoError(t, err)

	dest2, err := managers["2"].AssignPathivuIngester("soruce1")
	require.NoError(t, err)

	require.Equal(t, dest.ID, dest2.ID)
}

func TestRestartCluster(t *testing.T) {
	dir, err := ioutil.TempDir("", "pathivu-test")
	require.NoError(t, err)
	server := etcd.StartEtcd(&config.Config{
		PeerAddr:      "http://127.0.0.1:1234",
		KvClientAddr:  "http://127.0.0.1:8888",
		Dir:           dir,
		Name:          "node1",
		IntialCluster: "node1=http://127.0.0.1:1234",
	})
	defer server.Stop()
	m, err := NewManager(&config.Config{
		PeerAddr:     "http://127.0.0.1:1234",
		KvClientAddr: "http://127.0.0.1:8888",
		Dir:          dir,
	})
	m.StartManager()
	require.NoError(t, err)
	node := &PathivuMember{
		ID:       "id-123",
		HostAddr: "127.0.0.1:3131",
		PushAddr: "127.0.0.1:1313",
		state:    alive,
	}
	err = m.JoinNode(node)
	require.NoError(t, err)
	m.Stop()

	// Create another manager and check whether config are picked
	// in the initial start or not.
	m, err = NewManager(&config.Config{
		PeerAddr:     "http://127.0.0.1:1234",
		KvClientAddr: "http://127.0.0.1:8888",
		Name:         "node1",
		Dir:          dir,
	})
	m.StartManager()
	require.NoError(t, err)
	require.Equal(t, len(m.members), 1)
}
