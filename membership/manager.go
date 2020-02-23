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
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pathivu/arivu/config"
	"github.com/pathivu/arivu/utils"
	"github.com/prometheus/common/log"
	"go.etcd.io/etcd/clientv3"
)

const (
	alive = iota
	dead
)

func init() {
	rand.Seed(time.Now().Unix())
}

// PathivuMember is member of Pathivu cluster.
type PathivuMember struct {
	ID       string `json:"id"`
	HostAddr string `json:"host_addr"`
	PushAddr string `json:"push_addr"`
	State    int    `json:"state"`
}

// Manager is responsible for scheduling the sources to the destination pathivu.
type Manager struct {
	sync.Mutex
	sources    map[string]*PathivuMember
	members    map[string]*PathivuMember
	membersIDs []string
	kvClient   *clientv3.Client
	cfg        *config.Config
	stopped    uint32
}

// NewManager returns manager instance.
func NewManager(cfg *config.Config) (*Manager, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{cfg.KvClientAddr},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	return &Manager{
		sources:    make(map[string]*PathivuMember),
		members:    make(map[string]*PathivuMember),
		membersIDs: []string{},
		kvClient:   cli,
		cfg:        cfg,
	}, nil
}

// StartManager will start the membership manager.
func (m *Manager) StartManager() {
	// manager should start in following order.
	// first start all the watchers and range scan all
	// the config.
	// starting watcher.
	go func() {
		for {
			if atomic.LoadUint32(&m.stopped) == 1 {
				break
			}
			m.watchPathivuConfigChange()
		}
	}()

	// Scan all the config
	for {
		if err := m.initializeState(); err != nil {
			log.Errorf("error while initializing state %+v", err)
			continue
		}
		break
	}
}

func (m *Manager) watchPathivuConfigChange() {
	log.Info("starting watcher")

	watcherCh := m.kvClient.Watch(context.TODO(), utils.PathivuConfigPrefix, clientv3.WithPrefix())
	for res := range watcherCh {
		if res.Canceled {
			log.Error("etcd watcher cancled")
			return
		}
		m.Lock()
		for _, event := range res.Events {
			pathivuID := utils.UUIDromConfigKey(string(event.Kv.Key))
			_, ok := m.members[pathivuID]
			if !ok {
				// Update the member ID if it is a new node.
				m.membersIDs = append(m.membersIDs, pathivuID)
			}
			// It is safe to update the member directly since it is going through raft.
			// TODO: we have to update the clients that config has been changed.
			// that has to be done in another go routine.
			member := &PathivuMember{}
			err := json.Unmarshal(event.Kv.Value, member)
			if err != nil {
				log.Errorf("unable to unmarshal pathivu member %+v", err)
			}
			m.members[pathivuID] = member
		}
		m.Unlock()
	}
}

func (m *Manager) initializeState() error {
	res, err := m.kvClient.Get(
		context.TODO(), utils.PathivuConfigPrefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	m.Lock()
	defer m.Unlock()
	for _, kv := range res.Kvs {
		// we're getting from the state.
		pathivuID := utils.UUIDromConfigKey(string(kv.Key))
		_, ok := m.members[pathivuID]
		if ok {
			continue
			// updated by the watcher so no need to do anything.
		}
		m.membersIDs = append(m.membersIDs, pathivuID)
		member := &PathivuMember{}
		err := json.Unmarshal(kv.Value, member)
		if err != nil {
			return err
		}
		m.members[pathivuID] = member
	}
	return nil
}

// JoinNode will update the state.
func (m *Manager) JoinNode(in *PathivuMember) error {
	m.Lock()
	defer m.Unlock()
	// When pathivu restarts there may be a config change, if there is a config change
	// then you have to go thorugh raft.
	node, ok := m.members[in.ID]
	if !ok {
		// This is new node joining in the cluster for the first time. So, save the state
		// in the etcd, other arivu will picks up by listening on the change.
		buf, err := json.Marshal(in)
		if err != nil {
			return err
		}
		_, err = m.CompareAndSwap(utils.PathivuConfigKey(in.ID), "", string(buf))
		if err != nil {
			return err
		}
		return nil
	}
	if in.HostAddr != node.HostAddr || in.PushAddr != node.PushAddr {
		configKey := utils.PathivuConfigKey(in.ID)
		// Config has been changes so update the config.
		res, err := m.kvClient.Get(context.TODO(), configKey)
		if err != nil {
			return err
		}

		buf, err := json.Marshal(&in)
		if err != nil {
			return err
		}

		currentConfig := string(buf)
		previousConfig := string(res.Kvs[0].Value)

		txnRes, err := m.kvClient.Txn(context.TODO()).
			If(clientv3.Compare(clientv3.Value(configKey), "=", previousConfig)).
			Then(clientv3.OpPut(configKey, currentConfig)).
			Commit()

		if err != nil {
			return err
		}
		if !txnRes.Succeeded {
			return errors.New("node join config change failed")
		}
	}
	// same config so chill no need to worry about anything for now.
	return nil
}

// AssignPathivuIngester will assign ingester for the given source.
func (m *Manager) AssignPathivuIngester(source string) (*PathivuMember, error) {
	m.Lock()
	defer m.Unlock()
	member, ok := m.sources[source]
	if ok {
		return member, nil
	}
	// Now server may be initailize, we may not have in the cache so, check in the storage
	// if not then assingn an ingester.
	destinationKey := utils.DestinationKey(source)
	response, err := m.kvClient.Get(context.Background(), destinationKey)
	if err != nil {
		return nil, err
	}
	var destinationID string
	for _, kv := range response.Kvs {
		if string(kv.Key) == destinationKey {
			destinationID = string(kv.Value)
		}
	}
	// Cases, there may be a case where the destination is dead.
	// TODO: need to handle death and shard the source across the destination.
	// So, aggregation will be fast.
	if destinationID != "" {
		return m.members[destinationID], nil
	}

	if len(m.membersIDs) == 0 {
		return nil, errors.New("no destination available to ingest")
	}

	// Assign a destaination for the given source.
	// TODO: use some heuristic formula to assign pathivu.
	destinationID = m.membersIDs[rand.Int()%len(m.membersIDs)]
	res, err := m.CompareAndSwap(destinationKey, "", destinationID)
	if err != nil {
		return nil, err
	}
	if !res.Succeeded {
		// There may be a race condition for this to happen. So, check the
		// local state as well.
		return nil, errors.New("assignement failed please try again")
	}
	return m.members[destinationID], nil
}

// CompareAndSwap will compare and swap on etcd kv
func (m *Manager) CompareAndSwap(key, old, new string) (*clientv3.TxnResponse, error) {
	var cmp clientv3.Cmp
	if old == "" {
		cmp = clientv3.Compare(clientv3.Version(key), "=", 0)
	} else {
		cmp = clientv3.Compare(clientv3.Value(key), "=", old)
	}
	return m.kvClient.Txn(context.TODO()).
		If(cmp).
		Then(clientv3.OpPut(key, new)).Commit()
}

// Stop will stop the watcher
func (m *Manager) Stop() {
	atomic.StoreUint32(&m.stopped, 1)
}
