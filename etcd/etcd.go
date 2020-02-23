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
package etcd

import (
	"log"
	"net/url"

	"github.com/pathivu/arivu/config"
	"go.etcd.io/etcd/embed"
)

// EtcdServer is responsible for maintaining local etcd
type EtcdServer struct {
	server *embed.Etcd
}

// StartEtcd will start the etcd server in the background.
func StartEtcd(config *config.Config) *EtcdServer {
	cfg := embed.NewConfig()
	cfg.Dir = config.Dir
	cfg.Name = config.Name
	purl, err := url.Parse(config.PeerAddr)
	if err != nil {
		log.Fatal(err)
	}
	cfg.APUrls = []url.URL{*purl}
	cfg.LPUrls = []url.URL{*purl}
	curl, err := url.Parse(config.KvClientAddr)
	if err != nil {
		log.Fatal(err)
	}
	cfg.LCUrls = []url.URL{*curl}
	cfg.ACUrls = []url.URL{*curl}
	cfg.InitialCluster = config.IntialCluster
	e, err := embed.StartEtcd(cfg)
	if err != nil {
		log.Fatal(err)

	}
	return &EtcdServer{
		server: e,
	}
}

func (s *EtcdServer) ListenForErr() <-chan error {
	return s.server.Err()
}

// Stop will stop the etcd server.
func (s *EtcdServer) Stop() {
	s.server.Server.Stop()
}
