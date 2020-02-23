package main

import (
	"fmt"
	"log"

	"github.com/pathivu/arivu/etcd"
	"github.com/pathivu/arivu/membership"
	"github.com/pathivu/arivu/server"

	"github.com/pathivu/arivu/config"
	"github.com/spf13/cobra"
)

const (
	defaultPeerAddr     = "localhost:2252"
	defaultKvClientAddr = "localhost:3252"
	defaultPeerPort     = 2252
	defaultKvClientPort = 3252
	defaultServerPort   = 4252
	defaultPortOffset   = 0
)

func main() {
	arivuCmd := &cobra.Command{
		Use: "arivu",
		Short: `arivu is the brain of pathivu cluster. It's core responsibility is to manage pathivu
		cluster`,
		Run: func(cmd *cobra.Command, args []string) {
			cfg := &config.Config{}
			var err error
			if cfg.Name, err = cmd.Flags().GetString("name"); err != nil {
				log.Fatal(err)
			}
			if cfg.PortOffset, err = cmd.Flags().GetInt("port-offset"); err != nil {
				log.Fatal(err)
			}

			if cfg.PeerAddr, err = cmd.Flags().GetString("peer-addr"); err != nil {
				log.Fatal(err)
			}

			if cfg.KvClientAddr, err = cmd.Flags().GetString("client-kv-addr"); err != nil {
				log.Fatal(err)
			}

			if cfg.PeerAddr == "" {
				cfg.PeerAddr = fmt.Sprintf("http://localhost:%d", defaultPeerPort+cfg.PortOffset)
			}
			if cfg.KvClientAddr == "" {
				cfg.KvClientAddr = fmt.Sprintf("http://localhost:%d", defaultKvClientPort+cfg.PortOffset)
			}
			if cfg.IntialCluster, err = cmd.Flags().GetString("cluster-peers"); err != nil {
				log.Fatal(err)
			}
			eServer := etcd.StartEtcd(cfg)
			manager, err := membership.NewManager(cfg)
			if err != nil {
				log.Fatal(err)
			}

			go server.StartServer(cfg, manager)

			if err = <-eServer.ListenForErr(); err != nil {
				log.Fatal(err)
			}
		},
	}
	arivuCmd.Flags().String("peer-addr", "", `peer address of the arivu server.
	 peer address is used for internal communication between arivu servers`)
	arivuCmd.Flags().String("client-kv-addr", "", `client kv address is used for internal key value
	 storage`)
	arivuCmd.Flags().Int("port-offset", 0, `port-offset will 
	increment the default ports will the given offset. If no address mentioned`)
	arivuCmd.Flags().String("cluster-peers", "", `peer address of the arivu cluster`)
	arivuCmd.Flags().String("name", "", "name of the node")
	arivuCmd.Flags().Int("arivu-port", defaultServerPort, "arivu server port")
	arivuCmd.Execute()
}
