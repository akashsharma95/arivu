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
package server

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/pathivu/arivu/config"
	"github.com/pathivu/arivu/membership"
	"github.com/pathivu/arivu/pb"
	"google.golang.org/grpc"
)

// ArivuServer is
type ArivuServer struct {
	manager *membership.Manager
}

// AssignIngester will assign destination for the source.
func (a *ArivuServer) AssignIngester(ctx context.Context, req *pb.AssignIngesterRequest) (*pb.AssignIngesterResponse, error) {
	destinations := []*pb.Destination{}

	// Assign ingester for every source.
	for _, source := range req.Sources {
		dest, err := a.manager.AssignPathivuIngester(source)
		if err != nil {
			return nil, err
		}
		destinations = append(destinations, &pb.Destination{
			Source:      source,
			Destination: dest.PushAddr,
		})
	}
	return &pb.AssignIngesterResponse{
		Destinations: destinations,
	}, nil
}

// StartServer will start arivu server
func StartServer(cfg *config.Config, manager *membership.Manager) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.ServerPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterArivuServer(grpcServer, &ArivuServer{
		manager: manager,
	})
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
