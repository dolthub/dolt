// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"

	"google.golang.org/grpc"

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/datas"
)

var readOnlyParam *bool = flag.Bool("read-only", false, "run a read-only server which does not allow writes")

func main() {
	repoModeParam := flag.Bool("repo-mode", false, "act as a remote for a dolt directory, instead of stand alone")
	dirParam := flag.String("dir", "", "root directory that this command will run in.")
	grpcPortParam := flag.Int("grpc-port", -1, "root directory that this command will run in.")
	httpPortParam := flag.Int("http-port", -1, "root directory that this command will run in.")
	httpHostParam := flag.String("http-host", "localhost", "host url that this command will assume.")
	flag.Parse()

	if dirParam != nil && len(*dirParam) > 0 {
		err := os.Chdir(*dirParam)

		if err != nil {
			log.Fatalln("failed to chdir to:", *dirParam, "error:", err.Error())
		} else {
			log.Println("cwd set to " + *dirParam)
		}
	} else {
		log.Println("'dir' parameter not provided. Using the current working dir.")
	}

	if *httpPortParam != -1 {
		*httpHostParam = fmt.Sprintf("%s:%d", *httpHostParam, *httpPortParam)
	} else {
		*httpPortParam = 80
		log.Println("'http-port' parameter not provided. Using default port 80")
	}

	if *grpcPortParam == -1 {
		*grpcPortParam = 50051
		log.Println("'grpc-port' parameter not provided. Using default port 50051")
	}

	fs, err := filesys.LocalFilesysWithWorkingDir(".")
	if err != nil {
		log.Fatalln("could not get cwd path:", err.Error())
	}

	var dbCache DBCache
	if *repoModeParam {
		dEnv := env.Load(context.Background(), env.GetCurrentUserHomeDir, fs, doltdb.LocalDirDoltDB, "remotesrv")
		if !dEnv.Valid() {
			log.Fatalln("repo-mode failed to load repository")
		}
		db := doltdb.HackDatasDatabaseFromDoltDB(dEnv.DoltDB)
		cs := datas.ChunkStoreFromDatabase(db)
		dbCache = SingletonCSCache{cs.(store)}
	} else {
		dbCache = NewLocalCSCache(fs)
	}

	stopChan, wg := startServer(*httpHostParam, *httpPortParam, *grpcPortParam, fs, dbCache)
	waitForSignal()

	close(stopChan)
	wg.Wait()
}

func waitForSignal() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)
	<-c
}

func startServer(httpHost string, httpPort, grpcPort int, fs filesys.Filesys, dbCache DBCache) (chan interface{}, *sync.WaitGroup) {
	expectedFiles := newFileDetails()

	wg := sync.WaitGroup{}
	stopChan := make(chan interface{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		httpServer(dbCache, fs, expectedFiles, httpPort, stopChan)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		grpcServer(dbCache, fs, expectedFiles, httpHost, grpcPort, stopChan)
	}()

	return stopChan, &wg
}

func grpcServer(dbCache DBCache, fs filesys.Filesys, expectedFiles fileDetails, httpHost string, grpcPort int, stopChan chan interface{}) {
	defer func() {
		log.Println("exiting grpc Server go routine")
	}()

	var chnkSt remotesapi.ChunkStoreServiceServer
	chnkSt = NewHttpFSBackedChunkStore(httpHost, dbCache, expectedFiles, fs)
	if *readOnlyParam {
		chnkSt = ReadOnlyChunkStore{chnkSt}
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(128 * 1024 * 1024))
	go func() {
		remotesapi.RegisterChunkStoreServiceServer(grpcServer, chnkSt)

		log.Println("Starting grpc server on port", grpcPort)
		err := grpcServer.Serve(lis)
		log.Println("grpc server exited. error:", err)
	}()

	<-stopChan
	grpcServer.GracefulStop()
}

func httpServer(dbCache DBCache, fs filesys.Filesys, expectedFiles fileDetails, httpPort int, stopChan chan interface{}) {
	defer func() {
		log.Println("exiting http Server go routine")
	}()

	server := http.Server{
		Addr:    fmt.Sprintf(":%d", httpPort),
		Handler: newFileHandler(dbCache, expectedFiles, fs, *readOnlyParam),
	}

	go func() {
		log.Println("Starting http server on port ", httpPort)
		err := server.ListenAndServe()
		log.Println("http server exited. exit error:", err)
	}()

	<-stopChan
	server.Shutdown(context.Background())
}
