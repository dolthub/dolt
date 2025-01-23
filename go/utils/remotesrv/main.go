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
	"os"
	"os/signal"

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotesrv"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/datas"
)

var result []byte

func main() {
	readOnlyParam := flag.Bool("read-only", false, "run a read-only server which does not allow writes")
	repoModeParam := flag.Bool("repo-mode", false, "act as a remote for an existing dolt directory, instead of stand alone")
	dirParam := flag.String("dir", "", "root directory that this command will run in; default cwd")
	grpcPortParam := flag.Int("grpc-port", -1, "the port the grpc server will listen on; default 50051")
	httpPortParam := flag.Int("http-port", -1, "the port the http server will listen on; default 80; if http-port is equal to grpc-port, both services will serve over the same port")
	httpHostParam := flag.String("http-host", "", "hostname to use in the host component of the URLs that the server generates; default ''; if '', server will echo the :authority header")
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
		*httpHostParam = ":80"
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

	var dbCache remotesrv.DBCache
	if *repoModeParam {
		ctx := context.Background()
		dEnv := env.Load(ctx, env.GetCurrentUserHomeDir, fs, doltdb.LocalDirDoltDB, "remotesrv")
		if !dEnv.Valid() {
			log.Fatalln("repo-mode failed to load repository")
		}
		db := doltdb.HackDatasDatabaseFromDoltDB(dEnv.DoltDB(ctx))
		cs := datas.ChunkStoreFromDatabase(db)
		dbCache = SingletonCSCache{cs.(remotesrv.RemoteSrvStore)}
	} else {
		dbCache = NewLocalCSCache(fs)
	}

	server, err := remotesrv.NewServer(remotesrv.ServerArgs{
		HttpHost:           *httpHostParam,
		HttpListenAddr:     fmt.Sprintf(":%d", *httpPortParam),
		GrpcListenAddr:     fmt.Sprintf(":%d", *grpcPortParam),
		FS:                 fs,
		DBCache:            dbCache,
		ReadOnly:           *readOnlyParam,
		ConcurrencyControl: remotesapi.PushConcurrencyControl_PUSH_CONCURRENCY_CONTROL_IGNORE_WORKING_SET,
	})
	if err != nil {
		log.Fatalf("error creating remotesrv Server: %v\n", err)
	}
	listeners, err := server.Listeners()
	if err != nil {
		log.Fatalf("error starting remotesrv Server listeners: %v\n", err)
	}
	go func() {
		server.Serve(listeners)
	}()
	waitForSignal()
	server.GracefulStop()
}

func waitForSignal() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)
	<-c
}
