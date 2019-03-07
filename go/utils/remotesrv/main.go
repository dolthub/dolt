package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/liquidata-inc/ld/dolt/go/gen/proto/dolt/services/remotesapi_v1alpha1"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
	"google.golang.org/grpc"
	"log"
	"net"
	"net/http"
	"os"
	"sync"
)

var csCache = NewCSCache(filesys.LocalFS)

func main() {
	dir := flag.String("dir", "", "root directory that this command will run in.")
	httpHostParam := flag.String("http-host", "", "The host used for remote chunk upload and download requests")
	grpcPort := flag.Int("grpc-port", 50051, "root directory that this command will run in.")
	httpPort := flag.Int("http-port", 80, "root directory that this command will run in.")
	flag.Parse()

	if dir != nil && len(*dir) > 0 {
		err := os.Chdir(*dir)

		if err != nil {
			log.Fatalln("failed to chdir to:", *dir)
			log.Fatalln("error:", err.Error())
			os.Exit(1)
		}
	}

	httpHost := *httpHostParam
	if len(httpHost) == 0 {
		log.Println("http-host provided.  Using localhost.  This will only work on this machine.")
		httpHost = "localhost"
	}

	if *httpPort != 80 {
		httpHost = fmt.Sprintf("%s:%d", httpHost, *httpPort)
	}

	wg := sync.WaitGroup{}
	wg.Add(2)
	stopChan := make(chan interface{})
	go httpServer(*httpPort, &wg, stopChan)
	go grpcServer(httpHost, *grpcPort, &wg, stopChan)

	oneByte := [1]byte{}
	for {
		_, err := os.Stdin.Read(oneByte[:])

		if err != nil || oneByte[0] == '\n' {
			break
		}
	}

	close(stopChan)
	wg.Wait()
}

func grpcServer(httpHost string, grpcPort int, wg *sync.WaitGroup, stopChan chan interface{}) {
	defer func() {
		wg.Done()
		log.Println("exiting grpc Server go routine")
	}()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(128 * 1024 * 1024))
	remotesapi.RegisterChunkStoreServiceServer(grpcServer, RemoteChunkStore{httpHost})
	go func() {
		log.Println("Starting grpc server on port", grpcPort)
		err := grpcServer.Serve(lis)
		log.Println("grpc server exited. error:", err)
	}()

	<-stopChan
	grpcServer.GracefulStop()
}

func httpServer(httpPort int, wg *sync.WaitGroup, stopChan chan interface{}) {
	defer func() {
		wg.Done()
		log.Println("exiting http Server go routine")
	}()

	server := http.Server{
		Addr:    fmt.Sprintf(":%d", httpPort),
		Handler: http.HandlerFunc(ServeHTTP),
	}

	go func() {
		log.Println("Starting http server on port ", httpPort)
		err := server.ListenAndServe()
		log.Println("http server exited. exit error:", err)
	}()

	<-stopChan
	server.Shutdown(context.Background())
}
