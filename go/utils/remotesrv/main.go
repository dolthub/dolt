package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/liquidata-inc/ld/dolt/go/gen/proto/dolt/services/remotesapi_v1alpha1"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
	"google.golang.org/grpc"
	"log"
	"net"
	"net/http"
	"os"
	"sync"
)

func initAWSResources(bucket, dynamoTable string) *DBCache {
	awsConfig := aws.NewConfig().WithRegion("us-east-1")
	awsConfig = awsConfig.WithCredentials(credentials.NewEnvCredentials())
	sess := session.Must(session.NewSession(awsConfig))
	s3Api := s3.New(sess)
	dynamoApi := dynamodb.New(sess)

	return NewAWSCSCache(bucket, dynamoTable, s3Api, dynamoApi)
}

func main() {
	dirParam := flag.String("dir", "", "root directory that this command will run in.")
	grpcPortParam := flag.Int("grpc-port", 50051, "root directory that this command will run in.")
	storageParam := flag.String("file-storage", S3Storage, "Backing storage.  Valid options are 'http-file-server' or 's3' (default)")
	httpHostParam := flag.String("http-host", "", "The host used for remote chunk upload and download requests")
	httpPortParam := flag.Int("http-port", 80, "root directory that this command will run in.")
	httpFlag := flag.Bool("http", false, "Run the http server")
	grpcFlag := flag.Bool("grpc", false, "Run the grpc server")
	bucketParam := flag.String("bucket", "dulthub-dev-chunks", "The bucket where chunk files are stored")
	dynamoTableParam := flag.String("dynamo-table", "dolthub-manifests-dev", "The bucket where chunk files are stored")
	flag.Parse()

	if dirParam != nil && len(*dirParam) > 0 {
		err := os.Chdir(*dirParam)

		if err != nil {
			log.Fatalln("failed to chdir to:", *dirParam)
			log.Fatalln("error:", err.Error())
			os.Exit(1)
		} else {
			log.Println("cwd set to " + *dirParam)
		}
	}

	if !*httpFlag && !*grpcFlag {
		log.Fatalln("Need to provide one or both of the flags: 'http', 'grpc'")
		return
	}

	httpHost := *httpHostParam
	if len(httpHost) == 0 {
		log.Println("http-host provided.  Using localhost.  This will only work on this machine.")
		httpHost = "localhost"
	}

	if *httpPortParam != 80 {
		httpHost = fmt.Sprintf("%s:%d", httpHost, *httpPortParam)
	}

	stLoc := StoragLocFromString(*storageParam)

	if stLoc == InvalidStorageLoc {
		log.Fatalln("Invalid storage option for 'file-storage'. Valid options are 'http-file-server' or 's3'. Received", *storageParam)
	}

	stopChan, wg := startServer(stLoc, *bucketParam, *dynamoTableParam, httpHost, *httpFlag, *grpcFlag, *httpPortParam, *grpcPortParam)

	close(stopChan)
	wg.Wait()
}

func startServer(storageLoc StorageLocation, bucket, dynamoTable, httpHost string, serveHttp, serveGrpc bool, httpPort, grpcPort int) (chan interface{}, *sync.WaitGroup) {
	wg := sync.WaitGroup{}
	stopChan := make(chan interface{})

	if serveHttp {
		wg.Add(1)
		go func() {
			defer wg.Done()
			httpServer(httpPort, stopChan)
		}()
	}

	if serveGrpc {
		wg.Add(1)
		go func() {
			defer wg.Done()
			grpcServer(storageLoc, bucket, dynamoTable, httpHost, grpcPort, stopChan)
		}()
	}

	oneByte := [1]byte{}
	for {
		_, err := os.Stdin.Read(oneByte[:])

		if err != nil || oneByte[0] == '\n' {
			break
		}
	}

	return stopChan, &wg
}

func grpcServer(stLoc StorageLocation, bucket, dynamoTable, httpHost string, grpcPort int, stopChan chan interface{}) {
	defer func() {
		log.Println("exiting grpc Server go routine")
	}()

	var chnkSt *RemoteChunkStore
	if stLoc == S3Storage {
		dbCache := initAWSResources(bucket, dynamoTable)
		chnkSt = NewAwsBackedChunkStore(dbCache)
	} else {
		dbCache := NewLocalCSCache(filesys.LocalFS)
		chnkSt = NewHttpFSBackedChunkStore(httpHost, dbCache)
	}

	fmt.Println("Opening grpc socket on port", grpcPort)
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(128 * 1024 * 1024))
	go func() {
		remotesapi.RegisterChunkStoreServiceServer(grpcServer, chnkSt)

		log.Println("Starting grpc server")
		err := grpcServer.Serve(lis)
		log.Println("grpc server exited. error:", err)
	}()

	<-stopChan
	grpcServer.GracefulStop()
}

func httpServer(httpPort int, stopChan chan interface{}) {
	defer func() {
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
