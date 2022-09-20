package main

import (
	changeevents "audit_trail/changeEvents"
	"audit_trail/config"
	"audit_trail/db"
	"audit_trail/dispatch"
	"audit_trail/models"
	"audit_trail/pb/pb"
	"audit_trail/server"
	"audit_trail/watcher"
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/joho/godotenv"
	"google.golang.org/grpc"
)

func main() {
	godotenv.Load(".env")
	configdao, err := db.InitializeDataAccessMongoDB(os.Getenv("DB_URL"))

	if err != nil {
		log.Printf("ERROR, failed to initialize data access to the config database due to error: %v\n", err)
		os.Exit(1)
	}

	co := config.GetConfig{Dao: configdao}
	configs, err := co.GetConfiguration()
	if err != nil {
		log.Printf("ERROR, failed to get configurations due to error: %v\n", err)
		os.Exit(1)
	}
	auditDao, err := db.InitializeDataAccessMongoDB(os.Getenv("DB_URL"))
	if err != nil {
		log.Printf("ERROR, failed to initialize data access to the Audit database due to error: %v\n", err)
		os.Exit(1)
	}
	server := &server.AuditTrailServer{Dao: auditDao}
	var appDao *db.DataAccess
	var resToken models.ResumeToken
	mux := runtime.NewServeMux()
	err = pb.RegisterAuditTrailHandlerServer(context.Background(), mux, server)
	if err != nil {
		log.Fatalf("cannot register server: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterAuditTrailServer(grpcServer, server)

	if err != nil {
		log.Fatalf("cannot start server: %v", err)
	}
	listener, err := net.Listen("tcp", "0.0.0.0:8001")
	listener2, err := net.Listen("tcp", "0.0.0.0:8002")
	go grpcServer.Serve(listener)
	go http.Serve(listener2, mux)
	log.Println("Application started on port 8001 for grpc and 8002 for rest")
	for _, c := range configs {
		go func(conf config.Configuration) {
			if conf.AppDBType == "postgres" {
				appDao, err = db.InitializeDataAccessPostgres(conf.AppDBUrl)
				if err != nil {
					log.Printf("ERROR, failed to initialize data access to the App database due to error: %v\n", err)
					os.Exit(1)
				}
				r := changeevents.PostgresChangeLogTracker{Config: conf, Dao: auditDao}
				resToken, err := r.GetResumeToken()
				if err != nil {
					log.Printf("ERROR, failed to look up resume token with error: %v\n", err)
					os.Exit(1)
				}
				w := watcher.PostgresChangeStreamWatcher{Config: conf, Dao: appDao}
				err = w.WatchChangeEvents(resToken, dispatch.GetSaveChangeEventFuncPostgres(conf, auditDao))
				if err != nil {
					log.Printf("ERROR, failed to watch change stream due to error: %v\n", err)
					os.Exit(1)
				}
			} else {
				appDao, err = db.InitializeDataAccessMongoDB(conf.AppDBUrl)
				if err != nil {
					log.Printf("ERROR, failed to initialize data access to the App database due to error: %v\n", err)
					os.Exit(1)
				}
				r := changeevents.MongoDBChangeLogTracker{Config: conf, Dao: auditDao}
				resToken, err = r.GetResumeToken()
				if err != nil {
					log.Printf("ERROR, failed to look up resume token with error: %v\n", err)
					os.Exit(1)
				}
				w := watcher.MongoDBChangeStreamWatcher{Config: conf, Dao: appDao}
				err = w.WatchChangeEvents(resToken, dispatch.GetSaveChangeEventFuncMongoDB(conf, auditDao))
				if err != nil {
					log.Printf("ERROR, failed to watch change stream due to error: %v\n", err)
					os.Exit(1)
				}
			}
		}(c)
	}
	for {
		time.Sleep(10000 * time.Second)
	}
}
