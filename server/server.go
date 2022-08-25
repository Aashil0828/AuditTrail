package server

import (
	"audit_trail/db"
	"audit_trail/models"
	"audit_trail/pb/pb"
	"context"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type AuditTrailServer struct {
	Dao *db.DataAccess
	pb.UnimplementedAuditTrailServer
}

func (s *AuditTrailServer) FetchAuditTrail(ctx context.Context, req *pb.FetchAuditTrailRequest) (*pb.FetchAuditTrailResponse, error) {
	var coll *mongo.Collection
	var opts options.FindOptions
	var cur *mongo.Cursor
	var err error
	var auditrecords []*pb.AuditTrailRecord
	opts.Sort = map[string]int{"timestamp": 1}
	if req.DatabaseType == "postgres" {
		coll = s.Dao.MongoDbClient.Database("audit").Collection("audit_logs")
		cur, err = coll.Find(ctx, bson.D{{Key: "service", Value: req.Service}, {Key: "database", Value: req.Database}, {Key: "table", Value: req.Collection}, {Key: "rowid", Value: req.DocumentID}}, &opts)
		for cur.Next(ctx) {
			ce := models.ChangeEventPostgres{}
			err := cur.Decode(&ce)
			if err != nil {
				return nil, err
			}
			switch ce.OperationType {
			case "insert":
				auditrecords = append(auditrecords, &pb.AuditTrailRecord{FieldID: req.FieldPath, FieldValue: fmt.Sprintf("%v", db.TraverseForFieldValueMap(strings.Split(req.FieldPath, "."), ce.FullDocument)), UpdatedBy: ce.User, UpdatedAt: ce.Timestamp.String()})
			case "update":
				auditrecords = append(auditrecords, &pb.AuditTrailRecord{FieldID: req.FieldPath, FieldValue: fmt.Sprintf("%v", db.TraverseForFieldValueMap(strings.Split(req.FieldPath, "."), ce.UpdateDescription.UpdatedFields)), UpdatedBy: ce.User, UpdatedAt: ce.Timestamp.String()})
			}
		}
	} else if req.DatabaseType == "mongodb" {
		coll = s.Dao.MongoDbClient.Database("audit").Collection("mongo_audit_logs")
		cur, err = coll.Find(ctx, bson.D{{Key: "service", Value: req.Service}, {Key: "database", Value: req.Database}, {Key: "collection", Value: req.Collection}, {Key: "documentKey", Value: req.DocumentID}}, &opts)
		var previousRecord string
		for cur.Next(ctx) {
			ce := models.ChangeEventMongoDB{}
			err := cur.Decode(&ce)
			if err != nil {
				return &pb.FetchAuditTrailResponse{}, fmt.Errorf("cannot decode into change event: %v", err)
			}
			switch ce.OperationType {
			case "insert":
				auditrecords = append(auditrecords, &pb.AuditTrailRecord{FieldID: req.FieldPath, FieldValue: fmt.Sprintf("%v", db.TraverseForFieldValue(strings.Split(req.FieldPath, "."), ce.FullDocument)), UpdatedBy: ce.User, UpdatedAt: time.Unix(int64(ce.Timestamp.T), 0).String()})
			case "update":
				if previousRecord != fmt.Sprintf("%v", db.TraverseForFieldValueMap(strings.Split(req.FieldPath, "."), ce.UpdateDescription.UpdatedFields)) {
					if previousRecord == "" {
						previousRecord = fmt.Sprintf("%v", db.TraverseForFieldValueMap(strings.Split(req.FieldPath, "."), ce.UpdateDescription.UpdatedFields))
						break
					}
					auditrecords = append(auditrecords, &pb.AuditTrailRecord{FieldID: req.FieldPath, FieldValue: fmt.Sprintf("%v", db.TraverseForFieldValueMap(strings.Split(req.FieldPath, "."), ce.UpdateDescription.UpdatedFields)), UpdatedBy: ce.User, UpdatedAt: time.Unix(int64(ce.Timestamp.T), 0).String()})
					previousRecord = fmt.Sprintf("%v", db.TraverseForFieldValueMap(strings.Split(req.FieldPath, "."), ce.UpdateDescription.UpdatedFields))
				}
			}
		}
	} else {
		return &pb.FetchAuditTrailResponse{}, fmt.Errorf("enter correct database type value, accepted values are \"mongodb\" and \"postgres\"")
	}
	if err != nil {
		return &pb.FetchAuditTrailResponse{}, fmt.Errorf("cannot retreive documents from the database %v", err)
	}
	defer cur.Close(ctx)
	return &pb.FetchAuditTrailResponse{Audit: auditrecords}, nil
}
