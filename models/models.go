package models

import (
	"time"

	"github.com/jackc/pglogrepl"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type ChangeEventMongoDB struct {
	ID                ResumeToken         `bson:"_id" json:"_id"`
	User              string              `bson:"user" json:"user"`
	Service           string              `bson:"service" json:"service"`
	Timestamp         primitive.Timestamp `bson:"timestamp" json:"timestamp"`
	OperationType     string              `bson:"operationType" json:"operationType"`
	Database          string              `bson:"database" json:"database"`
	Collection        string              `bson:"collection" json:"collection"`
	DocumentKey       string              `bson:"documentKey" json:"documentKey"`
	TenantID          string              `bson:"tenantid" json:"tenantid"`
	FullDocument      primitive.D         `bson:"fullDocument" json:"fullDocument"`
	UpdateDescription struct {
		UpdatedFields map[string]interface{} `bson:"updatedFields" json:"updatedFields"`
		RemovedFields interface{}            `bson:"removedFields" json:"removedFields"`
	} `bson:"updateDescription" json:"updateDescription"`
}

type ResumeToken struct {
	TokenData interface{} `bson:"_data" json:"_data"`
}

type ResumeTokenPostgres struct {
	TokenData pglogrepl.LSN `bson:"_data" json:"_data"`
}

type ChangeEventPostgres struct {
	ID                ResumeTokenPostgres    `bson:"_id" json:"_id"`
	User              string                 `bson:"user" json:"user"`
	Service           string                 `bson:"service" json:"service"`
	Timestamp         time.Time              `bson:"timestamp" json:"timestamp"`
	OperationType     string                 `bson:"operationType" json:"operationType"`
	Database          string                 `bson:"database" json:"database"`
	Table             string                 `bson:"table" json:"table"`
	RowID             string                 `bson:"rowid" json:"rowid"`
	TenantID          string                 `bson:"tenantid" json:"tenantid"`
	FullDocument      map[string]interface{} `bson:"fullDocument" json:"fullDocument"`
	UpdateDescription struct {
		UpdatedFields map[string]interface{} `bson:"updatedFields" json:"updatedFields"`
		RemovedFields interface{}            `bson:"removedFields" json:"removedFields"`
	} `bson:"updateDescription" json:"updateDescription"`
}
