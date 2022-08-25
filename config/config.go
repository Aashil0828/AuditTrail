package config

import (
	"audit_trail/db"
	"context"
	"mgc"

	"go.mongodb.org/mongo-driver/bson"
)

type Configuration struct {
	AppDBType           string          `json:"appDbType" bson:"appDbType"`
	PublicationName     string          `json:"publicationName" bson:"publicationName"`
	ServiceName         string          `json:"serviceName" bson:"serviceName"`
	AppDBUrl            string          `json:"appDbUrl" bson:"appDbUrl"`
	AppDatabase         string          `json:"appDatabaseName" bson:"appDatabaseName"`
	AppCollection       string          `json:"appCollection" bson:"appCollection"`
	RowIDName           string          `json:"rowIDName" bson:"rowIDName"`
	TenantIDName        string          `json:"tenantIDName" bson:"tenantIDName"`
	UserFieldPath       string          `json:"userFieldPath" bson:"userFieldPath"`
	AuditDBUrl          string          `json:"auditDbUrl" bson:"auditDbUrl"`
	AuditDatabase       string          `json:"auditDatabaseName" bson:"auditDatabaseName"`
	AuditCollection     string          `json:"auditCollection" bson:"auditCollection"`
	CaptureFullDocument map[string]bool `json:"fullDocRecordOperations" bson:"fullDocRecordOperations"`
	Version             float64         `json:"version" bson:"version"`
}

type GetConfig struct {
	Dao *db.DataAccess
}

func (g *GetConfig) GetConfiguration() ([]Configuration, error) {
	coll := g.Dao.MongoDbClient.Database("app_config").Collection("config")
	ctx := context.Background()
	result, err := coll.Find(ctx, mgc.M{})
	if err != nil {
		return []Configuration{}, err
	}
	var conf Configuration
	var confResult []Configuration
	for result.Next(ctx) {
		raw := result.Current
		err := bson.Unmarshal(raw, &conf)
		if err != nil {
			return []Configuration{}, err
		}
		confResult = append(confResult, conf)
	}
	return confResult, err
}
