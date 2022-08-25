package changeevents

import (
	"audit_trail/config"
	"audit_trail/db"
	"audit_trail/models"
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ChangeLogger saves the supplied change event to a persistent store.
type ChangeLogger interface {
	SaveMongoChangeEvent(ce models.ChangeEventMongoDB) error
	SavePostgresChangeEvent(ce models.ChangeEventPostgres) error
}

// ChangeLogTracker fetches metadata of stored change events.
type ChangeLogTracker interface {
	GetResumeToken() (models.ResumeToken, error)
}

// MongoDBChangeLogTracker fetches metadata of change events stored in the MongoDB audit collection.
type MongoDBChangeLogTracker struct {
	Config config.Configuration
	Dao    *db.DataAccess
}

type PostgresChangeLogTracker struct {
	Config config.Configuration
	Dao    *db.DataAccess
}

// GetResumeToken returns the mongo stream token for the last change stream event that was recorded in the audit database.
// This may be used to resume receiving change events from the point of the last change event.
func (m *MongoDBChangeLogTracker) GetResumeToken() (models.ResumeToken, error) {
	coll := m.Dao.MongoDbClient.Database(m.Config.AuditDatabase).Collection(m.Config.AuditCollection)

	var opts options.FindOptions
	var l = int64(1)
	opts.Limit = &l
	opts.Sort = map[string]int{"timestamp": -1}
	opts.Projection = map[string]int{"_id": 1}
	ctx := context.Background()
	cur, err := coll.Find(ctx, bson.D{}, &opts)
	if err != nil {
		return models.ResumeToken{}, err
	}
	defer cur.Close(ctx)

	var ce models.ChangeEventMongoDB
	if cur.Next(ctx) {
		raw := cur.Current
		err := bson.Unmarshal(raw, &ce)
		if err != nil {
			return models.ResumeToken{}, err
		}
	}

	return ce.ID, nil
}

func (p *PostgresChangeLogTracker) GetResumeToken() (models.ResumeTokenPostgres, error) {
	coll := p.Dao.MongoDbClient.Database(p.Config.AuditDatabase).Collection(p.Config.AuditCollection)

	var opts options.FindOptions
	var l = int64(1)
	opts.Limit = &l
	opts.Sort = map[string]int{"timestamp": -1}
	opts.Projection = map[string]int{"_id": 1}
	ctx := context.Background()
	cur, err := coll.Find(ctx, bson.D{}, &opts)
	if err != nil {
		return models.ResumeTokenPostgres{}, err
	}
	defer cur.Close(ctx)

	var ce models.ChangeEventPostgres
	if cur.Next(ctx) {
		raw := cur.Current
		err := bson.Unmarshal(raw, &ce)
		if err != nil {
			return models.ResumeTokenPostgres{}, err
		}
	}

	return ce.ID, nil
}

// MongoDBChangeLogger can be used to save a change event to a MongoDB collection designated to store
// audit records.
type MongoDBChangeLogger struct {
	Config config.Configuration
	Dao    *db.DataAccess
}

// SaveChangeEvent saves the change event to the audit database.
func (m *MongoDBChangeLogger) SaveMongoChangeEvent(ce models.ChangeEventMongoDB) error {
	ctx := context.Background()
	coll := m.Dao.MongoDbClient.Database(m.Config.AuditDatabase).Collection(m.Config.AuditCollection)

	update := bson.M{
		"$set": ce,
	}

	_, err := coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: ce.ID}}, update, options.Update().SetUpsert(true))
	return err
}

func (m *MongoDBChangeLogger) SavePostgresChangeEvent(ce models.ChangeEventPostgres) error {
	ctx := context.Background()
	coll := m.Dao.MongoDbClient.Database(m.Config.AuditDatabase).Collection(m.Config.AuditCollection)

	update := bson.M{
		"$set": ce,
	}

	_, err := coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: ce.ID}}, update, options.Update().SetUpsert(true))
	return err
}
