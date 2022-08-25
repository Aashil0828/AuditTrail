package dispatch

import (
	changeevents "audit_trail/changeEvents"
	"audit_trail/config"
	"audit_trail/db"
	"audit_trail/models"
	"log"
)

// ChangeEventDispatcherFunc is a type of function that can act on a change event.
type MongoDBChangeEventDispatcherFunc func(ce models.ChangeEventMongoDB) error
type PostgresChangeEventDispatcherFunc func(ce models.ChangeEventPostgres) error

// GetSaveChangeEventFunc returns a function that can save a change event to an audit store.
func GetSaveChangeEventFuncMongoDB(c config.Configuration, dao *db.DataAccess) MongoDBChangeEventDispatcherFunc {
	lt := changeevents.MongoDBChangeLogger{Config: c, Dao: dao}
	return func(ce models.ChangeEventMongoDB) error {
		log.Printf("Saving change event of type %s for collection %s for database %s for record %v\n", ce.OperationType, ce.Collection, ce.Database, ce.DocumentKey)
		err := lt.SaveMongoChangeEvent(ce)
		if err != nil {
			log.Printf("ERROR: failed to save change event ID %v to audit DB due to error: %v\n", ce.ID.TokenData, err)
			return err
		}
		return nil
	}
}

func GetSaveChangeEventFuncPostgres(c config.Configuration, dao *db.DataAccess) PostgresChangeEventDispatcherFunc {
	lt := changeevents.MongoDBChangeLogger{Config: c, Dao: dao}
	return func(ce models.ChangeEventPostgres) error {
		log.Printf("Saving change event of type %s for collection %s for database %s for record %v\n", ce.OperationType, ce.Table, ce.Database, ce.RowID)
		err := lt.SavePostgresChangeEvent(ce)
		if err != nil {
			log.Printf("ERROR: failed to save change event ID %v to audit DB due to error: %v\n", ce.ID.TokenData, err)
			return err
		}
		return nil
	}
}
