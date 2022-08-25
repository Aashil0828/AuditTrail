package watcher

import (
	"audit_trail/config"
	"audit_trail/db"
	"audit_trail/dispatch"
	"audit_trail/models"
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ChangeEventWatcher watches a change stream for change events, and dispatches received changes events
// via one or more change event dispatch functions.
type MongoDBChangeEventWatcher interface {
	// WatchChangeEvents resumes watching the change events from the supplied change event token, passing any received
	// event to the supplied dispatch functions for handling.
	WatchChangeEvents(resumeToken *models.ResumeToken, disps ...dispatch.MongoDBChangeEventDispatcherFunc) error
}

// MongoDBChangeStreamWatcher watches a MongoDB change stream for change events and reacts to those events.
type MongoDBChangeStreamWatcher struct {
	Config config.Configuration
	Dao    *db.DataAccess
}

type PostgresChangeEventWatcher interface {
	// WatchChangeEvents resumes watching the change events from the supplied change event token, passing any received
	// event to the supplied dispatch functions for handling.
	WatchChangeEvents(resumeToken *models.ResumeToken, disps ...dispatch.PostgresChangeEventDispatcherFunc) error
}

type PostgresChangeStreamWatcher struct {
	Config config.Configuration
	Dao    *db.DataAccess
}

func recoverFunc() {
	if r := recover(); r != nil {
		fmt.Println("Recovered", r)
	}
}

// WatchChangeEvents starts watching the mongo change stream for the MongoDB collection associated with the  MongoDBChangeStreamWatcher. If a valid
// resume token is provided, the stream starts from that point.
func (m *MongoDBChangeStreamWatcher) WatchChangeEvents(resumeToken models.ResumeToken, dispatchFuncs ...dispatch.MongoDBChangeEventDispatcherFunc) error {
	defer recoverFunc()
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	if resumeToken.TokenData != nil {
		opts.SetResumeAfter(resumeToken)
	}

	coll := m.Dao.MongoDbClient.Database(m.Config.AppDatabase).Collection(m.Config.AppCollection)
	ctx := context.Background()
	watchCursor, err := coll.Watch(ctx, buildChangeStreamAggregationPipeline(), opts)
	if err != nil {
		log.Printf("ERROR: encountered error watching for change stream data, error is %v\n", err)
		return err
	}
	defer watchCursor.Close(ctx)

	// wait for the next change stream data to become available
	log.Println("Mongo stream watcher launched, waiting for change events...")
	for watchCursor.Next(ctx) {
		ce, err := m.extractChangeEvent(watchCursor.Current)
		if err != nil {
			log.Printf("ERROR: failed to extract change event from change stream data, error is %v\n ", err)
			return err
		}

		for _, dispFunc := range dispatchFuncs {
			go func(f func(ev models.ChangeEventMongoDB) error, e models.ChangeEventMongoDB) {
				log.Printf("Received change event %s, dispatching to audit logger\n", e.ID)
				err := f(e)
				if err != nil {
					log.Printf("ERROR: failed to invoke change event dispatch function, err is : %v\n", err)
				}
			}(dispFunc, ce)
		}
	}

	return nil
}

// extractChangeEvent transforms the raw data received from the MongoDB change stream to the model.ChangeEvent type.
func (m *MongoDBChangeStreamWatcher) extractChangeEvent(rawChange bson.Raw) (models.ChangeEventMongoDB, error) {
	var ce models.ChangeEventMongoDB
	err := bson.Unmarshal(rawChange, &ce)
	if err != nil {
		return ce, err
	}
	// extract the user name of the change owner, assuming this is contained within the configured field of the full document.
	// In case the field is not present, or if there is an error, this defaults to a blank value.
	ce.User = db.TraverseForFieldValue(strings.Split(m.Config.UserFieldPath, "."), ce.FullDocument).(string)
	ce.Service = m.Config.ServiceName

	// if there is no need to record the full document for the current operation type, remove it
	if !m.Config.CaptureFullDocument[ce.OperationType] {
		ce.FullDocument = nil
	}

	return ce, nil
}

// buildChangeStreamAggregationPipeline builds a MongoDB aggregation pipeline to reshape the change stream data received from MongoDB in
// the format of our change events. See model.ChangeEvent.
func buildChangeStreamAggregationPipeline() mongo.Pipeline {
	pipeline := mongo.Pipeline{bson.D{{Key: "$addFields", Value: bson.D{{Key: "timestamp", Value: "$clusterTime"}, {Key: "database", Value: "$ns.db"}, {Key: "collection", Value: "$ns.coll"}, {Key: "documentKey", Value: "$documentKey._id"}}}},
		bson.D{{Key: "$project", Value: bson.D{{Key: "timestamp", Value: 1}, {Key: "operationType", Value: 1}, {Key: "database", Value: 1}, {Key: "collection", Value: 1}, {Key: "documentKey", Value: 1}, {Key: "fullDocument", Value: 1}, {Key: "updateDescription", Value: 1}}}}}

	return pipeline
}

func (p *PostgresChangeStreamWatcher) WatchChangeEvents(resumeToken models.ResumeTokenPostgres, dispatchFuncs ...dispatch.PostgresChangeEventDispatcherFunc) error {
	const outputPlugin = "pgoutput"
	replica_identity:= fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", p.Config.AppCollection)
	create_sql := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", p.Config.PublicationName, p.Config.AppCollection)
	drop_sql := fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", p.Config.PublicationName)
	_, err := p.Dao.PostgresConn.Exec(context.Background(), replica_identity).ReadAll()
	if err != nil {
		return fmt.Errorf("cannot set replica identity to full: %v", err)
	}
	log.Printf("replica identity set to full: %v", p.Config.AppCollection)
	_, err = p.Dao.PostgresConn.Exec(context.Background(), drop_sql).ReadAll()
	if err != nil {
		return fmt.Errorf("cannot drop publication: %v", err)
	}
	log.Printf("dropped publication: %v", p.Config.PublicationName)
	_, err = p.Dao.PostgresConn.Exec(context.Background(), create_sql).ReadAll()
	if err != nil {
		return fmt.Errorf("cannot create publication: %v", err)
	}
	log.Printf("create publication %v", p.Config.PublicationName)
	var pluginArguments []string
	if outputPlugin == "pgoutput" {
		pubNames := fmt.Sprintf("publication_names '%v'", p.Config.PublicationName)
		pluginArguments = []string{"proto_version '1'", pubNames}
	} else if outputPlugin == "wal2json" {
		pluginArguments = []string{"\"pretty-print\" 'true'"}
	}
	sysident, err := pglogrepl.IdentifySystem(context.Background(), p.Dao.PostgresConn)
	if err != nil {
		return fmt.Errorf("IdentifySystem failed: %v", err)
	}
	log.Println("SystemID:", sysident.SystemID, "Timeline:", sysident.Timeline, "XLogPos:", sysident.XLogPos, "DBName:", sysident.DBName)
	_, err = pglogrepl.CreateReplicationSlot(context.Background(), p.Dao.PostgresConn, p.Config.PublicationName, outputPlugin, pglogrepl.CreateReplicationSlotOptions{Temporary: true})
	if err != nil {
		return fmt.Errorf("CreateReplicationSlot failed: %v", err)
	}
	log.Println("Created temporary replication slot:", p.Config.PublicationName)
	if resumeToken.TokenData != 0 {
		err = pglogrepl.StartReplication(context.Background(), p.Dao.PostgresConn, p.Config.PublicationName, resumeToken.TokenData, pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments})
	} else {
		err = pglogrepl.StartReplication(context.Background(), p.Dao.PostgresConn, p.Config.PublicationName, sysident.XLogPos, pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments})
	}
	if err != nil {
		return fmt.Errorf("StartReplication failed: %v", err)
	}
	log.Println("Logical replication started on slot", p.Config.PublicationName)
	clientXLogPos := resumeToken.TokenData
	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)
	relations := map[uint32]*pglogrepl.RelationMessage{}
	connInfo := pgtype.NewConnInfo()
	for {
		if time.Now().After(nextStandbyMessageDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(context.Background(), p.Dao.PostgresConn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
			if err != nil {
				return fmt.Errorf("SendStandbyStatusUpdate failed: %v", err)
			}
			log.Println("Sent Standby status message")
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		ctx, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)
		rawMsg, err := p.Dao.PostgresConn.ReceiveMessage(ctx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			return fmt.Errorf("ReceiveMessage failed: %v", err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			return fmt.Errorf("received Postgres WAL error: %+v", errMsg)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			log.Printf("Received unexpected message: %T\n", msg)
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				return fmt.Errorf("ParsePrimaryKeepaliveMessage failed: %v", err)
			}
			log.Println("Primary Keepalive Message =>", "ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime, "ReplyRequested:", pkm.ReplyRequested)

			if pkm.ReplyRequested {
				nextStandbyMessageDeadline = time.Time{}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				return fmt.Errorf("ParseXLogData failed: %v", err)
			}
			log.Println("XLogData =>", "WALStart", xld.WALStart, "ServerWALEnd", xld.ServerWALEnd, "ServerTime:", xld.ServerTime, "WALData", string(xld.WALData))
			logicalMsg, err := pglogrepl.Parse(xld.WALData)
			if err != nil {
				return fmt.Errorf("parse logical replication message: %v", err)
			}
			log.Printf("Receive a logical replication message: %s", logicalMsg.Type())
			switch logicalMsg := logicalMsg.(type) {
			case *pglogrepl.RelationMessage:
				relations[logicalMsg.RelationID] = logicalMsg

			case *pglogrepl.BeginMessage:
				// Indicates the beginning of a group of changes in a transaction. This is only sent for committed transactions. You won't get any events from rolled back transactions.

			case *pglogrepl.CommitMessage:

			case *pglogrepl.InsertMessage:
				rel, ok := relations[logicalMsg.RelationID]
				if !ok {
					return fmt.Errorf("unknown relation ID %d", logicalMsg.RelationID)
				}
				values := map[string]interface{}{}
				var User string
				var RowID string
				var TenantID string
				for idx, col := range logicalMsg.Tuple.Columns {
					colName := rel.Columns[idx].Name
					switch col.DataType {
					case 'n': // null
						values[colName] = nil
					case 'u': // unchanged toast
						// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
					case 't': //text
						if p.Config.CaptureFullDocument["insert"] {
							val, err := decodeTextColumnData(connInfo, col.Data, rel.Columns[idx].DataType)
							if err != nil {
								return fmt.Errorf("error decoding column data: %w", err)
							}
							if colName == p.Config.UserFieldPath {
								User = val.(string)
							}
							if colName == p.Config.RowIDName {
								RowID = fmt.Sprintf("%v", val)
							}
							if colName == p.Config.TenantIDName {
								TenantID = fmt.Sprintf("%v", val)
							}
							values[colName] = val
						}
					}
				}
				ce := models.ChangeEventPostgres{
					ID:            models.ResumeTokenPostgres{TokenData: xld.ServerWALEnd},
					User:          User,
					Timestamp:     xld.ServerTime,
					OperationType: "insert",
					Service:       p.Config.ServiceName,
					Database:      p.Config.AppDatabase,
					Table:         p.Config.AppCollection,
					RowID:         RowID,
					TenantID:      TenantID,
					FullDocument:  values,
					UpdateDescription: struct {
						UpdatedFields map[string]interface{} "bson:\"updatedFields\" json:\"updatedFields\""
						RemovedFields interface{}            "bson:\"removedFields\" json:\"removedFields\""
					}{},
				}
				for _, dispFunc := range dispatchFuncs {
					go func(f func(ev models.ChangeEventPostgres) error, e models.ChangeEventPostgres) {
						log.Printf("Received change event %s, dispatching to audit logger\n", e.ID)
						err := f(e)
						if err != nil {
							log.Printf("ERROR: failed to invoke change event dispatch function, err is : %v\n", err)
						}
					}(dispFunc, ce)
				}
				log.Printf("INSERT INTO %s.%s: %v", rel.Namespace, rel.RelationName, values)

			case *pglogrepl.UpdateMessage:
				rel, ok := relations[logicalMsg.RelationID]
				if !ok {
					return fmt.Errorf("unknown relation ID %d", logicalMsg.RelationID)
				}

				values := map[string]interface{}{}
				changedValues := map[string]interface{}{}
				var User string
				var RowID string
				var TenantID string
				for idx, col := range logicalMsg.NewTuple.Columns {
					colName := rel.Columns[idx].Name
					if logicalMsg.OldTuple.Columns[idx] != col {
						switch col.DataType {
						case 'n': // null
							values[colName] = nil
						case 'u': // unchanged toast
							// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
						case 't': //text
							val, err := decodeTextColumnData(connInfo, col.Data, rel.Columns[idx].DataType)
							if err != nil {
								return fmt.Errorf("error decoding column data: %w", err)
							}
							val2, err := decodeTextColumnData(connInfo, logicalMsg.OldTuple.Columns[idx].Data, rel.Columns[idx].DataType)
							if err != nil {
								return fmt.Errorf("error decoding old tuple column data: %w", err)
							}
							if colName == p.Config.UserFieldPath {
								User = val.(string)
							}
							if colName == p.Config.RowIDName {
								RowID = fmt.Sprintf("%v", val)
							}
							if colName == p.Config.TenantIDName {
								TenantID = fmt.Sprintf("%v", val)
							}
							if p.Config.CaptureFullDocument["update"] {
								values[colName] = val
							}
							if val != val2 {
								changedValues[colName] = val
							}
						}
					}
				}
				ce := models.ChangeEventPostgres{
					ID:            models.ResumeTokenPostgres{TokenData: xld.ServerWALEnd},
					User:          User,
					Timestamp:     xld.ServerTime,
					OperationType: "update",
					Service:       p.Config.ServiceName,
					Database:      p.Config.AppDatabase,
					Table:         p.Config.AppCollection,
					RowID:         RowID,
					TenantID:      TenantID,
					FullDocument:  values,
					UpdateDescription: struct {
						UpdatedFields map[string]interface{} "bson:\"updatedFields\" json:\"updatedFields\""
						RemovedFields interface{}            "bson:\"removedFields\" json:\"removedFields\""
					}{UpdatedFields: changedValues},
				}
				for _, dispFunc := range dispatchFuncs {
					go func(f func(ev models.ChangeEventPostgres) error, e models.ChangeEventPostgres) {
						log.Printf("Received change event %s, dispatching to audit logger\n", e.ID)
						err := f(e)
						if err != nil {
							log.Printf("ERROR: failed to invoke change event dispatch function, err is : %v\n", err)
						}
					}(dispFunc, ce)
				}
				log.Printf("updated %s.%s: %v", rel.Namespace, rel.RelationName, changedValues)
				// ...
			case *pglogrepl.DeleteMessage:
				rel, ok := relations[logicalMsg.RelationID]
				if !ok {
					return fmt.Errorf("unknown relation ID %d", logicalMsg.RelationID)
				}
				values := map[string]interface{}{}
				var User string
				var RowID string
				var TenantID string
				for idx, col := range logicalMsg.OldTuple.Columns {
					colName := rel.Columns[idx].Name
					switch col.DataType {
					case 'n': // null
						values[colName] = nil
					case 'u': // unchanged toast
						// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
					case 't': //text
						if p.Config.CaptureFullDocument["delete"] {
							val, err := decodeTextColumnData(connInfo, col.Data, rel.Columns[idx].DataType)
							if err != nil {
								return fmt.Errorf("error decoding column data: %w", err)
							}
							if colName == p.Config.UserFieldPath {
								User = val.(string)
							}
							if colName == p.Config.RowIDName {
								RowID = fmt.Sprintf("%v", val)
							}
							if colName == p.Config.TenantIDName {
								TenantID = fmt.Sprintf("%v", val)
							}
							values[colName] = val
						}
					}
				}
				ce := models.ChangeEventPostgres{
					ID:            models.ResumeTokenPostgres{TokenData: xld.ServerWALEnd},
					User:          User,
					Timestamp:     xld.ServerTime,
					OperationType: "delete",
					Service:       p.Config.ServiceName,
					Database:      p.Config.AppDatabase,
					Table:         p.Config.AppCollection,
					RowID:         RowID,
					TenantID:      TenantID,
					FullDocument:  values,
					UpdateDescription: struct {
						UpdatedFields map[string]interface{} "bson:\"updatedFields\" json:\"updatedFields\""
						RemovedFields interface{}            "bson:\"removedFields\" json:\"removedFields\""
					}{},
				}
				for _, dispFunc := range dispatchFuncs {
					go func(f func(ev models.ChangeEventPostgres) error, e models.ChangeEventPostgres) {
						log.Printf("Received change event %s, dispatching to audit logger\n", e.ID)
						err := f(e)
						if err != nil {
							log.Printf("ERROR: failed to invoke change event dispatch function, err is : %v\n", err)
						}
					}(dispFunc, ce)
				}
				log.Printf("deleted %s.%s: %v", rel.Namespace, rel.RelationName, values)
			case *pglogrepl.TruncateMessage:
				// ...

			case *pglogrepl.TypeMessage:
			case *pglogrepl.OriginMessage:
			default:
				log.Printf("Unknown message type in pgoutput stream: %T", logicalMsg)
			}
			clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
		}
	}
}

func decodeTextColumnData(ci *pgtype.ConnInfo, data []byte, dataType uint32) (interface{}, error) {
	var decoder pgtype.TextDecoder
	if dt, ok := ci.DataTypeForOID(dataType); ok {
		decoder, ok = dt.Value.(pgtype.TextDecoder)
		if !ok {
			decoder = &pgtype.GenericText{}
		}
	} else {
		decoder = &pgtype.GenericText{}
	}
	if err := decoder.DecodeText(ci, data); err != nil {
		return nil, err
	}
	return decoder.(pgtype.Value).Get(), nil
}
