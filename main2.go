package main

import (
	"fmt"
)

func main() {
	a := "adad"
	b := fmt.Sprintf("%v", a)
	fmt.Println(a == b)
	//const outputPlugin = "test_decoding"
	//resumeToken.TokenData.(pglogrepl.LSN)
	//for _, c := range conf {
	// go func(conf config.Configuration) {
	// 	if conf.AppDBType == "postgres" {
	// 		appDao, err = db.InitializeDataAccessPostgres(conf.AppDBUrl)
	// 		if err != nil {
	// 			log.Printf("ERROR, failed to initialize data access to the App database due to error: %v\n", err)
	// 			os.Exit(1)
	// 		}
	// 		r := changeevents.PostgresChangeLogTracker{Config: conf, Dao: auditDao}
	// 		resToken, err = r.GetResumeToken()
	// 		if err != nil {
	// 			log.Printf("ERROR, failed to look up resume token with error: %v\n", err)
	// 			os.Exit(1)
	// 		}
	// 		w := watcher.PostgresChangeStreamWatcher{Config: conf, Dao: appDao}
	// 		err = w.WatchChangeEvents(resToken, dispatch.GetSaveChangeEventFuncPostgres(conf, auditDao))
	// 		if err != nil {
	// 			log.Printf("ERROR, failed to watch change stream due to error: %v\n", err)
	// 			os.Exit(1)
	// 		}
	// 	} else {
	// 		appDao, err = db.InitializeDataAccessMongoDB(conf.AppDBUrl)
	// 		if err != nil {
	// 			log.Printf("ERROR, failed to initialize data access to the App database due to error: %v\n", err)
	// 			os.Exit(1)
	// 		}
	// 		r := changeevents.MongoDBChangeLogTracker{Config: conf, Dao: auditDao}
	// 		resToken, err = r.GetResumeToken()
	// 		if err != nil {
	// 			log.Printf("ERROR, failed to look up resume token with error: %v\n", err)
	// 			os.Exit(1)
	// 		}
	// 		w := watcher.MongoDBChangeStreamWatcher{Config: conf, Dao: appDao}
	// 		err = w.WatchChangeEvents(resToken, dispatch.GetSaveChangeEventFuncMongoDB(conf, auditDao))
	// 		if err != nil {
	// 			log.Printf("ERROR, failed to watch change stream due to error: %v\n", err)
	// 			os.Exit(1)
	// 		}
	// 	}
	// }(c)
	//}

	// if conf.AppDBType == "postgres" {
	// 	appDao, err = db.InitializeDataAccessPostgres(conf.AppDBUrl)
	// 	if err != nil {
	// 		log.Printf("ERROR, failed to initialize data access to the App database due to error: %v\n", err)
	// 		os.Exit(1)
	// 	}
	// 	r := changeevents.PostgresChangeLogTracker{Config: conf, Dao: auditDao}
	// 	resToken, err := r.GetResumeToken()
	// 	if err != nil {
	// 		log.Printf("ERROR, failed to look up resume token with error: %v\n", err)
	// 		os.Exit(1)
	// 	}
	// 	w := watcher.PostgresChangeStreamWatcher{Config: conf, Dao: appDao}
	// 	err = w.WatchChangeEvents(resToken, dispatch.GetSaveChangeEventFuncPostgres(conf, auditDao))
	// 	if err != nil {
	// 		log.Printf("ERROR, failed to watch change stream due to error: %v\n", err)
	// 		os.Exit(1)
	// 	}
	// } else {
	// 	appDao, err = db.InitializeDataAccessMongoDB(conf.AppDBUrl)
	// 	if err != nil {
	// 		log.Printf("ERROR, failed to initialize data access to the App database due to error: %v\n", err)
	// 		os.Exit(1)
	// 	}
	// 	r := changeevents.MongoDBChangeLogTracker{Config: conf, Dao: auditDao}
	// 	resToken, err = r.GetResumeToken()
	// 	if err != nil {
	// 		log.Printf("ERROR, failed to look up resume token with error: %v\n", err)
	// 		os.Exit(1)
	// 	}
	// 	w := watcher.MongoDBChangeStreamWatcher{Config: conf, Dao: appDao}
	// 	err = w.WatchChangeEvents(resToken, dispatch.GetSaveChangeEventFuncMongoDB(conf, auditDao))
	// 	if err != nil {
	// 		log.Printf("ERROR, failed to watch change stream due to error: %v\n", err)
	// 		os.Exit(1)
	// 	}
	// }
	// 	const outputPlugin = "pgoutput"
	// 	fmt.Println(os.Getenv("PGLOGREPL_TEST_CONN_STRING"))
	// 	conn, err := pgconn.Connect(context.Background(), os.Getenv("PGLOGREPL_TEST_CONN_STRING"))
	// 	if err != nil {
	// 		log.Fatalln("failed to connect to PostgreSQL server:", err)
	// 	}
	// 	defer conn.Close(context.Background())

	// 	result := conn.Exec(context.Background(), "DROP PUBLICATION IF EXISTS pglogrepl_demo;")
	// 	_, err = result.ReadAll()
	// 	if err != nil {
	// 		log.Fatalln("drop publication if exists error", err)
	// 	}

	// 	result = conn.Exec(context.Background(), "CREATE PUBLICATION pglogrepl_demo FOR ALL TABLES;")
	// 	_, err = result.ReadAll()
	// 	if err != nil {
	// 		log.Fatalln("create publication error", err)
	// 	}
	// 	log.Println("create publication pglogrepl_demo")

	// 	var pluginArguments []string
	// 	if outputPlugin == "pgoutput" {
	// 		pluginArguments = []string{"proto_version '1'", "publication_names 'pglogrepl_demo'"}
	// 	} else if outputPlugin == "wal2json" {
	// 		pluginArguments = []string{"\"pretty-print\" 'true'"}
	// 	}

	// 	sysident, err := pglogrepl.IdentifySystem(context.Background(), conn)
	// 	if err != nil {
	// 		log.Fatalln("IdentifySystem failed:", err)
	// 	}
	// 	log.Println("SystemID:", sysident.SystemID, "Timeline:", sysident.Timeline, "XLogPos:", sysident.XLogPos, "DBName:", sysident.DBName)

	// 	slotName := "pglogrepl_demo"

	// 	_, err = pglogrepl.CreateReplicationSlot(context.Background(), conn, slotName, outputPlugin, pglogrepl.CreateReplicationSlotOptions{Temporary: true})
	// 	if err != nil {
	// 		log.Fatalln("CreateReplicationSlot failed:", err)
	// 	}
	// 	log.Println("Created temporary replication slot:", slotName)
	// 	err = pglogrepl.StartReplication(context.Background(), conn, slotName, sysident.XLogPos, pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments})
	// 	if err != nil {
	// 		log.Fatalln("StartReplication failed:", err)
	// 	}
	// 	log.Println("Logical replication started on slot", slotName)

	// 	clientXLogPos := sysident.XLogPos
	// 	standbyMessageTimeout := time.Second * 10
	// 	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)
	// 	relations := map[uint32]*pglogrepl.RelationMessage{}
	// 	connInfo := pgtype.NewConnInfo()

	// 	for {
	// 		if time.Now().After(nextStandbyMessageDeadline) {
	// 			err = pglogrepl.SendStandbyStatusUpdate(context.Background(), conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
	// 			if err != nil {
	// 				log.Fatalln("SendStandbyStatusUpdate failed:", err)
	// 			}
	// 			log.Println("Sent Standby status message")
	// 			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
	// 		}

	// 		ctx, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)
	// 		rawMsg, err := conn.ReceiveMessage(ctx)
	// 		cancel()
	// 		if err != nil {
	// 			if pgconn.Timeout(err) {
	// 				continue
	// 			}
	// 			log.Fatalln("ReceiveMessage failed:", err)
	// 		}

	// 		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
	// 			log.Fatalln("received Postgres WAL error: %+v", errMsg)
	// 		}

	// 		msg, ok := rawMsg.(*pgproto3.CopyData)
	// 		if !ok {
	// 			log.Printf("Received unexpected message: %T\n", rawMsg)
	// 			continue
	// 		}

	// 		switch msg.Data[0] {
	// 		case pglogrepl.PrimaryKeepaliveMessageByteID:
	// 			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
	// 			if err != nil {
	// 				log.Fatalln("ParsePrimaryKeepaliveMessage failed:", err)
	// 			}
	// 			log.Println("Primary Keepalive Message =>", "ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime, "ReplyRequested:", pkm.ReplyRequested)

	// 			if pkm.ReplyRequested {
	// 				nextStandbyMessageDeadline = time.Time{}
	// 			}

	// 		case pglogrepl.XLogDataByteID:
	// 			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
	// 			if err != nil {
	// 				log.Fatalln("ParseXLogData failed:", err)
	// 			}
	// 			log.Println("XLogData =>", "WALStart", xld.WALStart, "ServerWALEnd", xld.ServerWALEnd, "ServerTime:", xld.ServerTime, "WALData", string(xld.WALData))
	// 			logicalMsg, err := pglogrepl.Parse(xld.WALData)
	// 			if err != nil {
	// 				log.Fatalf("Parse logical replication message: %s", err)
	// 			}
	// 			log.Printf("Receive a logical replication message: %s", logicalMsg.Type())
	// 			switch logicalMsg := logicalMsg.(type) {
	// 			case *pglogrepl.RelationMessage:
	// 				relations[logicalMsg.RelationID] = logicalMsg

	// 			case *pglogrepl.BeginMessage:
	// 				// Indicates the beginning of a group of changes in a transaction. This is only sent for committed transactions. You won't get any events from rolled back transactions.

	// 			case *pglogrepl.CommitMessage:

	// 			case *pglogrepl.InsertMessage:
	// 				rel, ok := relations[logicalMsg.RelationID]
	// 				if !ok {
	// 					log.Fatalf("unknown relation ID %d", logicalMsg.RelationID)
	// 				}
	// 				values := map[string]interface{}{}

	// 				for idx, col := range logicalMsg.Tuple.Columns {
	// 					colName := rel.Columns[idx].Name
	// 					switch col.DataType {
	// 					case 'n': // null
	// 						values[colName] = nil
	// 					case 'u': // unchanged toast
	// 						// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
	// 					case 't': //text
	// 						val, err := decodeTextColumnData(connInfo, col.Data, rel.Columns[idx].DataType)
	// 						if err != nil {
	// 							log.Fatalf("error decoding column data: %w", err)
	// 						}
	// 						values[colName] = val
	// 					}
	// 				}
	// 				log.Printf("INSERT INTO %s.%s: %v", rel.Namespace, rel.RelationName, values)

	// 			case *pglogrepl.UpdateMessage:
	// 				rel, ok := relations[logicalMsg.RelationID]
	// 				fmt.Println(rel.RelationName)
	// 				if !ok {
	// 					log.Fatalf("unknown relation ID %d", logicalMsg.RelationID)
	// 				}

	// 				values := map[string]interface{}{}
	// 				changedValues:= map[string]interface{}{}
	// 				for idx, col := range logicalMsg.NewTuple.Columns{
	// 					colName := rel.Columns[idx].Name
	// 					if logicalMsg.OldTuple.Columns[idx] != col{
	// 						switch col.DataType {
	// 							case 'n': // null
	// 								values[colName] = nil
	// 							case 'u': // unchanged toast
	// 								// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
	// 							case 't': //text
	// 								val, err := decodeTextColumnData(connInfo, col.Data, rel.Columns[idx].DataType)
	// 								val2, err := decodeTextColumnData(connInfo, logicalMsg.OldTuple.Columns[idx].Data, rel.Columns[idx].DataType)
	// 								if err != nil {
	// 									log.Fatalf("error decoding column data: %w", err)
	// 								}
	// 								values[colName] = val
	// 								if val != val2{
	// 									changedValues[colName] = val
	// 								}
	// 							}
	// 					}
	// 				}
	// 				log.Printf("updated %s.%s: %v", rel.Namespace, rel.RelationName, values)
	// 				// ...
	// 			case *pglogrepl.DeleteMessage:
	// 				// ...
	// 			case *pglogrepl.TruncateMessage:
	// 				// ...

	// 			case *pglogrepl.TypeMessage:
	// 			case *pglogrepl.OriginMessage:
	// 			default:
	// 				log.Printf("Unknown message type in pgoutput stream: %T", logicalMsg)
	// 			}

	// 			clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
	// 			fmt.Println(clientXLogPos)
	// 		}
	// 	}
}

// func decodeTextColumnData(ci *pgtype.ConnInfo, data []byte, dataType uint32) (interface{}, error) {
// 	var decoder pgtype.TextDecoder
// 	if dt, ok := ci.DataTypeForOID(dataType); ok {
// 		decoder, ok = dt.Value.(pgtype.TextDecoder)
// 		if !ok {
// 			decoder = &pgtype.GenericText{}
// 		}
// 	} else {
// 		decoder = &pgtype.GenericText{}
// 	}
// 	if err := decoder.DecodeText(ci, data); err != nil {
// 		return nil, err
// 	}
// 	return decoder.(pgtype.Value).Get(), nil
// }
