package db

import (
	"context"
	"log"
	"strconv"

	"github.com/jackc/pgconn"
	_ "github.com/lib/pq"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// DataAccess provides data access functions for the underlying MongoDB collections - both the audited collection, as well
// as the collection containing audit records.
type DataAccess struct {
	MongoDbClient *mongo.Client
	PostgresConn  *pgconn.PgConn
}

// InitializeDataAccess initializes a connection to the indicated MongoDB database.
func InitializeDataAccessMongoDB(dbURL string) (*DataAccess, error) {
	dao := DataAccess{}

	c, err := mongo.NewClient(options.Client().ApplyURI(dbURL))
	if err != nil {
		return &dao, err
	}

	err = c.Connect(context.Background())
	if err != nil {
		return &dao, err
	}
	err = c.Ping(context.Background(), nil)
	if err != nil {
		return &dao, err
	}
	dao.MongoDbClient = c
	return &dao, nil
}

func InitializeDataAccessPostgres(dbURL string) (*DataAccess, error) {
	dao := DataAccess{}
	conn, err := pgconn.Connect(context.Background(), dbURL)
	if err != nil {
		log.Fatalln("failed to connect to PostgreSQL server:", err)
	}
	dao.PostgresConn = conn
	return &dao, nil
}

// TraverseForFieldValue walks through the supplied document of type primitive.D to locate and fetch the value of the
// a specific field, whose field path is supplied as an array (for example, {"arr", "0", "field1"} denotes
// the field "field1" of the first element of the array "arr").
func TraverseForFieldValue(f []string, payload primitive.D) interface{} {
	f1 := f[0]
	v1 := payload.Map()[f1]
	if len(f) == 1 {
		return v1
	}

	f2 := f[1]
	i, err := strconv.ParseInt(f2, 10, 64)
	if i == 0 && f2 != "0" {
		v1 = v1.(primitive.D).Map()[f2]
	} else if err == nil {
		arr := v1.(primitive.A)
		v1 = arr[i]
	}

	if len(f) == 2 {
		return v1
	}

	return TraverseForFieldValue(f[2:], v1.(primitive.D))
}

func TraverseForFieldValueMap(f []string, payload primitive.M) interface{} {
	f1 := f[0]
	v1 := payload[f1]
	if len(f) == 1 {
		return v1
	}

	f2 := f[1]
	i, err := strconv.ParseInt(f2, 10, 64)
	if i == 0 && f2 != "0" {
		v1 = v1.(primitive.D).Map()[f2]
	} else if err == nil {
		arr := v1.(primitive.A)
		v1 = arr[i]
	}

	if len(f) == 2 {
		return v1
	}

	return TraverseForFieldValue(f[2:], v1.(primitive.D))
}
