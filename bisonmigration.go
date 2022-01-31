package bisonmigration

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"log"
	"sort"
	"time"
)

type migrationDbRecordType struct {
	Sequence                int64  `bson:"sequence"`
	Name                    string `bson:"name"`
	UniqueId                string `bson:"uniqueid"`
	ProcessedTimeUnix       int64  `bson:"processedtimeunix"`
	ProcessedTimeSpentMs    int64  `bosn:"processedtimespentms"`
	ProcessedBatch          int64  `bson:"processedbatch"`
	DbConnectionLabel       string `bson:"dbconnectionlabel"`
	DbConnectionDescription string `bson:"dbconnectiondescription"`
}

type migrationFunctionSignatureType func(db *mongo.Client) error

type migrationRegisteredType struct {
	Sequence                int64
	Name                    string
	UniqueId                string
	up                      migrationFunctionSignatureType
	down                    migrationFunctionSignatureType
	DbConnectionLabel       string
	DbConnectionDescription string
	DbConnectionMissing     bool
	Processed               bool
	ProcessedTimeUnix       int64
	ProcessedBatch          int64
}

type MigrationsRegisteredType []migrationRegisteredType
type MigrationsProcessedType []migrationDbRecordType
type DatabaseConnectionLabelType struct {
	Label       string
	dbClient    *mongo.Client
	Description string
}

const MigrationAppDefaultDatabase = "migrations"
const MigrationAppDefaultCollection = "migrations"
const SequenceStrictnessNoDuplicates = "NODUPLICATES" //Sequence ids cannot be used more than once, they are unique (like all of us....)
const SequenceStrictnessNoLateComers = "NOLATECOMERS" //The system won't allow processing a sequence smaller then a sequence already processed

const DbConnectionLabelDefault = "*DEFAULT*"

var migrationsRegistered MigrationsRegisteredType
var migrationsProcessed MigrationsProcessedType
var migrationAppMongoClient *mongo.Client
var migrationAppDatabase string
var migrationAppCollection string
var migrationAppDatabaseExists bool
var migrationAppCollectionExists bool

var dbConnectionsBox = make(map[string]DatabaseConnectionLabelType)
var dbConnectionsMissing []string

func RegisterDbConnection(label string, description string, dbClient *mongo.Client) {
	if _, exists := dbConnectionsBox[label]; exists {
		fatalIfError(errors.New(fmt.Sprintf("Connection label [%s] already used", label)))
	}
	dbConnectionsBox[label] = DatabaseConnectionLabelType{Label: label, Description: description, dbClient: dbClient}
	checkIfAllPendingMigrationsConnectionsAreRegistered()
}

func GetConnectionsLabels() []DatabaseConnectionLabelType {
	var l []DatabaseConnectionLabelType
	for _, v := range dbConnectionsBox {
		l = append(l, v)
	}
	return l
}

func GetMigrationAppDatabaseExists() bool {
	return migrationAppDatabaseExists
}

func GetMigrationAppCollectionExists() bool {
	return migrationAppCollectionExists
}

func MigrationEngineInitialise(databaseName string, collectionName string, dbClient *mongo.Client, sequenceStrictness []string) {
	migrationAppDatabase = databaseName
	migrationAppCollection = collectionName
	migrationAppMongoClient = dbClient

	//register system dtabase connections
	RegisterDbConnection(DbConnectionLabelDefault, "This is the same connection used by the migration engine", dbClient)

	//At this point all migrations have been registered
	//so we can order them by sequence instead of the way they have been registered
	orderMigrationsRegisteredBySequence()

	//Implement sequence strictness if required
	checkSequenceStrictness(sequenceStrictness)

	CheckIfDbIsInitialised()
	retrieveMigrationsProcessedFromDb()
	markMigrationsThatArePending()
	checkIfAllPendingMigrationsConnectionsAreRegistered()

}

//check if the connections referenced in the pending migrations are present in the connections list available
func checkIfAllPendingMigrationsConnectionsAreRegistered() {
	dbConnectionsMissing = []string{}
	for k, v := range migrationsRegistered {
		if _, exists := dbConnectionsBox[v.DbConnectionLabel]; !exists {
			//mark the connection as missing in a registered migration no matter if pending or not
			migrationsRegistered[k].DbConnectionMissing = true
			//log the missin db connection only for pending migrations
			if !v.Processed {
				dbConnectionsMissing = append(dbConnectionsMissing, v.DbConnectionLabel)
			}
		} else { //we need to set the value to false because connection could be registered later in the process and we need to update the status of the flags accordingly
			migrationsRegistered[k].DbConnectionMissing = false
		} // end if connection label not found in connection box
	} //end for each registered migration
}

func GetDbConnectionsMissing() []string {
	return dbConnectionsMissing
}

func InitialiseDatabase() {
	if !migrationAppCollectionExists {
		migrationAppMongoClient.Database(migrationAppDatabase).CreateCollection(context.TODO(), migrationAppCollection)
	}
}

func CheckIfDbIsInitialised() bool {
	migrationAppDatabaseExists = databaseExists(migrationAppDatabase)
	if migrationAppDatabaseExists {
		migrationAppCollectionExists = collectionExists(migrationAppDatabase, migrationAppCollection)
	}
	return migrationAppDatabaseExists == true && migrationAppCollectionExists == true
}

func orderMigrationsRegisteredBySequence() {
	sort.Slice(migrationsRegistered, func(i, j int) bool {
		return migrationsRegistered[i].Sequence < migrationsRegistered[j].Sequence
	})
}

func databaseExists(database string) bool {
	list, err := migrationAppMongoClient.ListDatabaseNames(context.TODO(), bson.M{})
	fatalIfError(err)
	for _, v := range list {
		if v == database {
			return true
		}
	}
	return false
}

func markMigrationsThatArePending() {
	for i, m := range migrationsRegistered {
		uniqueId := m.UniqueId
		if exists, ii := checkIfUniqIdPresentInProcesseddMigrations(uniqueId); exists {
			//enrich the registered migration with some info about its processed status
			migrationsRegistered[i].Processed = true
			migrationsRegistered[i].ProcessedTimeUnix = migrationsProcessed[ii].ProcessedTimeUnix
			migrationsRegistered[i].ProcessedBatch = migrationsProcessed[ii].ProcessedBatch
		}
	} //end for loop
}

func retrieveMigrationsProcessedFromDb() {
	if !migrationAppCollectionExists {
		return
	}
	cursor, err := migrationAppMongoClient.Database(migrationAppDatabase).Collection(migrationAppCollection).Find(context.TODO(), bson.M{})
	var record migrationDbRecordType
	fatalIfError(err)
	for cursor.Next(context.TODO()) {
		record = migrationDbRecordType{}
		_ = cursor.Decode(&record)
		migrationsProcessed = append(migrationsProcessed, record)
	}
}

func collectionExists(database string, collection string) bool {
	list, err := migrationAppMongoClient.Database(database).ListCollectionNames(context.TODO(), bson.M{"name": collection})
	fatalIfError(err)

	return len(list) > 0
}

func fatalIfError(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}

func GetMigrationsPendingCount() int {
	//we could retrieve this number in two ways...
	//- difference between migrations registered and migrations Processed
	//or
	//- count the actual migrations marked as "pending"
	//the second option is more reliable and always gives the number of migrations that are going to be Processed
	return len(GetMigrationsPending())
}

func GetMigrationsRegisteredCount() int {
	return len(migrationsRegistered)
}

func GetMigrationsRegistered() MigrationsRegisteredType {
	return migrationsRegistered
}

func GetMigrationsProcessedCount() int {
	return len(migrationsProcessed)
}

func GetMigrationsProcessed() MigrationsProcessedType {
	return migrationsProcessed
}

func GetMigrationsPending() MigrationsRegisteredType {
	var l MigrationsRegisteredType
	for _, record := range migrationsRegistered {
		if !record.Processed {
			l = append(l, record)
		}
	} //end for loop
	return l
}

func RegisterMigration(sequence int64, description string, dbConnLabel string, upFunction migrationFunctionSignatureType, downFunction migrationFunctionSignatureType) {
	var newMigration migrationRegisteredType
	newMigration.Sequence = sequence
	newMigration.Name = description
	newMigration.DbConnectionLabel = dbConnLabel
	newMigration.UniqueId = generateMigrationUniqueId(sequence, description)
	newMigration.up = upFunction
	newMigration.down = downFunction

	//be sure that there are not other migration with the same UniqueId.
	//this is a very rare rare rare possibility and if this happens you should celebrate! it is like winning the lottery
	if exists, i := checkIfUniqIdPresentInRegisteredMigrations(newMigration.UniqueId); exists {
		log.Println("Error while registering migrations, UniqueId collision")
		log.Printf("Another migration already found with the UniqueId %s\n", newMigration.UniqueId)
		log.Printf("Migration #1: %d %s\n", migrationsRegistered[i].Sequence, migrationsRegistered[i].Name)
		log.Printf("Migration #2: %d %s\n", newMigration.Sequence, newMigration.Name)
		log.Println("Please change the Sequence or the description of one of the migrations.")
		log.Println("It should be enough to change just one character.")
		log.Println("---------------------------")
		log.Println("PLEASE NOTE: CHANGE THE MOST RECENT MIGRATION MERGED INTO THE BRANCH, DO NOT CHANGE MIGRATIONS ALREADY DEPLOYED")
		log.Println("DOUBLE CHECK THE GIT BRANCH HISTORY, CHANGING A ALREADY DEPLOYED MIGRATION WILL MOST PROBABLY CAUSE THE END OF THE WORLD")
		log.Println()
		log.Fatalln("GOOD LUCK")
		//execution will be stopped due to the fatal log. bye bye....
	}

	//safe to register the migration....
	migrationsRegistered = append(migrationsRegistered, newMigration)

}

func checkIfUniqIdPresentInRegisteredMigrations(uniqueId string) (bool, int) {
	for k, v := range migrationsRegistered {
		if v.UniqueId == uniqueId {
			return true, k
		}
	} //end for loop
	return false, 0
}

func checkIfUniqIdPresentInProcesseddMigrations(uniqueId string) (bool, int) {
	for k, v := range migrationsProcessed {
		if v.UniqueId == uniqueId {
			return true, k
		}
	} //end for loop
	return false, 0
}

func generateMigrationUniqueId(sequence int64, description string) string {

	x := md5.Sum([]byte(description))
	xShortAndString := fmt.Sprintf("%x", x)[:5]
	return fmt.Sprintf("%d.%s", sequence, xShortAndString)
}

func checkSequenceStrictness(strictness []string) {
	for _, v := range strictness {
		switch v {
		case SequenceStrictnessNoLateComers:
			break
		case SequenceStrictnessNoDuplicates:
			break
		default:
			fatalIfError(errors.New(fmt.Sprintf("sequence strictness [%s] is not valid", v)))
		} //end switch case
	} //end for loop
}

func checkSequenceStrictnessNoLateComers() {
	//todo
}

func checkSequenceStrictnessNoDuplicates() {
	//todo
}

func RunPendingMigrations() error {
	fatalIfDbNotInitialised()
	processedBatch := time.Now().Unix()
	for _, v := range GetMigrationsPending() {
		recordForDb := migrationDbRecordType{}
		startTime := time.Now()
		err := v.up(dbConnectionsBox[v.DbConnectionLabel].dbClient)
		if err != nil {
			niceErr := errors.New(fmt.Sprint("Error while processing migration %s %s (sequence %d) - Migration aborted (original error: %s)", v.UniqueId, v.Name, v.Sequence, err.Error()))
			return niceErr
		}
		timeSpent := time.Since(startTime).Milliseconds()
		recordForDb.Name = v.Name
		recordForDb.UniqueId = v.UniqueId
		recordForDb.Sequence = v.Sequence
		recordForDb.DbConnectionLabel = v.DbConnectionLabel
		recordForDb.ProcessedTimeUnix = time.Now().Unix()
		recordForDb.DbConnectionDescription = v.DbConnectionDescription
		recordForDb.ProcessedTimeSpentMs = timeSpent
		recordForDb.ProcessedBatch = processedBatch
		saveProcessedMigrationToDb(recordForDb)
	}

	return nil
}

func saveProcessedMigrationToDb(record migrationDbRecordType) {
	migrationAppMongoClient.Database(migrationAppDatabase).Collection(migrationAppCollection).InsertOne(context.TODO(), record)
}

func fatalIfDbNotInitialised() {
	if !CheckIfDbIsInitialised() {
		fatalIfError(errors.New("Database not initialised"))
	}
}
