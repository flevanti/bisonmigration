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
)

type migrationDbRecordType struct {
	Sequence          int64  `bson:"sequence"`
	Name              string `bson:"name"`
	UniqueId          string `bson:"uniqueid"`
	ProcessedTimeUnix int64  `bson:"processedtimeunix"`
	ProcessedBatch    int    `bson:"processedbatch"`
}

type migrationFunctionSignatureType func(db *mongo.Client) error

type migrationRegisteredType struct {
	Sequence          int64
	Name              string
	UniqueId          string
	up                migrationFunctionSignatureType
	down              migrationFunctionSignatureType
	Processed         bool
	ProcessedTimeUnix int64
}

type MigrationsRegisteredType []migrationRegisteredType
type MigrationsProcessedType []migrationDbRecordType

const MigrationAppDefaultDatabase = "migrations"
const MigrationAppDefaultCollection = "migrations"
const SequenceStrictnessNoDuplicates = "NODUPLICATES" //Sequence ids cannot be used more than once, they are unique (like all of us....)
const SequenceStrictnessNoLateComers = "NOLATECOMERS" //The system won't allow processing a sequence smaller then a sequence already processed

var migrationsRegistered MigrationsRegisteredType
var migrationsProcessed MigrationsProcessedType
var migrationAppMongoClient *mongo.Client
var migrationAppDatabase string
var migrationAppCollection string
var migrationAppDatabaseExists bool
var migrationAppCollectionExists bool

func GetMigrationAppDatabaseExists() bool {
	return migrationAppDatabaseExists
}

func GetMigrationAppCollectionExists() bool {
	return migrationAppCollectionExists
}

func MigrationEngineInitialise(databaseName string, collectionName string, dbClient *mongo.Client, sequenceStrictness []string) {
	//At this point all migrations have been registered
	//so we can order them by sequence instead of the way they have been registered
	orderMigrationsRegisteredBySequence()

	//Implement sequence strictness if required
	checkSequenceStrictness(sequenceStrictness)

	migrationAppDatabase = databaseName
	migrationAppCollection = collectionName
	migrationAppMongoClient = dbClient
	migrationAppDatabaseExists = databaseExists(migrationAppDatabase)
	if migrationAppDatabaseExists {
		migrationAppCollectionExists = collectionExists(migrationAppDatabase, migrationAppCollection)
	}
	if migrationAppCollectionExists {
		retrieveMigrationsProcessedFromDb()
	}
	markMigrationsThatArePending()
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
			migrationsRegistered[i].Processed = true
			migrationsRegistered[i].ProcessedTimeUnix = migrationsProcessed[ii].ProcessedTimeUnix
		}
	} //end for loop
}

func retrieveMigrationsProcessedFromDb() {
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
	list, err := migrationAppMongoClient.Database(database).ListCollectionNames(context.TODO(), bson.M{"Name": collection})
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
	return len(getMigrationsPending())
}

func GetMigrationsRegisteredCount() int {
	return len(migrationsRegistered)
}

func GetMigrationsProcessedCount() int {
	return len(migrationsProcessed)
}

func GetMigrationsProcessed() MigrationsProcessedType {
	return migrationsProcessed
}

// please note that there are two versions of this function.
// in the one exported one we remove from the migration record the functions stored
// we don't want to expose them
func GetMigrationsPending() MigrationsRegisteredType {
	return getMigrationsPending()
}

func getMigrationsPending() MigrationsRegisteredType {
	var l MigrationsRegisteredType
	for _, record := range migrationsRegistered {
		if !record.Processed {
			l = append(l, record)
		}
	} //end for loop
	return l
}

func RegisterMigration(sequence int64, description string, upFunction migrationFunctionSignatureType, downFunction migrationFunctionSignatureType) {
	var newMigration migrationRegisteredType
	newMigration.Sequence = sequence
	newMigration.Name = description
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
	return fmt.Sprintf("%d.%x", sequence, x)
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
