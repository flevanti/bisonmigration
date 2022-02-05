package bisonmigration

var template = `package migrations

import (
	"github.com/flevanti/bisonmigration"
	"go.mongodb.org/mongo-driver/mongo"
)

//
// Please return an error if you want the migration to fail and the migration process to stop.
// Migration failed will continue to be pending ( or won't be rolled back if it was a down process) 
// Don't exit, panic or try any other way to stop the process.
// 
// just return a nice error
//

//
// IMPORTANT FOR SAFETY REASONS AND AVOID STUPID CONFLICTS: DO NOT CREATE EXPORTED FUNCTIONS 
// (translated, create only functions that start with lowercase characters)
//

func up_{{sequence}}(db *mongo.Client) error {
	// Your code here
	return nil
}

func down_{{sequence}}(db *mongo.Client) error {
	//your code here
	return nil
}


//
//
// DON'T TOUCH ANYTHING BEYOND THIS POINT
//
//


//
//this is adding the migration to the migration engine
//
func init() {
	bisonmigration.RegisterMigration({{sequence}}, "{{name}}", "{{connLabel}}", up_{{sequence}}, down_{{sequence}})
}
`
