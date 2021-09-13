package main

import (
	"database/sql"
	"dbTool/cmd"
	"dbTool/myformat"
	"dbTool/relation"
)

func main() {
	cmd.Execute()
	if cmd.RDBType != "" {
		relationDB()
	}
}

func relationDB() {
	db := &relation.MyDB{}
	err := db.OpenDB()
	myformat.Error(err, "Fail to open database connection")
	defer func(DB *sql.DB) {
		err := DB.Close()
		myformat.Error(err, "Fail to close database connection")
	}(db.DB)
	err = db.DB.Ping()
	myformat.Error(err, "Fail to connect database")
	db.Do(cmd.RDBModify, cmd.RDBQuery, cmd.RDBType)
}
