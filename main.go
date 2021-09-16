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
		relationDB(cmd.SQLParamsSliced)
	}
}

func relationDB(sqlParamsSliced [][]interface{}) {
	db := &relation.MyDB{}
	err := db.OpenDB()
	myformat.Error(err, "Fail to open database connection")
	defer func(DB *sql.DB) {
		err := DB.Close()
		myformat.Error(err, "Fail to close database connection")
	}(db.DB)
	err = db.DB.Ping()
	myformat.Error(err, "Fail to connect database")
	if sqlParamsSliced != nil {
		for _, v := range sqlParamsSliced {
			db.Do(cmd.RDBSql, cmd.Output, v)
		}
	} else {
		db.Do(cmd.RDBSql, cmd.Output, nil)
	}
}
