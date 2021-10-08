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
		relationDB(cmd.SQLParamsSliced, cmd.RDBType)
	}
}

func relationDB(sqlParamsSliced [][]interface{}, rdbType string) {
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
			db.Do(cmd.RDBSql, cmd.Output, rdbType, v)
		}
	} else {
		db.Do(cmd.RDBSql, cmd.Output, rdbType, nil)
	}
}
