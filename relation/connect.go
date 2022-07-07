package relation

import (
	"database/sql"
	"dbTool/cmd"
	"time"
)

type MyDB struct {
	DB *sql.DB
}

func (m *MyDB) OpenDB() error {
	var err error
	m.DB, err = sql.Open(cmd.RDBType, cmd.RDBConnection)
	m.DB.SetConnMaxLifetime(time.Hour)
	m.DB.SetConnMaxIdleTime(3 * time.Minute)
	m.DB.SetMaxOpenConns(100)
	m.DB.SetMaxIdleConns(5)
	return err
}
