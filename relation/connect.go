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
	m.DB.SetConnMaxLifetime(-1 * time.Minute)
	m.DB.SetConnMaxIdleTime(time.Minute)
	m.DB.SetMaxOpenConns(100)
	m.DB.SetMaxIdleConns(100)
	return err
}
