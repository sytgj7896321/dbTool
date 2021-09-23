package relation

import (
	"database/sql"
	"dbTool/myformat"
	"fmt"
	"github.com/imroc/biu"
	"os"
	"strings"
	"time"
)

func (m *MyDB) Do(sql, outputType string, params []interface{}) {
	sqlType := strings.ToUpper(strings.Split(sql, " ")[0])
	if sqlType == "SELECT" || sqlType == "SHOW" {
		switch outputType {
		case "normal":
			m.Query(sql, params...)
		case "json":
			fmt.Println("In plan")
			os.Exit(0)
		case "csv":
			fmt.Println("In plan")
			os.Exit(0)
		default:
			fmt.Println("Unknown Output Type")
			os.Exit(1)
		}
	} else {
		m.Exec(sql, sqlType, params...)
	}
}

func (m *MyDB) Exec(sqlString, sqlType string, args ...interface{}) {
	start := time.Now()
	tx, err := m.DB.Begin()
	myformat.Error(err, "Begin Transaction")
	stmt, err := tx.Prepare(sqlString)
	myformat.Error(err, "Please Check your SQL")
	defer func(stmt *sql.Stmt) {
		err := stmt.Close()
		myformat.Error(err, "Fail to close Transaction")
	}(stmt)
	res, err := stmt.Exec(args...)
	myformat.Error(err, "Fail to Exec the SQL")
	id, err := res.LastInsertId()
	myformat.Error(err, "Get LastInsertID Failed")
	affected, err := res.RowsAffected()
	myformat.Error(err, "Get Affected Rows Failed")
	var argsStr string
	for _, arg := range args {
		argsStr = argsStr + arg.(string) + " "
	}
	argsStr = strings.TrimRight(argsStr, " ")
	if argsStr != "" {
		fmt.Printf("Exec SQL: %s with SQL params: [%s] Success\n", sqlString, argsStr)
	} else {
		fmt.Printf("Exec SQL: %s Success\n", sqlString)
	}
	if sqlType == "INSERT" {
		fmt.Printf("LastInsertID = %d, AffectedRows = %d (%v)\n", id, affected, time.Since(start))
	} else {
		fmt.Printf("AffectedRows = %d (%v)\n", affected, time.Since(start))
	}
	err = tx.Commit()
	myformat.Error(err, "Transaction Commit Failed")
}

func (m *MyDB) Query(sqlStr string, args ...interface{}) {
	start := time.Now()
	stmt, err := m.DB.Prepare(sqlStr)
	myformat.Error(err, "Please Check your SQL")

	rows, err := stmt.Query(args...)
	defer func(rows *sql.Rows) {
		err := rows.Close()
		myformat.Error(err, "Close Query Failed")
	}(rows)
	myformat.Error(err, "Query SQL Failed")

	types, _ := rows.ColumnTypes()
	longestCol, asterisk := FindLongestColAndMakeAsterisk(types)

	scans := make([]interface{}, len(types))
	cols := make([]interface{}, len(types))
	for i := range types {
		cols[i] = types[i].Name()
		scans[i] = &cols[i]
	}

	rowCount := 0
	for rows.Next() {
		err := rows.Scan(scans...)
		myformat.Error(err, "Scan Row Data Failed")
		rowCount++
		fmt.Printf("%s %d. row %s\n", asterisk, rowCount, asterisk)
		for i, v := range cols {
			if v != nil {
				DataOutput(types, i, longestCol, v)
			} else {
				fmt.Printf("%*s:%s\n", longestCol, types[i].Name(), "null")
			}
		}
	}
	fmt.Printf("%d rows in set (%v)\n", rowCount, time.Since(start))
}

func FindLongestColAndMakeAsterisk(types []*sql.ColumnType) (int, string) {
	var longestCol int
	for _, v := range types {
		if len(v.Name()) >= longestCol {
			longestCol = len(v.Name())
		}
	}
	asterisk := make([]byte, longestCol)
	for i := range asterisk {
		asterisk[i] = '*'
	}
	return longestCol, string(asterisk)
}

func DataOutput(types []*sql.ColumnType, i, longestCol int, v interface{}) {
	switch types[i].DatabaseTypeName() {
	case "BIGINT":
		switch v.(type) {
		case []uint8:
			fmt.Printf("%*s:%s\n", longestCol, types[i].Name(), v)
		case int64:
			fmt.Printf("%*s:%d\n", longestCol, types[i].Name(), v)
		}
	case "BINARY":
		fmt.Printf("%*s:Not support Binary type\n", longestCol, types[i].Name())
	case "BIT":
		cache1 := v.([]uint8)
		fmt.Printf("%*s:", longestCol, types[i].Name())
		for _, u := range v.([]uint8) {
			if u == 0 {
				cache1 = cache1[1:]
			} else {
				break
			}
		}
		cache2 := ""
		for _, u := range cache1 {
			cache2 = cache2 + biu.ToBinaryString(u)
		}
		cache2 = strings.TrimLeft(cache2, "0")
		fmt.Printf("%s\n", cache2)
	case "BLOB":
		fmt.Printf("%*s:Not support Blob type\n", longestCol, types[i].Name())
	case "CHAR":
		fmt.Printf("%*s:%s\n", longestCol, types[i].Name(), v)
	case "DATE":
		fmt.Printf("%*s:%s\n", longestCol, types[i].Name(), v)
	case "DATETIME":
		fmt.Printf("%*s:%s\n", longestCol, types[i].Name(), v)
	case "DECIMAL":
		fmt.Printf("%*s:%s\n", longestCol, types[i].Name(), v)
	case "DOUBLE":
		fmt.Printf("%*s:%g\n", longestCol, types[i].Name(), v)
	case "FLOAT":
		fmt.Printf("%*s:%g\n", longestCol, types[i].Name(), v)
	case "GEOMETRY":
		fmt.Printf("%*s:Not support Geometry type\n", longestCol, types[i].Name())
	case "INT":
		fmt.Printf("%*s:%d\n", longestCol, types[i].Name(), v)
	case "JSON":
		fmt.Printf("%*s:%s\n", longestCol, types[i].Name(), v)
	case "MEDIUMINT":
		fmt.Printf("%*s:%d\n", longestCol, types[i].Name(), v)
	case "SMALLINT":
		fmt.Printf("%*s:%d\n", longestCol, types[i].Name(), v)
	case "TEXT":
		fmt.Printf("%*s:%s\n", longestCol, types[i].Name(), v)
	case "TIME":
		fmt.Printf("%*s:%s\n", longestCol, types[i].Name(), v)
	case "TIMESTAMP":
		fmt.Printf("%*s:%s\n", longestCol, types[i].Name(), v)
	case "TINYINT":
		fmt.Printf("%*s:%d\n", longestCol, types[i].Name(), v)
	case "VARBINARY":
		fmt.Printf("%*s:Not support Varbinary type\n", longestCol, types[i].Name())
	case "VARCHAR":
		fmt.Printf("%*s:%s\n", longestCol, types[i].Name(), v)
	case "YEAR":
		fmt.Printf("%*s:%d\n", longestCol, types[i].Name(), v)
	default:
		fmt.Printf("%*s:Unknown Data Type\n", longestCol, types[i].Name())
	}
}
