package relation

import (
	"database/sql"
	"dbTool/myformat"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

func (m *MyDB) Do(sql, outputType string, params []interface{}) {
	sqlType := strings.ToUpper(strings.Split(sql, " ")[0])
	if sqlType == "SELECT" {
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
	cols, err := rows.Columns()
	myformat.Error(err, "Read Columns Failed")
	scans := make([]interface{}, len(cols))
	colsToBeScan := make([]string, len(cols))
	for i := range cols {
		colsToBeScan[i] = cols[i]
		scans[i] = &colsToBeScan[i]
	}
	i := 0
	for rows.Next() {
		err := rows.Scan(scans...)
		myformat.Error(err, "Scan Row Data Failed")
		i++
		fmt.Printf("*************************** %d. row ***************************\n", i)
		for col, v := range colsToBeScan {
			fmt.Printf("%27s:%-27s\n", cols[col], v)
		}
	}
	fmt.Printf("%d rows in set (%v)\n", i, time.Since(start))
}

func (m *MyDB) QueryJson(sqlString string, args ...interface{}) {
	stmt, err := m.DB.Prepare(sqlString)
	myformat.Error(err, "json")
	defer stmt.Close()
	rows, err := stmt.Query()
	myformat.Error(err, "json")
	defer rows.Close()
	columns, err := rows.Columns()
	myformat.Error(err, "json")
	count := len(columns)
	tableData := make([]map[string]interface{}, 0)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)
	for rows.Next() {
		for i := 0; i < count; i++ {
			valuePtrs[i] = &values[i]
		}
		rows.Scan(valuePtrs...)
		entry := make(map[string]interface{})
		for i, col := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			entry[col] = v
		}
		tableData = append(tableData, entry)
	}
	jsonData, err := json.Marshal(tableData)
	myformat.Error(err, "json")
	fmt.Println(string(jsonData))
}
