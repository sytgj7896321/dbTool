package relation

import (
	"database/sql"
	"dbTool/cmd"
	"dbTool/myformat"
	"encoding/json"
	"fmt"
	"strings"
)

func (m *MyDB) Do(modifySql, querySql, dbType string) {
	modify := strings.Split(modifySql, " ")
	query := strings.Split(querySql, " ")
	if modifySql != "" && querySql != "" {
		fmt.Println("Please use --modify or --query, not both")
		return
	} else if modifySql != "" && strings.ToUpper(modify[0]) != "SELECT" {
		m.Exec(modifySql, cmd.SQLParams...)
	} else if querySql != "" && strings.ToUpper(query[0]) == "SELECT" {
		switch cmd.Output {
		case "standard":
			fmt.Println("Standard")
			//m.Query(querySql)
		case "json":
			m.QueryJson(querySql)
		default:
			fmt.Println("Unknown output type")
			return
		}
	} else {
		fmt.Println("Wrong usage, please use 'dbTool " + dbType + " -h' for more information")
		return
	}
}

func (m *MyDB) Exec(sqlString string, args ...interface{}) {
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
	fmt.Println("Exec SQL: " + sqlString + " with SQL params: \"" + argsStr + "\" Success")
	fmt.Printf("LastInsertID = %d, AffectedRows = %d\n", id, affected)
	err = tx.Commit()
	myformat.Error(err, "Transaction Commit Failed")
}

//func (m *MyDB) Query(sqlString string) {
//	rows, err := m.DB.Query(sqlString)
//	defer func(rows *sql.Rows) {
//		err := rows.Close()
//		myformat.Error(err, "Fail to End this Query")
//	}(rows)
//	myformat.Error(err, "Fail to Query this SQL")
//	columns, _ := rows.Columns()
//	count := len(columns)
//	//tableData := make([]map[string]interface{}, 0)
//	values := make([]interface{}, count)
//	//valueP := make([]interface{}, count)
//	for rows.Next() {
//		//for i := 0; i < count; i++ {
//		//	valueP[i] = &values[i]
//		//}
//		for range columns {
//			rows.Scan(values...)
//			//myformat.Printf(values.([]byte))
//		}
//		//	err := rows.Scan()
//		//	if err != nil {
//		//		fmt.Println("Fail to read Query result")
//		//	} else {
//		//		fmt.Printf("%+v\n", )
//		//	}
//	}
//}

func (m *MyDB) QueryJson(sqlString string) {
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

//func (m *MyDB) Query(sqlStr string) {
//	rows, err := m.DB.Query(sqlStr)
//	//函数结束释放链接
//	defer rows.Close()
//	//读出查询出的列字段名
//	cols, _ := rows.Columns()
//	//values是每个列的值，这里获取到byte里
//	values := make([]string, len(cols))
//	//query.Scan的参数，因为每次查询出来的列是不定长的，用len(cols)定住当次查询的长度
//	scans := make([]interface{}, len(cols))
//	//让每一行数据都填充到[][]byte里面,狸猫换太子
//	for i := range values {
//		scans[i] = &values[i]
//	}
//	for rows.Next() {
//		err := rows.Scan(scans...)
//		row := make(map[string]string, 0)
//		for k, v := range values { //每行数据是放在values里面，现在把它挪到row里
//			key := cols[k]
//			row[key] = v
//			fmt.Printf("%s:%s ", cols[k], v)
//			if k == len(cols)-1 {
//				fmt.Printf("\n")
//			}
//		}
//	}
//}
