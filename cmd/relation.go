package cmd

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
	_ "github.com/sytgj7896321/dm"
	"strings"
)

var (
	dbList          = []string{"mysql", "postgres", "dm"}
	RDBType         string
	RDBConnection   string
	RDBSql          string
	sqlParams       []string
	SQLParamsSliced [][]interface{}
)

func CreateDBCmd(dbType string) {
	var dbCmd = &cobra.Command{
		Use:   dbType,
		Short: "Use " + dbType + " driver",
		Run: func(cmd *cobra.Command, args []string) {
			RDBConnection = Join(dbType)
			RDBType = dbType
		},
	}
	dbCmd.Flags().StringVar(&RDBSql, "sql", "", "Support DDL, DML, DCL(Transaction has been auto enable) and DQL")
	dbCmd.Flags().StringVar(&connectionParams, "connection-params", "", "Param1=Value1&...&ParamN=ValueN")
	dbCmd.Flags().StringSliceVar(&sqlParams, "sql-params", sqlParams, `Use SQL with '?'
e.g. You have two parameters need to be pass, it could be used like
--sql "SQL with '?'" --sql-params="v1,v2" --sql-params="v3,v4" ...--sql-params="vN,vN+1"
or --sql "SQL with '?'" --sql-params="v1,v2,v3,v4,...,vN,vN+1"`)
	rootCmd.AddCommand(dbCmd)
}

func Join(dbType string) string {
	switch dbType {
	case "mysql":
		if port == "" {
			port = "3306"
		}
		return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?%s", username, password, host, port, instance, connectionParams)
	case "postgres":
		if port == "" {
			port = "5432"
		}
		connectionParams = strings.Replace(connectionParams, "&", " ", -1)
		return fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=disable %s", host, port, instance, username, password, connectionParams)
	case "dm":
		if port == "" {
			port = "5236"
		}
		if instance == "" {
			instance = "SYSDBA"
		}
		return fmt.Sprintf("dm://%s:%s@%s:%s?schema=%s&%s", username, password, host, port, instance, connectionParams)
	default:
		return ""
	}
}
