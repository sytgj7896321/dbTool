package cmd

import (
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
	_ "github.com/sytgj7896321/dm"
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
	dbCmd.Flags().StringSliceVar(&sqlParams, "sql-params", sqlParams, `Use with SQL's parameters '?'
--sql-params="v1,v2" --sql-params="v3,v4" ...
or --sql-params="v1,v2,v3,v4,..."`)
	rootCmd.AddCommand(dbCmd)
}

func Join(dbType string) string {
	switch dbType {
	case "mysql":
		if port == "" {
			port = "3306"
		}
		return username + ":" + password + "@tcp(" + host + ":" + port + ")/" + instance + "?" + connectionParams
	case "postgres":
		if port == "" {
			port = "5432"
		}
		return "postgres://" + username + ":" + password + "@" + host + ":" + port + "/" + instance + "?" + connectionParams
	case "dm":
		if port == "" {
			port = "5236"
		}
		if instance == "" {
			instance = "SYSDBA"
		}
		return "dm://" + username + ":" + password + "@" + host + ":" + port + "?" + "schema=" + instance + "&" + connectionParams
	default:
		return ""
	}
}
