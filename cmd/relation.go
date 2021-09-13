package cmd

import (
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
	_ "github.com/sytgj7896321/dm"
)

var (
	dbList        = []string{"mysql", "postgres", "dm"}
	RDBType       string
	RDBConnection string
	RDBModify     string
	RDBQuery      string
	sqlParams     string
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
	dbCmd.Flags().StringVar(&RDBModify, "modify", "", `A query without returning any rows, including DDL, DML and DCL
like --modify "INSERT INTO users (uid, sha) VALUES (uuid(), sha1(uuid()));"`)
	dbCmd.Flags().StringVar(&RDBQuery, "query", "", `A query return at least one row, including DQL
like --query "SELECT uid, sha FROM users WHERE id > 100;"`)
	dbCmd.Flags().StringVar(&connectionParams, "connectionParams", "", "Param1=Value1&...&ParamN=ValueN")
	dbCmd.Flags().StringVar(&sqlParams, "sql-params", "", "")
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
		return "postgres://" + username + ":" + password + "@" + host + ":" + port + "/" + instance + "?" + connectionParams
	case "dm":
		return "dm://" + username + ":" + password + "@" + host + ":" + port + "/" + instance + "?" + connectionParams
	default:
		return ""
	}
}
