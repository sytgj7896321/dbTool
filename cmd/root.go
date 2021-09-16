package cmd

import (
	"dbTool/myformat"
	"errors"
	"github.com/spf13/cobra"
	"strings"
)

var (
	host             string
	port             string
	username         string
	password         string
	instance         string
	connectionParams string
	Output           string
	rootCmd          = &cobra.Command{
		Use:   "dbTool",
		Short: "dbTool is a free CLI for databases connection",
		Long:  `dbTool is a free CLI for databases connection`,
		Run: func(cmd *cobra.Command, args []string) {
			Error(cmd, args, errors.New("unrecognized command"))
		},
	}
)

func init() {
	for _, v := range dbList {
		CreateDBCmd(v)
	}
	rootCmd.PersistentFlags().StringVar(&host, "host", "127.0.0.1", "Host address")
	rootCmd.PersistentFlags().StringVar(&port, "port", "", "Listen port")
	rootCmd.PersistentFlags().StringVar(&username, "username", "", "Username")
	rootCmd.PersistentFlags().StringVar(&password, "password", "", "Password")
	rootCmd.PersistentFlags().StringVar(&instance, "instance", "", `In mysql and postgres it called 'database'
In dm it called 'schema'`)
	rootCmd.PersistentFlags().StringVar(&Output, "output-type", "normal", "normal|json|csv")
}

func Execute() {
	err := rootCmd.Execute()
	myformat.Error(err, "CLI Init Failed")
	if RDBType != "" {
		count := strings.Count(RDBSql, "?")
		length := len(sqlParams)
		if RDBSql != "" && count == 0 && length == 0 {
			return
		} else if RDBSql != "" && count != 0 {
			relationDB(count, length)
		} else {
			myformat.Error(errors.New(""), "Please use --sql to specify SQL, use 'dbTool "+RDBType+" -h' for more information")
		}
	}
}

func relationDB(count, length int) {
	if length%count != 0 {
		myformat.Error(errors.New(""), "Please check your total parameters, it must be divided by count of SQL's '?'")
	} else {
		for i := 0; i < length/count; i++ {
			var cache []interface{}
			for j := 0; j < count; j++ {
				cache = append(cache, sqlParams[j])
			}
			SQLParamsSliced = append(SQLParamsSliced, cache)
			sqlParams = sqlParams[count:]
		}
	}
}
