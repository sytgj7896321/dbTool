package cmd

import (
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
	Column           bool
	Output           string
	SQLParams        []interface{}
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
	rootCmd.PersistentFlags().StringVar(&host, "host", "127.0.0.1", "")
	rootCmd.PersistentFlags().StringVar(&port, "port", "", "")
	rootCmd.PersistentFlags().StringVar(&username, "username", "root", "")
	rootCmd.PersistentFlags().StringVar(&password, "password", "", "")
	rootCmd.PersistentFlags().StringVar(&instance, "instance", "test", "")
	rootCmd.PersistentFlags().BoolVar(&Column, "show-column-name", true, "")
	rootCmd.PersistentFlags().StringVar(&Output, "output", "standard", "")
}

func Execute() {
	rootCmd.Execute()
	if sqlParams != "" {
		args := strings.Split(sqlParams, ",")
		for _, v := range args {
			SQLParams = append(SQLParams, v)
		}
	}
}
