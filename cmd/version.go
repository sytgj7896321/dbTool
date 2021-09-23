package cmd

import (
	"github.com/spf13/cobra"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "version subcommand show dbTool version info.",

	Run: func(cmd *cobra.Command, args []string) {
		//TODO Version
	},
}
