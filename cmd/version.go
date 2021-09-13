package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "version subcommand show git version info.",

	Run: func(cmd *cobra.Command, args []string) {
		output, err := ExecuteCommand("git", "version", args...)
		if err != nil {
			Error(cmd, args, err)
		}

		fmt.Fprint(os.Stdout, output)
	},
}
var cloneCmd = &cobra.Command{
	Use:   "clone",
	Short: "clone subcommand will call git clone function.",

	Run: func(cmd *cobra.Command, args []string) {
		output, err := ExecuteCommand("git", "clone", args...)
		if err != nil {
			Error(cmd, args, err)
		}

		fmt.Fprint(os.Stdout, output)
	},
}
