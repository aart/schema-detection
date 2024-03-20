package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func init() {

	rootCmd.PersistentFlags().Bool("local", true, "uses for configuration")
	rootCmd.AddCommand(runCmd)
}

var rootCmd = &cobra.Command{
	Use:   "schemagen",
	Short: "schemagen",
	Long:  `Bigquery schema generator from ndjson input files. More info at: https://github.com/aart/schema-detection`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("For more info _>schemactl help")
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
