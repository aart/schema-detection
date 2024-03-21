package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	outputFile string
	fanOut     int
	bufferSize int64
)

func init() {

	rootCmd.PersistentFlags().StringVar(&outputFile, "output-file", "schema.json", "todo")
	rootCmd.PersistentFlags().IntVar(&fanOut, "fan-out", 5, "todo")
	rootCmd.PersistentFlags().Int64Var(&bufferSize, "buffer-size", 1000000, "todo")
	rootCmd.AddCommand(runCmd)
}

var rootCmd = &cobra.Command{
	Use:   "schemagen",
	Short: "schemagen",
	Long:  `Bigquery schema generator from ndjson input files. More info at: https://github.com/aart/schema-detection`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("For more info _>schemagen help")
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
