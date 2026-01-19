package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	outputFile         string
	fanOut             int
	bufferSize         int64
	samplingPercentage int
)

func init() {

	rootCmd.PersistentFlags().StringVar(&outputFile, "output-file", "schema.json", "")
	rootCmd.PersistentFlags().IntVar(&fanOut, "fan-out", 2 "")
	rootCmd.PersistentFlags().Int64Var(&bufferSize, "buffer-size", 1000000, "")
	rootCmd.PersistentFlags().IntVar(&samplingPercentage, "sampling-percentage", 100, "")
	rootCmd.AddCommand(runCmd)
}

var rootCmd = &cobra.Command{
	Use:   "schema-detection",
	Short: "schema-detection",
	Long:  `Bigquery schema generator from ndjson input files. More info at: https://github.com/aart/schema-detection`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("For more info _>schema-detection help")
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
