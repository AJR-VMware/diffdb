package main

import (
	"os"

	"github.com/AJR-VMware/diffdb/diffengine"
	"github.com/spf13/cobra"
)

var err error

// flags
const (
	BASE_DB   = "basedb"
	TEST_DB   = "testdb"
	FAIL_FAST = "failfast" // TODO -- add this behavior in diffengine
)

func main() {
	var rootCmd = &cobra.Command{
		Use:     "diffdb",
		Short:   "A simple utility for checking whether data in two Greenplum databases matches",
		Args:    cobra.NoArgs,
		Version: "0.0.1",
		Run: func(cmd *cobra.Command, args []string) {
			baseDbName, _ := cmd.Flags().GetString(BASE_DB)
			testDbName, _ := cmd.Flags().GetString(TEST_DB)
			failFast, _ := cmd.Flags().GetBool(FAIL_FAST)

			diffengine.DiffDB(baseDbName, testDbName, failFast)
		}}
	DoInit(rootCmd)
	if err = rootCmd.Execute(); err != nil {
		os.Exit(2)
	}
}

func DoInit(cmd *cobra.Command) {
	cmd.Flags().String(BASE_DB, "", "The base DB we're comparing against")
	cmd.Flags().String(TEST_DB, "", "The test DB we're comparing")
	cmd.Flags().Bool(FAIL_FAST, false, "If true, quit after first table data difference found")
}
