package main

import (
	"log"
	"os"

	"github.com/AJR-VMware/diffdb/diffengine"
	"github.com/AJR-VMware/diffdb/options"
	"github.com/spf13/cobra"
)

var err error

func main() {
	var rootCmd = &cobra.Command{
		Use:     "diffdb",
		Short:   "A simple utility for checking whether data in two Greenplum databases matches",
		Args:    cobra.NoArgs,
		Version: "0.0.1",
		Run: func(cmd *cobra.Command, args []string) {
			workingDir, _ := cmd.Flags().GetString(options.WORKING_DIR)
			baseDbName, _ := cmd.Flags().GetString(options.BASE_DB)
			testDbName, _ := cmd.Flags().GetString(options.TEST_DB)
			failFast, _ := cmd.Flags().GetBool(options.FAIL_FAST)

			defer DoCleanup(workingDir)
			DoSetup(workingDir)
			diffengine.DiffDB(baseDbName, testDbName, workingDir, failFast)
		}}
	DoInit(rootCmd)
	if err = rootCmd.Execute(); err != nil {
		os.Exit(2)
	}
}

func DoInit(cmd *cobra.Command) {
	cmd.Flags().String(options.BASE_DB, "", "The base DB we're comparing against")
	cmd.Flags().String(options.TEST_DB, "", "The test DB we're comparing")
	cmd.Flags().String(options.WORKING_DIR, "", "The working directory we'll make and delete for storing test data")
	cmd.Flags().Bool(options.FAIL_FAST, false, "If true, quit after first table data difference found")
}

func DoSetup(workingDir string) {
	DoCleanup(workingDir)
	err := os.Mkdir(workingDir, 0744)
	if err != nil {
		log.Fatal(err, "Could not set up working directory")
	}

}

func DoCleanup(workingDir string) {
	err := os.RemoveAll(workingDir)
	if err != nil {
		log.Fatal(err, "Could not clean up working directory")
	}
}
