package diffengine

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
)

var (
	err error

	baseDBConnectionPool *dbconn.DBConn
	testDBConnectionPool *dbconn.DBConn
	mismatches           []Mismatch
)

func initializeConnectionPools(baseDbName string, testDbName string) {
	baseDBConnectionPool = dbconn.NewDBConnFromEnvironment(baseDbName)
	testDBConnectionPool = dbconn.NewDBConnFromEnvironment(testDbName)
	baseDBConnectionPool.MustConnect(2)
	testDBConnectionPool.MustConnect(2)
}

type Table struct {
	Schema string
	Name   string
}

type Mismatch struct {
	TableName    string
	mismatchDesc string
}

type RowCount struct {
	rowCount int64
}

func DiffDB(baseDbName string, testDbName string, failFast bool) {
	// get db connection to basedb and testdb
	initializeConnectionPools(baseDbName, testDbName)

	tableList, foundMismatch := getAndCompareTableList()
	if foundMismatch {
		fmt.Printf("Database %s does not match database %s\n", baseDbName, testDbName)
		for _, mism := range mismatches {
			fmt.Printf("Table: %s --- mismatch: %s\n", mism.TableName, mism.mismatchDesc)
		}
		return
	}
	foundMismatch, matchedTables := compareRowCounts(tableList)

	if foundMismatch {
		fmt.Printf("Database %s does not match database %s\n", baseDbName, testDbName)
		fmt.Printf("Found matched data for only %d of %d tables\n", matchedTables, len(tableList))
		for _, mism := range mismatches {
			fmt.Printf("Table: %s --- mismatch: %s\n", mism.TableName, mism.mismatchDesc)
		}
		return
	}
	fmt.Printf("Database %s matches database %s\n", baseDbName, testDbName)
}

func getAndCompareTableList() ([]Table, bool) {
	foundMismatch := false
	// get list of tables from basedb
	tableListQuery := `
	SELECT 
		tb.table_schema as Schema,
		tb.table_name as Name
	FROM   
		information_schema.tables tb
	WHERE  
		tb.table_schema not in ('pg_catalog', 'gp_toolkit', 'information_schema', 'pg_toast', 'pg_aoseg')
		AND tb.table_type='BASE TABLE'
	ORDER BY
		tb.table_schema,
		tb.table_name`

	baseDBTables := make([]Table, 0)
	err = baseDBConnectionPool.Select(&baseDBTables, tableListQuery)
	if err != nil {
		log.Fatal(err, "Could not pull table list from base db")
	}
	baseDBTableCount := len(baseDBTables)

	testDBTables := make([]Table, 0)
	err = testDBConnectionPool.Select(&testDBTables, tableListQuery)
	if err != nil {
		log.Fatal(err, "Could not pull table list from base db")
	}
	testDBTableCount := len(testDBTables)

	if baseDBTableCount != testDBTableCount {
		foundMismatch = true
		mismatchDesc := fmt.Sprintf(
			"Found %d tables in basedb, %d tables in testdb",
			baseDBTableCount,
			testDBTableCount)

		mismatchStruct := Mismatch{"Table Count", mismatchDesc}
		mismatches = append(mismatches, mismatchStruct)
		return baseDBTables, foundMismatch
	}

	// if list doesn't match, fail out
	return baseDBTables, foundMismatch
}

func compareData(baseFilePath string, testFilePath string) bool {
	// TODO: make this smarter and faster so it doesn't need to read the whole table into memory at once
	match := false
	baseFileContents, err := os.ReadFile(baseFilePath)
	if err != nil {
		log.Fatalf("Could not read %s contents: %v", baseFilePath, err)
	}
	testFileContents, err := os.ReadFile(testFilePath)
	if err != nil {
		log.Fatalf("Could not read %s contents: %v", testFilePath, err)
	}

	if bytes.Equal(baseFileContents, testFileContents) {
		match = true
	}
	return match
}

func compareRowCounts(tableList []Table) (bool, int) {
	foundMisMatch := false
	matchedTables := 0

	for _, table := range tableList {
		testRowCounts := make([]int64, 0)
		baseRowCounts := make([]int64, 0)
		tableFQN := fmt.Sprintf("quote_ident('%s.%s')", strings.ReplaceAll(table.Schema, "'", "''"), strings.ReplaceAll(table.Name, "'", "''"))
		rowCountQuery := fmt.Sprintf("SELECT count(*) as rowCount FROM %s", tableFQN)

		err = testDBConnectionPool.Select(&testRowCounts, rowCountQuery)
		if err != nil {
			// TODO: this mostly only fires on external tables.  Explore options for those with the team
			fmt.Printf("WARNING: Unable to select rowcount of table %s from testdb: %v\n", tableFQN, err)
			continue
		}
		testRowCount := testRowCounts[0]

		err = baseDBConnectionPool.Select(&baseRowCounts, rowCountQuery)
		if err != nil {
			// TODO: this mostly only fires on external tables.  Explore options for those with the team
			fmt.Printf("WARNING: Unable to select rowcount of table %s from basedb: %v\n", tableFQN, err)
			continue
		}
		baseRowCount := baseRowCounts[0]

		if testRowCount != baseRowCount {
			mismatchDesc := fmt.Sprintf("Table %s has %d bytes in basedb and %d bytes in testdb",
				tableFQN,
				baseRowCount,
				testRowCount)
			mismatchStruct := Mismatch{tableFQN, mismatchDesc}
			mismatches = append(mismatches, mismatchStruct)
			continue
		}

		// table rowcounts matched successfully
		matchedTables += 1
	}

	return foundMisMatch, matchedTables
}
