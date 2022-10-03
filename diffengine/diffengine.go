package diffengine

import (
	"bytes"
	"fmt"
	"log"
	"os"

	"github.com/AJR-VMware/diffdb/options"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
)

var (
	err error

	baseDBConnectionPool *dbconn.DBConn
	testDBConnectionPool *dbconn.DBConn
	mismatches []Mismatch
)

func initializeConnectionPools(baseDbName string, testDbName string) {
	baseDBConnectionPool = dbconn.NewDBConnFromEnvironment(baseDbName)
	testDBConnectionPool = dbconn.NewDBConnFromEnvironment(testDbName)
	baseDBConnectionPool.MustConnect(2)
	testDBConnectionPool.MustConnect(2)
}

func reInitDir() {
	os.RemoveAll(options.WORKING_DIR)
	err := os.Mkdir(options.WORKING_DIR, 0744)
	if err != nil {
		log.Fatal(err, "Could not reinit working directory")
	}
}

type Table struct {
	Schema string
	Name   string
}

type Mismatch struct {
	TableName string
	mismatchDesc string
}

func DiffDB(baseDbName string, testDbName string, workingDir string, failFast bool) {
	// get db connection to basedb and testdb
	initializeConnectionPools(baseDbName, testDbName)

	tableList, foundMismatch := getAndCompareTableList()
	if foundMismatch {
		fmt.Printf("Database %s does not match database %s\n", baseDbName, testDbName)
		for _, mism := range mismatches{
			fmt.Printf("Table: %s --- mismatch: %s\n", mism.TableName, mism.mismatchDesc)
		}
		return
	}
	foundMismatch, matchedTables := compareTables(tableList, workingDir)

	if foundMismatch {
		fmt.Printf("Database %s does not match database %s\n", baseDbName, testDbName)
		fmt.Printf("Found matched data for only %d of %d tables\n", matchedTables, len(tableList))
		for _, mism := range mismatches{
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

func compareData(baseFilePath string, testFilePath string) (bool) {
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

	if bytes.Equal(baseFileContents, testFileContents){
		match = true
	}
	return match
}

func compareTables(tableList []Table, workingDir string) (bool, int) {
	foundMismatch := false
	matchedTables := 0

	for _, table := range tableList {
		reInitDir()
		baseFilePath := fmt.Sprintf(`%s_base_file.gz`, workingDir)
		testFilePath := fmt.Sprintf(`%s_test_file.gz`, workingDir)
		tableFQN := fmt.Sprintf("%s.%s", table.Schema, table.Name)
		copyOutBaseQuery := fmt.Sprintf(`
		COPY %s
		TO PROGRAM  'gzip -c -1 > %s' 
		WITH CSV DELIMITER ',' 
		IGNORE EXTERNAL PARTITIONS;`,
			tableFQN,
			baseFilePath,
		)
		copyOutTestQuery := fmt.Sprintf(`
		COPY %s
		TO PROGRAM  'gzip -c -1 > %s'
		WITH CSV DELIMITER ',' 
		IGNORE EXTERNAL PARTITIONS;`,
			tableFQN,
			testFilePath,
		)

		_, err = baseDBConnectionPool.Exec(copyOutBaseQuery, 1)
		if err != nil {
			// TODO: this mostly only fires on external tables.  Explore options for those with the team
			fmt.Printf("WARNING: Unable to copy out table %s from basedb: %v\n", tableFQN, err)
			continue
		}
		_, err = testDBConnectionPool.Exec(copyOutTestQuery, 1)
		if err != nil {
			fmt.Printf("WARNING: Unable to copy out table %s from testdb: %v\n", tableFQN, err)
			continue
		}

		baseFileStat, err := os.Stat(baseFilePath)
		if err != nil {
			log.Fatalf("Unable to stat file %s: %v", baseFilePath, err)
		}
		testFileStat, err := os.Stat(testFilePath)
		if err != nil {
			log.Fatalf("Unable to stat file %s: %v", testFilePath, err)
		}

		if baseFileStat.Size() != testFileStat.Size() {
			foundMismatch = true
			mismatchDesc := fmt.Sprintf("Table %s has %d bytes in basedb and %d bytes in testdb",
				tableFQN,
				baseFileStat.Size(),
				testFileStat.Size())
			mismatchStruct := Mismatch{tableFQN, mismatchDesc}
			mismatches = append(mismatches, mismatchStruct)
			continue
		}

		if !compareData(baseFilePath, testFilePath){
			foundMismatch = true
			mismatchDesc := fmt.Sprintf("Table %s has a data mismatch", tableFQN)
			mismatchStruct := Mismatch{tableFQN, mismatchDesc}
			mismatches = append(mismatches, mismatchStruct)
			continue
		}
		matchedTables += 1

	}
	return foundMismatch, matchedTables
}
