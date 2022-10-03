package diffengine

import (
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

func DiffDB(baseDbName string, testDbName string, workingDir string, failFast bool) {

	// get db connection to basedb and testdb
	initializeConnectionPools(baseDbName, testDbName)

	tableList := getAndCompareTableList()
	compareTables(tableList, workingDir)
}

func getAndCompareTableList() []Table {
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
		log.Fatal(fmt.Errorf(
			"data mismatches, found %d tables in basedb, %d tables in testdb",
			baseDBTableCount,
			testDBTableCount),
			"")
	}

	// if list doesn't match, fail out
	return baseDBTables
}

func compareTables(tableList []Table, workingDir string) {
	// iterate through list, copying data from both dbs and comparing.
	// if a difference is found, either store or fail out depending on failfast flag

	for _, table := range tableList {
		reInitDir()
		baseFilePath := fmt.Sprintf(`%s_base_file.gz`, workingDir)
		testFilePath := fmt.Sprintf(`%s_test_file.gz`, workingDir)
		copyOutBaseQuery := fmt.Sprintf(`
		COPY %s.%s
		TO PROGRAM  'gzip -c -1 > %s' 
		WITH CSV DELIMITER ',' 
		IGNORE EXTERNAL PARTITIONS;`,
			table.Schema,
			table.Name,
			baseFilePath,
		)
		copyOutTestQuery := fmt.Sprintf(`
		COPY %s.%s
		TO PROGRAM  'gzip -c -1 > %s'
		WITH CSV DELIMITER ',' 
		IGNORE EXTERNAL PARTITIONS;`,
			table.Schema,
			table.Name,
			testFilePath,
		)

		_, err = baseDBConnectionPool.Exec(copyOutBaseQuery, 1)
		if err != nil {
			log.Fatalf("Unable to copy out table %s.%s from basedb: %v", table.Schema, table.Name, err)
		}
		_, err = testDBConnectionPool.Exec(copyOutTestQuery, 1)
		if err != nil {
			log.Fatalf("Unable to copy out table %s.%s from testdb: %v", table.Schema, table.Name, err)
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
			log.Fatalf("Table %s.%s has %d bytes in baseFile and %d bytes in testFile",
				table.Schema,
				table.Name,
				baseFileStat.Size(),
				testFileStat.Size())
		}
		fmt.Printf("Table %s.%s matches: %d bytes\n", table.Schema, table.Name, baseFileStat.Size())

		// TODO: compare actual byte values or hash to check these

	}
}
