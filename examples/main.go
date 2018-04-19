package main

import (
	"fmt"

	hive "github.com/3-shake/hive-go"
	"github.com/k0kubun/pp"
)

var cli *hive.Client

func init() {
	cli = hive.New("http://127.0.0.1", "50111", "hive")
}

func main() {
	database()
	table()
}

func table() {
	createTable()
}

func database() {
	createDatabase()
	listDatabase()
	showDatabase()
	dropDatabase()
}

func listDatabase() {
	res, err := cli.ListDatabase()
	pp.Println(res, err)
}

func showDatabase() {
	res, err := cli.ShowDatabase("sample_hive")
	pp.Println(res, err)
}

func createDatabase() {
	res, err := cli.CreateDatabase(fmt.Sprintf("sample_hive%d", 100), &hive.CreateDatabaseInput{})
	pp.Println(res, err)
}

func dropDatabase() {
	res, err := cli.DropDatabase("sample_hive")
	pp.Println(res, err)
}

func createTable() {
	res, err := cli.CreateTable("sample_hive", "sample_table", &hive.CreateTableInput{
		Comment: "test",
		Columns: []hive.Column{
			{"id", "int", ""},
			{"name", "string", ""},
		},
		PartitionBy: []hive.Column{
			{"year", "string", ""},
			{"month", "string", ""},
			{"day", "string", ""},
		},
	})
	pp.Println(res, err)
}
