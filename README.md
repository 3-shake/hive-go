# hive-go

## WHAT
Go library for the Hive(WebHCat) API
ref: https://cwiki.apache.org/confluence/display/Hive/WebHCat+Reference

## Example

```
package main

import (
	"fmt"

	hive "github.com/3-shake/hive-go"
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
	fmt.Println(res, err)
}

func showDatabase() {
	res, err := cli.ShowDatabase("sample_hive")
	fmt.Println(res, err)
}

func createDatabase() {
	res, err := cli.CreateDatabase(fmt.Sprintf("sample_hive%d", 100), &hive.CreateDatabaseInput{})
	fmt.Println(res, err)
}

func dropDatabase() {
	res, err := cli.DropDatabase("sample_hive")
	fmt.Println(res, err)
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
	fmt.Println(res, err)
}

```
