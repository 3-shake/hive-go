package hive

import (
	"encoding/json"
	"fmt"
)

var (
	ENDPOINT_PARTITION        = `%v:%v/templeton/v1/ddl/database/%v/table/:%v/partition?user.name=%v`
	ENDPOINT_PARTITION_DETAIL = `%v:%v/templeton/v1/ddl/database/%v/table/:%v/partition/%v?user.name=%v`
)

type ListPartitionResponse struct {
	Database string `json:"database"`
	Table    string `json:"table"`

	Partitions []struct {
		Name   string  `json:"name"`
		Values []Value `json:"values"`
	} `json:"Partitions"`
}

type ShowPartitionResponse struct {
	Database  string   `json:"database"`
	Table     string   `json:"table"`
	Partition string   `json:"partition"`
	Columns   []Column `json:"columns"`

	// extend
	Partitioned      bool     `json:"partitioned"`
	Location         string   `json:"location"`
	OutputFormat     string   `json:"outputFormat"`
	Owner            string   `json:"owner"`
	PartitionColumns []Column `json:"partitionColumns"`
	InputFormat      string   `json:"inputFormat"`
}

type CreatePartitionResponse struct {
	Database  string `json:"database"`
	Table     string `json:"table"`
	Partition string `json:"partition"`
}

type DeletePartitionResponse struct {
	Database  string `json:"database"`
	Table     string `json:"table"`
	Partition string `json:"partition"`
}

type Value struct {
	ColumnName  string `json:"columnName"`
	ColumnValue string `json:"columnValue"`
}

func (this *Client) ListPartition(database, table string) (*ListPartitionResponse, error) {
	endpoint := fmt.Sprintf(ENDPOINT_PARTITION, this.BaseUrl, this.Port, database, table, this.User)
	resp, err := this.request(HTTP_GET, endpoint, nil)
	if err != nil {
		return nil, err
	}

	res := &ListPartitionResponse{}
	if err := json.NewDecoder(resp.Body).Decode(res); err != nil {
		return nil, err
	}

	return res, nil
}

func (this *Client) ShowPartition(database, table string) (*ShowPartitionResponse, error) {
	endpoint := fmt.Sprintf(ENDPOINT_PARTITION, this.BaseUrl, this.Port, database, table, this.User)
	resp, err := this.request(HTTP_GET, endpoint, nil)
	if err != nil {
		return nil, err
	}

	res := &ShowPartitionResponse{}
	if err := json.NewDecoder(resp.Body).Decode(res); err != nil {
		return nil, err
	}

	return res, nil
}

func (this *Client) CreatePartition(database, table string, partitions map[string]string) (*CreatePartitionResponse, error) {
	var partition string
	for k, v := range partitions {
		partition += fmt.Sprintf("%s=%s", k, v)
	}

	endpoint := fmt.Sprintf(ENDPOINT_PARTITION, this.BaseUrl, this.Port, database, table, this.User)
	resp, err := this.request(HTTP_PUT, endpoint, nil)
	if err != nil {
		return nil, err
	}

	res := &CreatePartitionResponse{}
	if err := json.NewDecoder(resp.Body).Decode(res); err != nil {
		return nil, err
	}

	return res, nil
}

func (this *Client) DeletePartition(database, table string, partitions map[string]string) (*DeletePartitionResponse, error) {
	var partition string
	for k, v := range partitions {
		partition += fmt.Sprintf("%s=%s", k, v)
	}

	endpoint := fmt.Sprintf(ENDPOINT_PARTITION, this.BaseUrl, this.Port, database, table, this.User)
	resp, err := this.request(HTTP_PUT, endpoint, nil)
	if err != nil {
		return nil, err
	}

	res := &DeletePartitionResponse{}
	if err := json.NewDecoder(resp.Body).Decode(res); err != nil {
		return nil, err
	}

	return res, nil
}
