package hive

import (
	"bytes"
	"encoding/json"
	"fmt"
)

var (
	ENDPOINT_TABLE        = `%v:%v/templeton/v1/ddl/database/%v/table?user.name=%v`
	ENDPOINT_TABLE_DETAIL = `%v:%v/templeton/v1/ddl/database/%v/table/%v?user.name=%v`
	ENDPOINT_TABLE_COPY   = `%v:%v/templeton/v1/ddl/database/%v/table/%v/like/%v?user.name=%v`
)

type ListTableResponse struct {
	Database string   `json:"database"`
	Tables   []string `json:"tables"`
}

type ShowTableResponse struct {
	Columns  []Column `json:"columns"`
	Database string   `json:"database"`
	Table    string   `json:"table"`

	// extend
	Partitioned      bool     `json:"partitioned"`
	Location         string   `json:"location"`
	OutputFormat     string   `json:"outputFormat"`
	Owner            string   `json:"owner"`
	PartitionColumns []Column `json:"partitionColumns"`
	InputFormat      string   `json:"inputFormat"`
}

type RowFormat struct {
	FieldsTerminatedBy string `json:"fieldsTerminatedBy"`
}

type SortedBy []struct {
	ColumnName string `json:"columnName"`
	Order      string `json:"order"`
}

type CreateTableInput struct {
	Comment     string   `json:"comment,omitempty"`
	Columns     []Column `json:"columns,omitempty"`
	PartitionBy []Column `json:"partitionedBy,omitempty"`
	ClusteredBy struct {
		ColumnNames     []string `json:"columnNames"`
		SortedBy        SortedBy `json:"sortedBy"`
		NumberOfBuckets string   `json:"numberOfBuckets"`
	} `json:"clusteredBy"`
	IfNotExists bool        `json:"ifNotExists,omitempty"`
	External    bool        `json:"external,omitempty"`
	Format      interface{} `json:"format,omitempty"`
	Location    string      `json:"location,omitempty"`
}

type CreateTableResponse struct {
	Database string `json:"database"`
	Table    string `json:"table"`
}

type RenameTableInput struct {
	Rename string `json:"rename,omitempty"`
}

type RenameTableResponse struct {
	Database string `json:"database"`
	Table    string `json:"table"`
}

type CopyTableResponse struct {
	Database string `json:"database"`
	Table    string `json:"table"`
}

type UpdateTableResponse struct {
	Database string `json:"database"`
	Table    string `json:"table"`
}

type DeleteTableResponse struct {
	Database string `json:"database"`
	Table    string `json:"table"`
}

func (this *Client) ListTable(database string) (*ListTableResponse, error) {
	endpoint := fmt.Sprintf(ENDPOINT_TABLE, this.BaseUrl, this.Port, database, this.User)
	resp, err := this.request(HTTP_GET, endpoint, nil)
	if err != nil {
		return nil, err
	}

	res := &ListTableResponse{}
	if err := json.NewDecoder(resp.Body).Decode(res); err != nil {
		return nil, err
	}

	return res, nil
}

func (this *Client) ShowTable(database, table string) (*ShowTableResponse, error) {
	endpoint := fmt.Sprintf(ENDPOINT_TABLE_DETAIL, this.BaseUrl, this.Port, database, table, this.User)
	resp, err := this.request(HTTP_GET, endpoint, nil)
	if err != nil {
		return nil, err
	}

	res := &ShowTableResponse{}
	if err := json.NewDecoder(resp.Body).Decode(res); err != nil {
		return nil, err
	}

	return res, nil
}

func (this *Client) CreateTable(database, table string, in *CreateTableInput) (*CreateTableResponse, error) {
	body := new(bytes.Buffer)
	json.NewEncoder(body).Encode(in)
	endpoint := fmt.Sprintf(ENDPOINT_TABLE_DETAIL, this.BaseUrl, this.Port, database, table, this.User)
	resp, err := this.request(HTTP_PUT, endpoint, body)
	if err != nil {
		return nil, err
	}

	res := &CreateTableResponse{}
	if err := json.NewDecoder(resp.Body).Decode(res); err != nil {
		return nil, err
	}

	return res, nil
}

func (this *Client) RenameTable(database, table string, in *RenameTableInput) (*RenameTableResponse, error) {
	body := new(bytes.Buffer)
	json.NewEncoder(body).Encode(in)
	endpoint := fmt.Sprintf(ENDPOINT_TABLE_DETAIL, this.BaseUrl, this.Port, database, table, this.User)
	resp, err := this.request(HTTP_POST, endpoint, body)
	if err != nil {
		return nil, err
	}

	res := &RenameTableResponse{}
	if err := json.NewDecoder(resp.Body).Decode(res); err != nil {
		return nil, err
	}

	return res, nil
}

func (this *Client) CopyTable(database, existingTable, newTable string) (*CopyTableResponse, error) {
	endpoint := fmt.Sprintf(ENDPOINT_TABLE_COPY, this.BaseUrl, this.Port, database, existingTable, newTable, this.User)
	resp, err := this.request(HTTP_POST, endpoint, nil)
	if err != nil {
		return nil, err
	}

	res := &CopyTableResponse{}
	if err := json.NewDecoder(resp.Body).Decode(res); err != nil {
		return nil, err
	}

	return res, nil
}

// TODO: params
func (this *Client) UpdateTable(database, table string) (*UpdateTableResponse, error) {
	endpoint := fmt.Sprintf(ENDPOINT_TABLE_DETAIL, this.BaseUrl, this.Port, database, table, this.User)
	resp, err := this.request(HTTP_PUT, endpoint, nil)
	if err != nil {
		return nil, err
	}

	res := &UpdateTableResponse{}
	if err := json.NewDecoder(resp.Body).Decode(res); err != nil {
		return nil, err
	}

	return res, nil
}

func (this *Client) DeleteTable(database, table string) (*DeleteTableResponse, error) {
	endpoint := fmt.Sprintf(ENDPOINT_TABLE_DETAIL, this.BaseUrl, this.Port, database, table, this.User)
	resp, err := this.request(HTTP_DELETE, endpoint, nil)
	if err != nil {
		return nil, err
	}

	res := &DeleteTableResponse{}
	if err := json.NewDecoder(resp.Body).Decode(res); err != nil {
		return nil, err
	}

	return res, nil
}
