package hive

import (
	"encoding/json"
	"fmt"
)

var (
	ENDPOINT_COLUMN        = `%v:%v/templeton/v1/ddl/database/%v/table/:%v/column?user.name=%v`
	ENDPOINT_COLUMN_DETAIL = `%v:%v/templeton/v1/ddl/database/%v/table/:%v/column/%v?user.name=%v`
)

type Column struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Comment string `json:"comment,omitempty"`
}

type ListColumnResponse struct {
	Columns  []Column `json:"columns"`
	Database string   `json:"database"`
	Table    string   `json:"table"`
}

type ShowColumnResponse struct {
	Database string `json:"database"`
	Table    string `json:"table"`
	Column   Column `json:"column"`
}

type CreateColumnResponse struct {
	Column   Column `json:"column"`
	Database string `json:"database"`
	Table    string `json:"table"`
}

func (this *Client) ListColumn(database, table string) (*ListColumnResponse, error) {
	endpoint := fmt.Sprintf(ENDPOINT_COLUMN, this.BaseUrl, this.Port, database, table, this.User)
	resp, err := this.request(HTTP_GET, endpoint, nil)
	if err != nil {
		return nil, err
	}

	res := &ListColumnResponse{}
	if err := json.NewDecoder(resp.Body).Decode(res); err != nil {
		return nil, err
	}

	return res, nil
}

func (this *Client) ShowColumn(database, table, column string) (*ShowColumnResponse, error) {
	endpoint := fmt.Sprintf(ENDPOINT_COLUMN_DETAIL, this.BaseUrl, this.Port, database, table, column, this.User)
	resp, err := this.request(HTTP_GET, endpoint, nil)
	if err != nil {
		return nil, err
	}

	res := &ShowColumnResponse{}
	if err := json.NewDecoder(resp.Body).Decode(res); err != nil {
		return nil, err
	}

	return res, nil
}

func (this *Client) CreateColumn(database, table, column string) (*CreateColumnResponse, error) {
	endpoint := fmt.Sprintf(ENDPOINT_COLUMN_DETAIL, this.BaseUrl, this.Port, database, table, column, this.User)
	resp, err := this.request(HTTP_PUT, endpoint, nil)
	if err != nil {
		return nil, err
	}

	res := &CreateColumnResponse{}
	if err := json.NewDecoder(resp.Body).Decode(res); err != nil {
		return nil, err
	}

	return res, nil
}
