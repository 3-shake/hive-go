package hive

import (
	"encoding/json"
	"fmt"
)

var (
	ENDPOINT_PROPERTY        = `%v:%v/templeton/v1/ddl/database/%v/table/:%v/property?user.name=%v`
	ENDPOINT_PROPERTY_DETAIL = `%v:%v/templeton/v1/ddl/database/%v/table/:%v/property/%v?user.name=%v`
)

type Property interface{}

type ListPropertyResponse struct {
	Properties Property `json:"properties"`
	Database   string   `json:"database"`
	Table      string   `json:"table"`
}

type ShowPropertyResponse struct {
	Database string   `json:"database"`
	Table    string   `json:"table"`
	Property Property `json:"property"`
}

type CreatePropertyResponse struct {
	Property Property `json:"property"`
	Database string   `json:"database"`
	Table    string   `json:"table"`
}

func (this *Client) ListProperty(database, table string) (*ListPropertyResponse, error) {
	endpoint := fmt.Sprintf(ENDPOINT_COLUMN, this.BaseUrl, this.Port, database, table, this.User)
	resp, err := this.request(HTTP_GET, endpoint, nil)
	if err != nil {
		return nil, err
	}

	res := &ListPropertyResponse{}
	if err := json.NewDecoder(resp.Body).Decode(res); err != nil {
		return nil, err
	}

	return res, nil
}

func (this *Client) ShowProperty(database, table, property string) (*ShowPropertyResponse, error) {
	endpoint := fmt.Sprintf(ENDPOINT_COLUMN_DETAIL, this.BaseUrl, this.Port, database, table, property, this.User)
	resp, err := this.request(HTTP_GET, endpoint, nil)
	if err != nil {
		return nil, err
	}

	res := &ShowPropertyResponse{}
	if err := json.NewDecoder(resp.Body).Decode(res); err != nil {
		return nil, err
	}

	return res, nil
}

// TODO
func (this *Client) CreateProperty(database, table, property string) (*CreatePropertyResponse, error) {
	endpoint := fmt.Sprintf(ENDPOINT_COLUMN_DETAIL, this.BaseUrl, this.Port, database, table, property, this.User)
	resp, err := this.request(HTTP_PUT, endpoint, nil)
	if err != nil {
		return nil, err
	}

	res := &CreatePropertyResponse{}
	if err := json.NewDecoder(resp.Body).Decode(res); err != nil {
		return nil, err
	}

	return res, nil
}
