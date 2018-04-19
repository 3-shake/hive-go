package hive

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
)

var (
	ENDPOINT_DATABASE        = `%v:%v/templeton/v1/ddl/database?user.name=%v`
	ENDPOINT_DATABASE_DETAIL = `%v:%v/templeton/v1/ddl/database/%v?user.name=%v`
)

type ListDatabaseResponse struct {
	Databases []string `json:"databases"`
}

type ShowDatabaseResponse struct {
	Location string `json:"location"`
	Params   string `json:"params"`
	Comment  string `json:"comment"`
	Database string `json:"database"`
}

type CreateDatabaseInput struct {
	Comment    string      `json:"comment"`
	Location   string      `json:"location"`
	Properties interface{} `json:"properties"`
}

type CreateDatabaseResponse struct {
	Database string `json:"database"`
}

type DropDatabaseResponse struct {
	Database string `json:"database"`
}

func (this *Client) ListDatabase() (*ListDatabaseResponse, error) {
	endpoint := fmt.Sprintf(ENDPOINT_DATABASE, this.BaseUrl, this.Port, this.User)
	resp, err := this.request(HTTP_GET, endpoint, nil)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	res := &ListDatabaseResponse{}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	if err := json.Unmarshal(b, res); err != nil {
		log.Println(err)
		return nil, err
	}

	return res, nil
}

func (this *Client) ShowDatabase(database string) (*ShowDatabaseResponse, error) {
	endpoint := fmt.Sprintf(ENDPOINT_DATABASE_DETAIL, this.BaseUrl, this.Port, database, this.User)
	resp, err := this.request(HTTP_GET, endpoint, nil)
	if err != nil {
		return nil, err
	}

	res := &ShowDatabaseResponse{}
	if err := json.NewDecoder(resp.Body).Decode(res); err != nil {
		log.Println(err)
		return nil, err
	}

	return res, nil
}

func (this *Client) CreateDatabase(database string, in *CreateDatabaseInput) (*CreateDatabaseResponse, error) {
	body := new(bytes.Buffer)
	json.NewEncoder(body).Encode(in)
	endpoint := fmt.Sprintf(ENDPOINT_DATABASE_DETAIL, this.BaseUrl, this.Port, database, this.User)
	resp, err := this.request(HTTP_PUT, endpoint, body)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	res := &CreateDatabaseResponse{}
	if err := json.NewDecoder(resp.Body).Decode(res); err != nil {
		log.Println(err)
		return nil, err
	}

	// b, err := ioutil.ReadAll(resp.Body)
	// pp.Println(string(b))
	// if err != nil {
	// log.Println(err)
	// return nil, err
	// }

	return res, nil
}

func (this *Client) DropDatabase(database string) (*DropDatabaseResponse, error) {
	endpoint := fmt.Sprintf(ENDPOINT_DATABASE_DETAIL, this.BaseUrl, this.Port, database, this.User)
	resp, err := this.request(HTTP_DELETE, endpoint, nil)
	if err != nil {
		return nil, err
	}

	res := &DropDatabaseResponse{}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	if err := json.Unmarshal(b, res); err != nil {
		log.Println(err)
		return nil, err
	}

	return res, nil
}
