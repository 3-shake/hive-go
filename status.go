package hive

import (
	"encoding/json"
	"fmt"
	"log"
)

var (
	ENDPOINT_STATUS = `%v:%v/templeton/v1/status?user.name=%v`
)

type StatusResponse struct {
	Status  string `json:"status"`
	Version string `json:"version"`
}

func (this *Client) Status() (*StatusResponse, error) {
	endpoint := fmt.Sprintf(ENDPOINT_STATUS, this.BaseUrl, this.Port, this.User)
	resp, err := this.request(HTTP_GET, endpoint, nil)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	res := &StatusResponse{}
	if err := json.NewDecoder(resp.Body).Decode(res); err != nil {
		log.Println(err)
		return nil, err
	}

	return res, nil
}
