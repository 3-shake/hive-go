package hive

import (
	"encoding/json"
	"fmt"
)

var (
	ENDPOINT_EXEC = `%v:%v/templeton/v1/ddl?user.name=%v`
)

type ExecResponse struct {
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
	Exitcode int    `json:"exitcode"`
}

func (this *Client) Exec(sql string) (*ExecResponse, error) {
	endpoint := fmt.Sprintf(ENDPOINT_EXEC, this.BaseUrl, this.Port, this.User)
	resp, err := this.request(HTTP_GET, endpoint, nil)
	if err != nil {
		return nil, err
	}

	res := &ExecResponse{}
	if err := json.NewDecoder(resp.Body).Decode(res); err != nil {
		return nil, err
	}

	return res, nil
}
