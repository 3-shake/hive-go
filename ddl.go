package hive

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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
	body := strings.NewReader(fmt.Sprintf("exec=%s", sql))
	resp, err := this.requestWithoutJSON(HTTP_POST, endpoint, body)
	if err != nil {
		return nil, err
	}

	res := &ExecResponse{}
	if err := json.NewDecoder(resp.Body).Decode(res); err != nil {
		return nil, err
	}
	if res.Exitcode != 0 {
		return nil, errors.New(res.Stderr)
	}

	return res, nil
}
