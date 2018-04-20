package hive

import (
	"errors"
	"io"
	"log"
	"net/http"
)

var (
	HTTP_GET    = "GET"
	HTTP_POST   = "POST"
	HTTP_PUT    = "PUT"
	HTTP_DELETE = "DELETE"
)

type Client struct {
	BaseUrl string
	Port    string
	User    string
	Timeout int
}

func New(baseurl, port, user string) *Client {
	svc := &Client{
		BaseUrl: baseurl,
		Port:    port,
		User:    user,
		Timeout: 100000,
	}
	return svc
}

func (this *Client) request(method, endpoint string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(method, endpoint, body)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	if 400 < resp.StatusCode {
		return nil, errors.New(resp.Status)
	}

	return resp, nil
}
