package hive

import (
	"errors"
	"io"
	"log"
	"net/http"
	"time"
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
	Timeout time.Duration
}

func New(baseurl, port, user string) *Client {
	svc := &Client{
		BaseUrl: baseurl,
		Port:    port,
		User:    user,
		Timeout: 60 * time.Second,
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
	cli := http.Client{Timeout: this.Timeout}
	resp, err := cli.Do(req)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	if 400 < resp.StatusCode {
		return nil, errors.New(resp.Status)
	}

	return resp, nil
}

func (this *Client) requestWithoutJSON(method, endpoint string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(method, endpoint, body)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	cli := http.Client{Timeout: this.Timeout}
	resp, err := cli.Do(req)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	if 400 < resp.StatusCode {
		return nil, errors.New(resp.Status)
	}
	return resp, nil
}
