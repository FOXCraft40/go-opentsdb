package opentsdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"
	"errors"
)

type Options struct {
	// Host value for the opentsdb server
	// Default: 127.0.0.1
	Endpoint string

	// Timeout for http client
	// Default: no timeout
	Timeout time.Duration

	// Username for basic https auth
	Username string

	// Password for basic https auth
	Password string
}

type Client struct {
	url        *url.URL
	httpClient *http.Client
	tr         *http.Transport
	username   string
	password   string
}

func NewClient(opt Options) (*Client, error) {
	if opt.Endpoint == "" {
		opt.Endpoint = "http://127.0.0.1:4242"
	}

	endpoint := fmt.Sprintf("%s", opt.Endpoint)

	u, err := url.Parse(endpoint)

	if err != nil {
		return nil, err
	}

	tr := &http.Transport{}

	return &Client{
		url: u,
		httpClient: &http.Client{
			Timeout:   opt.Timeout,
			Transport: tr,
		},
		tr:       tr,
		username: opt.Username,
		password: opt.Password,
	}, nil
}


func (c *Client) SetPassword(password string) error {
	c.password = password
	return nil
}


func (c *Client) Close() error {
	c.tr.CloseIdleConnections()
	return nil
}

func (c *Client) Aggregators() error {
	return nil
}

func (c *Client) Annotation() error {
	return nil
}

func (c *Client) Config() error {
	return nil
}

func (c *Client) Dropcaches() error {
	return nil
}

func (c *Client) Put(bp *BatchPoints, params string) ([]byte, error) {
	data, err := bp.ToJson()
	if err != nil {
		return nil, err
	}

	u := c.url
	u.Path = "api/put"
	u.RawQuery = params

	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// If StatusCode 4XX or 5XX -> error
	if resp.StatusCode >= 400 {
		return body, fmt.Errorf(resp.Status)
	}
	
	return body, nil
}

func (c *Client) Query(q *QueryParams) ([]byte, error) {

	data, err := json.Marshal(q)
	if err != nil {
		return nil, err
	}

	body, err := c.ExecRequest("POST", "api/query", data)
	if err != nil {
		return nil, err
	}

	return body, nil

}

func (c *Client) QueryDelete(q *QueryParams) ([]byte, error) {

	data, err := json.Marshal(q)
	if err != nil {
		return nil, err
	}

	body, err := c.ExecRequest("DELETE", "api/query", data)
	if err != nil {
		return nil, err
	}

	return body, nil

}

func (c *Client) Search() error {
	return nil
}

func (c *Client) Serializers() error {
	return nil
}

func (c *Client) Stats() error {
	return nil
}

func (c *Client) Suggest(s *SuggestParams) ([]string, error) {

	data, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}

	body, err := c.ExecRequest("POST", "api/suggest", data)
	if err != nil {
		return nil, err
	}

	values := make([]string, 0)

	json.Unmarshal(body, &values)

	return values, nil

}

func (c *Client) ExecRequest(requestType string, requestPath string, requestParams []byte) ([]byte, error) {

	u := c.url
	u.Path = requestPath

	req, err := http.NewRequest(requestType, u.String(), bytes.NewReader(requestParams))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode > 300 {
		return nil, errors.New(resp.Status)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return body, nil

}

func (c *Client) Tree() error {
	return nil
}

func (c *Client) Uid() error {
	return nil
}

func (c *Client) Version() error {
	return nil
}
