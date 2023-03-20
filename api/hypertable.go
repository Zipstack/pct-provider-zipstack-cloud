package api

import (
	"encoding/json"
	"fmt"
)

type Hypertable struct {
	Id               string   `json:"id,omitempty"`
	LastModifiedDate string   `json:"lastModifiedDate,omitempty"`
	Name             string   `json:"name"`
	Description      string   `json:"description"`
	ShortName        string   `json:"shortName"`
	Tags             []string `json:"tags"`
	Admins           []string `json:"admins"`
	RefreshMode      string   `json:"refreshMode"`
	SqlSelect        string   `json:"sqlSelect"`
	Deleted          bool     `json:"deleted,omitempty"`
}

func (c *Client) CreateHypertable(payload Hypertable) (Hypertable, error) {
	// logger := fwhelpers.GetLogger()

	method := "POST"
	url := c.Host + "/api/v1/catalog/hypertable/"
	body, err := json.Marshal(payload)
	if err != nil {
		return Hypertable{}, err
	}

	b, statusCode, _, _, _, err := c.doRequest(method, url, body, nil)
	if err != nil {
		return Hypertable{}, err
	}

	source := Hypertable{}
	if statusCode >= 200 && statusCode <= 299 {
		err = json.Unmarshal(b, &source)
		return source, err
	} else {
		msg, err := c.getAPIError(b)
		if err != nil {
			return source, err
		} else {
			return source, fmt.Errorf(msg)
		}
	}
}

func (c *Client) ReadHypertable(id string) (Hypertable, error) {
	// logger := fwhelpers.GetLogger()

	method := "GET"
	url := c.Host + "/api/v1/catalog/hypertable/" + id

	b, statusCode, _, _, _, err := c.doRequest(method, url, nil, nil)
	if err != nil {
		return Hypertable{}, err
	}

	source := Hypertable{}
	if statusCode >= 200 && statusCode <= 299 {
		err = json.Unmarshal(b, &source)
		return source, err
	} else {
		msg, err := c.getAPIError(b)
		if err != nil {
			return source, err
		} else {
			return source, fmt.Errorf(msg)
		}
	}
}

func (c *Client) UpdateHypertable(id string, payload Hypertable) (Hypertable, error) {
	// logger := fwhelpers.GetLogger()

	method := "PUT"
	url := c.Host + "/api/v1/catalog/hypertable/" + id
	body, err := json.Marshal(payload)
	if err != nil {
		return Hypertable{}, err
	}

	b, statusCode, _, _, _, err := c.doRequest(method, url, body, nil)
	if err != nil {
		return Hypertable{}, err
	}

	source := Hypertable{}
	if statusCode >= 200 && statusCode <= 299 {
		err = json.Unmarshal(b, &source)
		return source, err
	} else {
		msg, err := c.getAPIError(b)
		if err != nil {
			return source, err
		} else {
			return source, fmt.Errorf(msg)
		}
	}
}

func (c *Client) DeleteHypertable(id string) error {
	// logger := fwhelpers.GetLogger()

	method := "DELETE"
	url := c.Host + "/api/v1/catalog/hypertable/" + id

	b, statusCode, _, _, _, err := c.doRequest(method, url, nil, nil)
	if err != nil {
		return err
	}

	if statusCode >= 200 && statusCode <= 299 {
		return nil
	} else {
		msg, err := c.getAPIError(b)
		if err != nil {
			return err
		} else {
			return fmt.Errorf(msg)
		}
	}
}
