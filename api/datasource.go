package api

import (
	"encoding/json"
	"fmt"
)

type Datasource struct {
	Id                        string   `json:"id,omitempty"`
	Name                      string   `json:"name,omitempty"`
	Description               string   `json:"description,omitempty"`
	Tags                      []string `json:"tags,omitempty"`
	Admins                    []string `json:"admins,omitempty"`
	ShortName                 string   `json:"shortName,omitempty"`
	ConnectionMetadata        string   `json:"connectionMetadata,omitempty"`
	DbConnector               string   `json:"dbConnector,omitempty"`
	DbSubConnector            string   `json:"dbSubConnector,omitempty"`
	DbSubConnectorDisplayName string   `json:"dbSubConnectorDisplayName,omitempty"`
	Deleted                   bool     `json:"deleted,omitempty"`
}

func (c *Client) CreateDatasource(payload Datasource) (Datasource, error) {
	// logger := fwhelpers.GetLogger()

	method := "POST"
	url := c.Host + "/api/v1/catalog/meshdb/"
	body, err := json.Marshal(payload)
	if err != nil {
		return Datasource{}, err
	}

	b, statusCode, _, _, _, err := c.doRequest(method, url, body, nil)
	if err != nil {
		return Datasource{}, err
	}

	source := Datasource{}
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

func (c *Client) ReadDatasource(id string) (Datasource, error) {
	// logger := fwhelpers.GetLogger()

	method := "GET"
	url := c.Host + "/api/v1/catalog/meshdb/" + id

	b, statusCode, _, _, _, err := c.doRequest(method, url, nil, nil)
	if err != nil {
		return Datasource{}, err
	}

	source := Datasource{}
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

func (c *Client) UpdateDatasource(id string, payload Datasource) (Datasource, error) {
	// logger := fwhelpers.GetLogger()

	method := "PUT"
	url := c.Host + "/api/v1/catalog/meshdb/" + id
	body, err := json.Marshal(payload)
	if err != nil {
		return Datasource{}, err
	}

	b, statusCode, _, _, _, err := c.doRequest(method, url, body, nil)
	if err != nil {
		return Datasource{}, err
	}

	source := Datasource{}
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

func (c *Client) DeleteDatasource(id string) error {
	// logger := fwhelpers.GetLogger()

	method := "DELETE"
	url := c.Host + "/api/v1/catalog/meshdb/" + id

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
