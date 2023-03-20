package api

import (
	"encoding/json"
	"fmt"
)

type Datasource struct {
	Id                        string   `json:"id,omitempty"`
	LastModifiedDate          string   `json:"lastModifiedDate,omitempty"`
	Name                      string   `json:"name"`
	Description               string   `json:"description"`
	Tags                      []string `json:"tags"`
	Admins                    []string `json:"admins"`
	ShortName                 string   `json:"shortName"`
	ConnectionMetadata        string   `json:"connectionMetadata"`
	DbConnector               string   `json:"dbConnector"`
	DbSubConnector            string   `json:"dbSubConnector"`
	DbSubConnectorDisplayName string   `json:"dbSubConnectorDisplayName"`
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
