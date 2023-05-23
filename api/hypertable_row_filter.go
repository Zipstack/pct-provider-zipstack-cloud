package api

import (
	"encoding/json"
	"fmt"
	"strings"
)

type HypertableRowFilter struct {
	HypertableId string `json:"hypertableId"`
	UserEmail    string `json:"userEmail,omitempty"`
	GroupName    string `json:"groupName,omitempty"`
	SQLCondition string `json:"sqlCondition,omitempty"`
	Column       string `json:"column,omitempty"`
}

type HypertableRowFilters struct {
	StatusCode   int                    `json:"statusCode"`
	HypertableId string                 `json:"hypertableId"`
	Users        []rowFilterUserOrGroup `json:"users"`
	Groups       []rowFilterUserOrGroup `json:"groups"`
}

type rowFilterUserOrGroup struct {
	PolicyId         string `json:"policyId"`
	Member           string `json:"member"`
	FilterExpression string `json:"filterExpression"`
	Column           string `json:"column"`
}

func (c *Client) GetHypertableRowFilterStateId(hypertableId string, userOrGroup string, column string) string {
	if hypertableId != "" && userOrGroup != "" && column != "" {
		return fmt.Sprintf("%s:%s:%s", hypertableId, userOrGroup, column)
	} else {
		return ""
	}
}

func (c *Client) ParseHypertableRowFilterStateId(stateId string) []string {
	if stateId != "" && strings.Contains(stateId, ":") {
		return strings.Split(stateId, ":")
	} else {
		return []string{}
	}
}

func (c *Client) CreateHypertableRowFilter(payload HypertableRowFilter) (string, error) {
	// logger := fwhelpers.GetLogger()

	method := "POST"
	url := c.Host + "/api/v1/access-control/rowFilter"
	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}

	b, statusCode, _, _, _, err := c.doRequest(method, url, body, nil)
	if err != nil {
		return "", err
	}

	if statusCode >= 200 && statusCode <= 299 {
		return string(b), err
	} else {
		msg, err := c.getAPIError(b)
		if err != nil {
			return "", err
		} else {
			return "", fmt.Errorf(msg)
		}
	}
}

func (c *Client) ReadHypertableRowFilter(id string) (HypertableRowFilters, error) {
	// logger := fwhelpers.GetLogger()

	method := "GET"
	url := c.Host + "/api/v1/access-control/rowFilter/" + id

	b, statusCode, _, _, _, err := c.doRequest(method, url, nil, nil)
	if err != nil {
		return HypertableRowFilters{}, err
	}

	htrowfilters := HypertableRowFilters{}
	if statusCode >= 200 && statusCode <= 299 {
		err = json.Unmarshal(b, &htrowfilters)
		return htrowfilters, err
	} else {
		msg, err := c.getAPIError(b)
		if err != nil {
			return htrowfilters, err
		} else {
			return htrowfilters, fmt.Errorf(msg)
		}
	}
}

func (c *Client) DeleteHypertableRowFilter(payload HypertableRowFilter) error {
	// logger := fwhelpers.GetLogger()

	method := "DELETE"
	url := c.Host + "/api/v1/access-control/rowFilter"
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	b, statusCode, _, _, _, err := c.doRequest(method, url, body, nil)
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
