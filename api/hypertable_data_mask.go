package api

import (
	"encoding/json"
	"fmt"
	"strings"
)

type HypertableDataMask struct {
	HypertableId  string `json:"hypertableId"`
	UserEmail     string `json:"userEmail,omitempty"`
	GroupName     string `json:"groupName,omitempty"`
	MaskingOption string `json:"maskingOption,omitempty"`
	Column        string `json:"column,omitempty"`
}

type HypertableDataMasks struct {
	StatusCode   int                   `json:"statusCode"`
	HypertableId string                `json:"hypertableId"`
	Users        []dataMaskUserOrGroup `json:"users"`
	Groups       []dataMaskUserOrGroup `json:"groups"`
}

type dataMaskUserOrGroup struct {
	PolicyId      string `json:"policyId"`
	Member        string `json:"member"`
	MaskingOption string `json:"maskingOption"`
	Column        string `json:"column"`
}

func (c *Client) GetHypertableDataMaskStateId(hypertableId string, userOrGroup string, column string) string {
	if hypertableId != "" && userOrGroup != "" && column != "" {
		return fmt.Sprintf("%s:%s:%s", hypertableId, userOrGroup, column)
	} else {
		return ""
	}
}

func (c *Client) ParseHypertableDataMaskStateId(stateId string) []string {
	if stateId != "" && strings.Contains(stateId, ":") {
		return strings.Split(stateId, ":")
	} else {
		return []string{}
	}
}

func (c *Client) CreateHypertableDataMask(payload HypertableDataMask) (string, error) {
	// logger := fwhelpers.GetLogger()

	method := "POST"
	url := c.Host + "/api/v1/access-control/mask"
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

func (c *Client) ReadHypertableDataMask(id string) (HypertableDataMasks, error) {
	// logger := fwhelpers.GetLogger()

	method := "GET"
	url := c.Host + "/api/v1/access-control/mask/" + id

	b, statusCode, _, _, _, err := c.doRequest(method, url, nil, nil)
	if err != nil {
		return HypertableDataMasks{}, err
	}

	htdatamasks := HypertableDataMasks{}
	if statusCode >= 200 && statusCode <= 299 {
		err = json.Unmarshal(b, &htdatamasks)
		return htdatamasks, err
	} else {
		msg, err := c.getAPIError(b)
		if err != nil {
			return htdatamasks, err
		} else {
			return htdatamasks, fmt.Errorf(msg)
		}
	}
}

func (c *Client) DeleteHypertableDataMask(payload HypertableDataMask) error {
	// logger := fwhelpers.GetLogger()

	method := "DELETE"
	url := c.Host + "/api/v1/access-control/mask"
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
