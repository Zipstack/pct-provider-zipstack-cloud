package api

import (
	"encoding/json"
	"fmt"
	"strings"
)

type HypertableAccessControl struct {
	HypertableId string `json:"hypertableId"`
	UserEmail    string `json:"userEmail,omitempty"`
	GroupName    string `json:"groupName,omitempty"`
}

type HypertableAccessControlList struct {
	StatusCode   int                        `json:"statusCode"`
	HypertableId string                     `json:"hypertableId"`
	Users        []accessControlUserOrGroup `json:"users"`
	Groups       []accessControlUserOrGroup `json:"groups"`
}

type accessControlUserOrGroup struct {
	PolicyId         string `json:"policyId"`
	Column           string `json:"column"`
	MaskingOption    string `json:"maskingOption"`
	FilterExpression string `json:"filterExpression"`
	Member           string `json:"member"`
}

func (c *Client) GetHypertableAccessControlStateId(hypertableId string, userOrGroup string) string {
	if hypertableId != "" && userOrGroup != "" {
		return fmt.Sprintf("%s:%s", hypertableId, userOrGroup)
	} else {
		return ""
	}
}

func (c *Client) ParseHypertableAccessControlStateId(stateId string) []string {
	if stateId != "" && strings.Contains(stateId, ":") {
		return strings.Split(stateId, ":")
	} else {
		return []string{}
	}
}

func (c *Client) CreateHypertableAccessControl(payload HypertableAccessControl) (string, error) {
	// logger := fwhelpers.GetLogger()

	method := "POST"
	url := c.Host + "/api/v1/access-control/access"
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

func (c *Client) ReadHypertableAccessControl(id string) (HypertableAccessControlList, error) {
	// logger := fwhelpers.GetLogger()

	method := "GET"
	url := c.Host + "/api/v1/access-control/access/" + id

	b, statusCode, _, _, _, err := c.doRequest(method, url, nil, nil)
	if err != nil {
		return HypertableAccessControlList{}, err
	}

	htacl := HypertableAccessControlList{}
	if statusCode >= 200 && statusCode <= 299 {
		err = json.Unmarshal(b, &htacl)
		return htacl, err
	} else {
		msg, err := c.getAPIError(b)
		if err != nil {
			return htacl, err
		} else {
			return htacl, fmt.Errorf(msg)
		}
	}
}

func (c *Client) DeleteHypertableAccessControl(payload HypertableAccessControl) error {
	// logger := fwhelpers.GetLogger()

	method := "DELETE"
	url := c.Host + "/api/v1/access-control/access"
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
