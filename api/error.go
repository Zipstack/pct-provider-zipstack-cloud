package api

import (
	"encoding/json"
	"fmt"
	"strings"
)

type APIError struct {
	Timestamp        string            `json:"timestamp"`
	Status           int16             `json:"status"`
	Message          string            `json:"message"`
	Error            string            `json:"error"`
	ValidationErrors []ValidationError `json:"errors"`
	Path             string            `json:"path"`
}

type ValidationError struct {
	/*
		FIXME Has a heterogeneous structure:

		"arguments": [
			{
				"codes": [
					"meshDbRequest.name",
					"name"
				],
				"arguments": null,
				"defaultMessage": "name",
				"code": "name"
			},
			[],
			{
				"defaultMessage": "^(?=.{3,80}$)[a-zA-Z0-9 ]+$",
				"arguments": null,
				"codes": [
					"^(?=.{3,80}$)[a-zA-Z0-9 ]+$"
				]
			}

		]
	*/
	// Arguments      []Argument `json:"arguments"`

	Codes          []string `json:"codes"`
	DefaultMessage string   `json:"defaultMessage"`
	ObjectName     string   `json:"objectName"`
	Field          string   `json:"field"`
	RejectedValue  string   `json:"rejectedValue"`
	BindingFailure bool     `json:"bindingFailure"`
	Code           string   `json:"code"`
}

// type Argument struct {
// 	Codes          []string   `json:"codes"`
// 	Arguments      []Argument `json:"-"`
// 	DefaultMessage string     `json:"defaultMessage"`
// 	Code           string     `json:"code"`
// }

func (c *Client) getAPIError(body []byte) (string, error) {
	apiErr := APIError{}
	err := json.Unmarshal(body, &apiErr)
	if err != nil {
		return "", fmt.Errorf("content type mismatch or invalid provider api host or path")
	} else {
		slices := strings.Split(apiErr.Message, "at [Source:")
		msg := slices[0]
		if len(apiErr.ValidationErrors) > 0 {
			msg += ", Errors: ["
			for _, ve := range apiErr.ValidationErrors {
				msg += ve.DefaultMessage + ": \"" + ve.RejectedValue + "\", "
			}
			msg = strings.TrimSuffix(msg, ", ")
			msg += "]"
		}
		return strings.TrimSpace(msg), nil
	}
}
