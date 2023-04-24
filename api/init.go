package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type Client struct {
	HTTPClient       *http.Client `json:"-"`
	Host             string       `json:"-"`
	OrganisationName string       `json:"organisationname"`
	Email            string       `json:"email"`
	Password         string       `json:"password"`
	SessionCookie    string       `json:"-"`
	TokenCookie      string       `json:"-"`
	TokenHeader      string       `json:"-"`
	Session          string       `json:"-"`
	Token            string       `json:"-"`
}

func NewClient(host string, orgname string, email string, password string) (*Client, error) {
	c := Client{
		HTTPClient: &http.Client{
			Timeout: time.Duration(120) * time.Second,
		},
		Host:             host,
		OrganisationName: orgname,
		Email:            email,
		Password:         password,
		SessionCookie:    "SESSION",
		TokenCookie:      "XSRF-TOKEN",
		TokenHeader:      "X-XSRF-TOKEN",
		Session:          "",
		Token:            "",
	}
	return &c, nil
}

func (c *Client) doRequest(method string, url string, body []byte, headers map[string]string) ([]byte, int, string, map[string][]string, map[string]string, error) {
	// Attempt login (for non-login requests only), if token is unset.
	if !strings.Contains(url, "/login") {
		if c.Session == "" || c.Token == "" {
			err := c.doLogin()
			if err != nil {
				return nil, 500, "500 Internal Server Error", nil, nil, err
			}
		}
	}

	// Attempt relogin only once.
	retryLogin := false

DO_REQUEST:
	// Create request.
	payload := bytes.NewBuffer(body)

	req, err := http.NewRequest(method, url, payload)
	if err != nil {
		return nil, 500, "500 Internal Server Error", nil, nil, err
	}

	// Add headers.
	for header, value := range headers {
		req.Header.Add(header, value)
	}
	req.Header.Add("Accept", "*/*")
	req.Header.Add("User-Agent", "PCT")
	req.Header.Add("Content-Type", "application/json")
	if !strings.Contains(url, "/login") {
		req.Header.Add(c.TokenHeader, c.Token)
	}

	// Add cookies.
	sessionCookie := &http.Cookie{
		Name:  c.SessionCookie,
		Value: c.Session,
	}
	tokenCookie := &http.Cookie{
		Name:  c.TokenCookie,
		Value: c.Token,
	}
	req.AddCookie(sessionCookie)
	req.AddCookie(tokenCookie)

	// Send request.
	res, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, 500, "500 Internal Server Error", nil, nil, err
	}
	b, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, 500, "500 Internal Server Error", nil, nil, err
	}
	defer res.Body.Close()

	// Attempt relogin (for non-login requests only) only once, if
	// original request failed.
	if res.StatusCode == 401 {
		if !strings.Contains(url, "/login") && !retryLogin {
			err := c.doLogin()
			if err != nil {
				return nil, 500, "500 Internal Server Error", nil, nil, err
			}

			retryLogin = true
			goto DO_REQUEST
		}
	}

	// Parse cookies.
	cookies := map[string]string{}
	if res.StatusCode >= 200 && res.StatusCode <= 299 {
		for _, cookie := range res.Cookies() {
			cookies[cookie.Name] = cookie.Value
		}
	}

	return b, res.StatusCode, res.Status, res.Header, cookies, nil
}

func (c *Client) doLogin() error {
	method := "POST"
	url := c.Host + "/api/v1/account/login"
	payload := Client{
		OrganisationName: c.OrganisationName,
		Email:            c.Email,
		Password:         c.Password,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	b, statusCode, _, _, cookies, err := c.doRequest(method, url, body, nil)
	if err != nil {
		return err
	}

	if statusCode >= 200 && statusCode <= 299 {
		for name, value := range cookies {
			if name == c.SessionCookie {
				c.Session = value
			}
			if name == c.TokenCookie {
				c.Token = value
			}
		}
		if c.Session == "" || c.Token == "" {
			return fmt.Errorf("failed to login")
		}

		return nil
	} else {
		c.Session, c.Token = "", ""

		msg, err := c.getAPIError(b)
		if err != nil {
			return err
		} else {
			return fmt.Errorf(msg)
		}
	}
}
