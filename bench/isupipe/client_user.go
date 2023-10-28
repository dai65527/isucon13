package isupipe

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/isucon/isucandar/agent"
	"github.com/isucon/isucon13/bench/internal/bencherror"
	"github.com/isucon/isucon13/bench/internal/benchscore"
	"github.com/isucon/isucon13/bench/internal/config"
)

type User struct {
	Id          int    `json:"id"`
	Name        string `json:"name"`
	DisplayName string `json:"display_name"`
	Description string `json:"description"`
	CreatedAt   int    `json:"created_at"`
	UpdatedAt   int    `json:"updated_at"`

	Theme     Theme `json:"theme"`
	IsPopular bool  `json:"is_popular"`
}

type (
	PostUserRequest struct {
		Name        string `json:"name"`
		DisplayName string `json:"display_name"`
		Description string `json:"description"`
		// Password is non-hashed password.
		Password string `json:"password"`
		Theme    Theme  `json:"theme"`
	}
	LoginRequest struct {
		UserName string `json:"username"`
		// Password is non-hashed password.
		Password string `json:"password"`
	}
)

func (c *Client) DownloadIcon(ctx context.Context, user *User, opts ...ClientOption) error {
	// FIXME: impl
	return nil
}

func (c *Client) GetUsers(ctx context.Context, opts ...ClientOption) ([]*User, error) {
	var (
		defaultStatusCode = http.StatusOK
		o                 = newClientOptions(defaultStatusCode, opts...)
	)

	req, err := c.agent.NewRequest(http.MethodGet, "/user", nil)
	if err != nil {
		return nil, bencherror.NewInternalError(err)
	}

	resp, err := c.sendRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != o.wantStatusCode {
		return nil, bencherror.NewHttpStatusError(req, o.wantStatusCode, resp.StatusCode)
	}

	var users []*User
	if resp.StatusCode == defaultStatusCode {
		if err := json.NewDecoder(resp.Body).Decode(&users); err != nil {
			return users, bencherror.NewHttpResponseError(err, req)
		}
	}

	benchscore.AddScore(benchscore.SuccessGetUsers)
	return users, nil
}

func (c *Client) PostUser(ctx context.Context, r *PostUserRequest, opts ...ClientOption) (*User, error) {
	var (
		defaultStatusCode = http.StatusCreated
		o                 = newClientOptions(defaultStatusCode, opts...)
	)

	payload, err := json.Marshal(r)
	if err != nil {
		return nil, bencherror.NewInternalError(err)
	}

	req, err := c.agent.NewRequest(http.MethodPost, "/user", bytes.NewReader(payload))
	if err != nil {
		return nil, bencherror.NewInternalError(err)
	}
	req.Header.Add("Content-Type", "application/json;chatset=utf-8")

	resp, err := c.sendRequest(ctx, req)
	if err != nil {
		// sendRequestはWrapErrorを行っているのでそのままreturn
		return nil, err
	}

	if resp.StatusCode != o.wantStatusCode {
		return nil, bencherror.NewHttpStatusError(req, o.wantStatusCode, resp.StatusCode)
	}

	var user *User
	if resp.StatusCode == defaultStatusCode {
		if err := json.NewDecoder(resp.Body).Decode(&user); err != nil {
			return nil, bencherror.NewHttpResponseError(err, req)
		}
	}

	benchscore.AddScore(benchscore.SuccessRegister)
	return user, nil
}

func (c *Client) Login(ctx context.Context, r *LoginRequest, opts ...ClientOption) error {
	var (
		defaultStatusCode = http.StatusOK
		o                 = newClientOptions(defaultStatusCode, opts...)
	)

	payload, err := json.Marshal(r)
	if err != nil {
		return bencherror.NewInternalError(err)
	}

	req, err := c.agent.NewRequest(http.MethodPost, "/login", bytes.NewReader(payload))
	if err != nil {
		return bencherror.NewInternalError(err)
	}
	req.Header.Add("Content-Type", "application/json;chatset=utf-8")

	resp, err := c.sendRequest(ctx, req)
	if err != nil {
		return err
	}

	if resp.StatusCode != o.wantStatusCode {
		return bencherror.NewHttpStatusError(req, o.wantStatusCode, resp.StatusCode)
	}

	// cookieを流用して各種ページアクセス用agentを初期化
	domain := fmt.Sprintf("%s.u.isucon.dev", r.UserName)
	c.themeAgent, err = agent.NewAgent(
		agent.WithBaseURL(fmt.Sprintf("http://%s:12345", domain)),
		WithClient(c.agent.HttpClient),
		agent.WithNoCache(),
	)
	if err != nil {
		return bencherror.NewInternalError(err)
	}
	c.assetAgent, err = agent.NewAgent(
		agent.WithBaseURL(config.TargetBaseURL),
		WithClient(c.agent.HttpClient),
		// NOTE: 画像はキャッシュできるようにする
	)
	if err != nil {
		return bencherror.NewInternalError(err)
	}

	benchscore.AddScore(benchscore.SuccessLogin)
	return nil
}

// FIXME: meに変える
func (c *Client) GetUserSession(ctx context.Context, opts ...ClientOption) error {
	var (
		defaultStatusCode = http.StatusOK
		o                 = newClientOptions(defaultStatusCode, opts...)
	)

	req, err := c.agent.NewRequest(http.MethodGet, "/user/me", nil)
	if err != nil {
		return bencherror.NewInternalError(err)
	}

	resp, err := c.sendRequest(ctx, req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != o.wantStatusCode {
		return bencherror.NewHttpStatusError(req, o.wantStatusCode, resp.StatusCode)
	}

	return nil
}

func (c *Client) GetUser(ctx context.Context, username string, opts ...ClientOption) error {
	var (
		defaultStatusCode = http.StatusOK
		o                 = newClientOptions(defaultStatusCode, opts...)
	)

	urlPath := fmt.Sprintf("/user/%s", username)
	req, err := c.agent.NewRequest(http.MethodGet, urlPath, nil)
	if err != nil {
		return bencherror.NewInternalError(err)
	}

	resp, err := c.sendRequest(ctx, req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != o.wantStatusCode {
		return bencherror.NewHttpStatusError(req, o.wantStatusCode, resp.StatusCode)
	}

	benchscore.AddScore(benchscore.SuccessGetUser)
	return nil
}
