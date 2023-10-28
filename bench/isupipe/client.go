package isupipe

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/isucon/isucandar/agent"
	"github.com/isucon/isucon13/bench/internal/bencherror"
	"github.com/isucon/isucon13/bench/internal/config"
)

var ErrCancelRequest = errors.New("contextのタイムアウトによりリクエストがキャンセルされます")

type Client struct {
	agent *agent.Agent

	// ユーザカスタムテーマ適用ページアクセス用agent
	// ライブ配信画面など
	themeAgent *agent.Agent

	// 画像ダウンロード用agent
	assetAgent *agent.Agent
}

func NewClient(customOpts ...agent.AgentOption) (*Client, error) {
	opts := []agent.AgentOption{
		agent.WithBaseURL(config.TargetBaseURL),
		agent.WithCloneTransport(&http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
			// Custom DNS Resolver
			DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
				dialTimeout := 10000 * time.Millisecond
				dialer := net.Dialer{
					Timeout: dialTimeout,
					Resolver: &net.Resolver{
						PreferGo: true,
						Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
							dialer := net.Dialer{Timeout: dialTimeout}
							nameserver := net.JoinHostPort(config.TargetNameserver, strconv.Itoa(config.DNSPort))
							return dialer.DialContext(ctx, "udp", nameserver)
						},
					},
				}
				return dialer.DialContext(ctx, network, address)
			},
		}),
		agent.WithNoCache(),
		agent.WithTimeout(1 * time.Second),
	}
	for _, customOpt := range customOpts {
		opts = append(opts, customOpt)
	}
	customAgent, err := agent.NewAgent(opts...)
	if err != nil {
		return nil, bencherror.NewInternalError(err)
	}

	return &Client{
		agent: customAgent,
	}, nil
}

// sendRequestはagent.Doをラップしたリクエスト送信関数
// bencherror.WrapErrorはここで実行しているので、呼び出し側ではwrapしない
func (c *Client) sendRequest(ctx context.Context, req *http.Request) (*http.Response, error) {
	endpoint := fmt.Sprintf("%s %s", req.Method, req.URL.EscapedPath())
	resp, err := c.agent.Do(ctx, req)
	if err != nil {
		var (
			netErr net.Error
		)
		if errors.Is(err, context.DeadlineExceeded) {
			// 締切がすぎるのはベンチの都合なので、減点しない
			// リクエストをキャンセルする
			return resp, ErrCancelRequest
		} else if errors.As(err, &netErr) {
			if netErr.Timeout() {
				return resp, bencherror.NewTimeoutError(err, "%s", endpoint)
			} else {
				// 接続ができないなど、ベンチ継続する上で致命的なエラー
				return resp, bencherror.NewViolationError(err, "webappの %s に対するリクエストで、接続に失敗しました", endpoint)
			}
		} else {
			return resp, bencherror.NewApplicationError(err, "%s に対するリクエストが失敗しました", endpoint)
		}
	}

	return resp, nil
}
