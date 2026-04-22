package databricksconnect

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/oauth2"
)

// OAuthM2M is the Databricks OAuth machine-to-machine credential. Accepted
// by spark.Databricks(auth) to authenticate against a workspace without
// any PAT involvement.
type OAuthM2M struct {
	WorkspaceURL string
	ClientID     string
	ClientSecret string
	ClusterID    string
	// HTTPClient overrides the default 30s-timeout client (tests only).
	HTTPClient *http.Client
	// TokenRefreshBuffer is the safety margin before token expiry at
	// which we refresh. Default 5 minutes — avoids races with long-
	// running queries where the stated expiry lands mid-RPC and the
	// query fails with an opaque auth error deep inside Spark.
	TokenRefreshBuffer time.Duration
}

// PAT is a personal-access-token credential. Useful for tests and
// legacy workspaces; production deployments should prefer OAuth M2M.
type PAT struct {
	WorkspaceURL string
	Token        string
	ClusterID    string
}

// Auth is the sum type spark.Databricks accepts.
type Auth interface{ databricksAuth() }

func (OAuthM2M) databricksAuth() {}
func (PAT) databricksAuth()      {}

// databricksTokenResp matches the Databricks OIDC endpoint response.
type databricksTokenResp struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
	Scope       string `json:"scope"`
}

// newOAuthTokenSource returns an oauth2.TokenSource that refreshes
// tokens `buffer` before stated expiry.
func newOAuthTokenSource(cfg OAuthM2M) oauth2.TokenSource {
	hc := cfg.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: 30 * time.Second}
	}
	buffer := cfg.TokenRefreshBuffer
	if buffer == 0 {
		buffer = 5 * time.Minute
	}
	return &databricksTokenSource{cfg: cfg, hc: hc, buffer: buffer}
}

type databricksTokenSource struct {
	cfg    OAuthM2M
	hc     *http.Client
	buffer time.Duration

	cached *oauth2.Token
}

func (s *databricksTokenSource) Token() (*oauth2.Token, error) {
	if s.cached != nil && time.Until(s.cached.Expiry) > s.buffer {
		return s.cached, nil
	}
	tok, err := s.fetch(context.Background())
	if err != nil {
		return nil, err
	}
	s.cached = tok
	return tok, nil
}

func (s *databricksTokenSource) fetch(ctx context.Context) (*oauth2.Token, error) {
	endpoint := strings.TrimRight(s.cfg.WorkspaceURL, "/") + "/oidc/v1/token"

	form := url.Values{"grant_type": {"client_credentials"}, "scope": {"all-apis"}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("create token request: %w", err)
	}
	req.SetBasicAuth(s.cfg.ClientID, s.cfg.ClientSecret)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := s.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("token request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		return nil, fmt.Errorf("token request failed: %s – %s", resp.Status, strings.TrimSpace(string(body)))
	}

	var tr databricksTokenResp
	if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil {
		return nil, fmt.Errorf("decode token response: %w", err)
	}
	if tr.AccessToken == "" {
		return nil, fmt.Errorf("empty access_token in response")
	}

	return &oauth2.Token{
		AccessToken: tr.AccessToken,
		TokenType:   tr.TokenType,
		Expiry:      time.Now().Add(time.Duration(tr.ExpiresIn) * time.Second),
	}, nil
}

// buildSparkConnectURL renders the Spark Connect URL with the OAuth
// token and cluster-ID header. Same shape the production client used.
func buildSparkConnectURL(workspaceURL, token, clusterID string) string {
	host := strings.TrimPrefix(strings.TrimPrefix(workspaceURL, "https://"), "http://")
	if i := strings.IndexByte(host, '/'); i >= 0 {
		host = host[:i]
	}
	return fmt.Sprintf("sc://%s:443/;token=%s;x-databricks-cluster-id=%s", host, token, clusterID)
}
