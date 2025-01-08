package sr

import (
	"crypto/tls"
	"net"
	"net/http"
	"strings"
	"time"
)

type (
	// ClientOpt is an option to configure a client.
	ClientOpt interface{ apply(*Client) }
	clientOpt struct{ fn func(*Client) }
)

func (o clientOpt) apply(cl *Client) { o.fn(cl) }

// HTTPClient sets the http client that the schema registry client uses,
// overriding the default client that speaks plaintext with a timeout of 5s.
func HTTPClient(httpcl *http.Client) ClientOpt {
	return clientOpt{func(cl *Client) { cl.httpcl = httpcl }}
}

// UserAgent sets the User-Agent to use in requests, overriding the default
// "franz-go".
func UserAgent(ua string) ClientOpt {
	return clientOpt{func(cl *Client) { cl.ua = ua }}
}

// URLs sets the URLs that the client speaks to, overriding the default
// http://localhost:8081. This option automatically prefixes any URL that is
// missing an http:// or https:// prefix with http://.
func URLs(urls ...string) ClientOpt {
	return clientOpt{func(cl *Client) {
		for i, u := range urls {
			if strings.HasPrefix(u, "http://") || strings.HasPrefix(u, "https://") {
				continue
			}

			urls[i] = "http://" + u
		}
		cl.urls = urls
	}}
}

// DialTLSConfig sets a tls.Config to use in the default http client.
func DialTLSConfig(c *tls.Config) ClientOpt {
	return clientOpt{func(cl *Client) {
		cl.httpcl = &http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				Proxy: http.ProxyFromEnvironment,
				DialContext: (&net.Dialer{
					Timeout:   30 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				TLSClientConfig:       c,
				ForceAttemptHTTP2:     true,
				MaxIdleConns:          100,
				MaxIdleConnsPerHost:   100,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
			},
		}
	}}
}

// BasicAuth sets basic authorization to use for every request.
func BasicAuth(user, pass string) ClientOpt {
	return clientOpt{func(cl *Client) {
		cl.basicAuth = &struct {
			user string
			pass string
		}{user, pass}
	}}
}

// BearerToken sets an Authorization header to use for every request.
// The format will be: "Authorization: Bearer $token".
func BearerToken(token string) ClientOpt {
	return clientOpt{func(cl *Client) {
		cl.bearerToken = token
	}}
}

// PreReq sets a hook func to call before every request is sent.
func PreReq(preReq func(req *http.Request) error) ClientOpt {
	return clientOpt{func(cl *Client) {
		cl.preReq = preReq
	}}
}

// DefaultParams sets default parameters to apply to every request.
func DefaultParams(ps ...Param) ClientOpt {
	return clientOpt{func(cl *Client) {
		cl.defParams = mergeParams(ps...)
	}}
}
