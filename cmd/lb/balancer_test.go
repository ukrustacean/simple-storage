package main

import (
	"fmt"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

// newTestServer creates a Server instance for testing.
func newTestServer(rawURL string, isHealthy bool, connections int64) *Server {
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		panic(fmt.Sprintf("Failed to parse URL for test server %s: %v", rawURL, err))
	}
	return &Server{
		URL:          parsedURL,
		ActiveConns:  connections,
		IsHealthy:    isHealthy,
		ReverseProxy: nil,
	}
}

func TestSelectLeastLoadedServer(t *testing.T) {
	originalServers := servers
	defer func() { servers = originalServers }()

	testCases := []struct {
		name              string
		setupServers      func() []*Server
		expectedServerURL string
	}{
		{
			name: "single healthy server with zero connections",
			setupServers: func() []*Server {
				return []*Server{
					newTestServer("http://server1:8080", true, 0),
				}
			},
			expectedServerURL: "http://server1:8080",
		},
		{
			name: "multiple healthy servers, select one with least connections",
			setupServers: func() []*Server {
				return []*Server{
					newTestServer("http://server1:8080", true, 5),
					newTestServer("http://server2:8080", true, 2),
					newTestServer("http://server3:8080", true, 3),
				}
			},
			expectedServerURL: "http://server2:8080",
		},
		{
			name: "all servers unhealthy",
			setupServers: func() []*Server {
				return []*Server{
					newTestServer("http://server1:8080", false, 0),
					newTestServer("http://server2:8080", false, 0),
				}
			},
			expectedServerURL: "",
		},
		{
			name: "one healthy server among unhealthy ones",
			setupServers: func() []*Server {
				return []*Server{
					newTestServer("http://server1:8080", false, 10),
					newTestServer("http://server2:8080", true, 5),
					newTestServer("http://server3:8080", false, 0),
				}
			},
			expectedServerURL: "http://server2:8080",
		},
		{
			name: "tie in connections, should pick the first one encountered in the list",
			setupServers: func() []*Server {
				return []*Server{
					newTestServer("http://server1:8080", true, 2),
					newTestServer("http://server2:8080", true, 5),
					newTestServer("http://server3:8080", true, 2),
				}
			},
			expectedServerURL: "http://server1:8080",
		},
		{
			name: "no servers configured (empty list)",
			setupServers: func() []*Server {
				return []*Server{}
			},
			expectedServerURL: "",
		},
		{
			name: "all healthy, all zero connections, pick first",
			setupServers: func() []*Server {
				return []*Server{
					newTestServer("http://server1:8080", true, 0),
					newTestServer("http://server2:8080", true, 0),
					newTestServer("http://server3:8080", true, 0),
				}
			},
			expectedServerURL: "http://server1:8080",
		},
		{
			name: "last server has least connections",
			setupServers: func() []*Server {
				return []*Server{
					newTestServer("http://server1:8080", true, 3),
					newTestServer("http://server2:8080", true, 4),
					newTestServer("http://server3:8080", true, 1),
				}
			},
			expectedServerURL: "http://server3:8080",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			servers = tc.setupServers()
			selected := selectLeastLoadedServer()

			if tc.expectedServerURL == "" {
				assert.Nil(t, selected, "Expected nil server but got one")
			} else {
				if assert.NotNil(t, selected, "Expected a server but got nil") {
					assert.Equal(t, tc.expectedServerURL, selected.URL.String(), "Selected server URL mismatch")
					assert.True(t, selected.GetHealth(), "Selected server should be healthy")
				}
			}
		})
	}
}
