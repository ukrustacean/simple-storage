package integration

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var baseAddress = "http://balancer:8080"

var expectedServers = []string{
	"server1:8080",
	"server2:8080",
	"server3:8080",
}

func TestMain(m *testing.M) {
	if addr := os.Getenv("BALANCER_ADDR"); addr != "" {
		baseAddress = addr
	}
	os.Exit(m.Run())
}

func TestLeastConnectionsDistribution(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Skipping integration test: INTEGRATION_TEST environment variable not set")
	}

	time.Sleep(10 * time.Second)
	client := http.Client{Timeout: 8 * time.Second}

	serverRequestCounts := make(map[string]int)
	var mu sync.Mutex

	numConcurrentLoadGenerators := 6
	requestsPerGenerator := 5
	totalRequests := numConcurrentLoadGenerators * requestsPerGenerator

	var wg sync.WaitGroup
	successfulRequests := 0
	var successMu sync.Mutex

	for i := 0; i < numConcurrentLoadGenerators; i++ {
		wg.Add(1)
		go func(generatorID int) {
			defer wg.Done()
			for j := 0; j < requestsPerGenerator; j++ {
				url := fmt.Sprintf("%s/api/v1/some-data?gen=%d&req=%d&time=%d", baseAddress, generatorID, j, time.Now().UnixNano())

				var resp *http.Response
				var err error

				for attempt := 0; attempt < 3; attempt++ {
					resp, err = client.Get(url)
					if err == nil && resp.StatusCode == http.StatusOK {
						break
					}
					if resp != nil {
						resp.Body.Close()
					}
					time.Sleep(time.Duration(500+attempt*500) * time.Millisecond)
				}

				require.NoErrorf(t, err, "Generator %d, Request %d: failed after retries", generatorID, j)

				require.Equalf(t, http.StatusOK, resp.StatusCode, "Generator %d, Request %d: expected 200 OK", generatorID, j)

				successMu.Lock()
				successfulRequests++
				successMu.Unlock()

				serverFrom := resp.Header.Get("lb-from")
				require.NotEmptyf(t, serverFrom, "Generator %d, Request %d: lb-from header missing", generatorID, j)

				mu.Lock()
				serverRequestCounts[serverFrom]++
				mu.Unlock()

				resp.Body.Close()
			}
		}(i)
	}

	wg.Wait()

	t.Logf("Finished sending requests. Total successful: %d/%d", successfulRequests, totalRequests)
	t.Logf("Server request distribution: %v", serverRequestCounts)

	require.GreaterOrEqualf(t, successfulRequests, totalRequests*3/4,
		"Less than 75%% of requests were successful (%d/%d)", successfulRequests, totalRequests)

	if len(serverRequestCounts) == 0 && successfulRequests > 0 {
		t.Error("No 'lb-from' headers were found, cannot verify distribution.")
	} else if len(serverRequestCounts) <= 1 && successfulRequests > 1 && len(expectedServers) > 1 {
		t.Logf("Warning: All requests went to %d server(s): %v", len(serverRequestCounts), serverRequestCounts)
	} else if len(serverRequestCounts) > 1 {
		t.Logf("Requests were distributed among %d servers: %v", len(serverRequestCounts), serverRequestCounts)
	}

	for serverName := range serverRequestCounts {
		found := false
		for _, expected := range expectedServers {
			if strings.HasPrefix(serverName, expected) {
				found = true
				break
			}
		}
		assert.Truef(t, found, "Unexpected server: %s not in expected list %v", serverName, expectedServers)
	}

	if len(serverRequestCounts) == len(expectedServers) {
		minReq := totalRequests
		maxReq := 0
		for _, count := range serverRequestCounts {
			if count < minReq {
				minReq = count
			}
			if count > maxReq {
				maxReq = count
			}
		}
		if minReq > 0 {
			ratio := float64(maxReq) / float64(minReq)
			if ratio > 3.0 && totalRequests > len(expectedServers)*3 {
				t.Logf("Warning: Load skewed. Max: %d, Min: %d, Ratio: %.2f", maxReq, minReq, ratio)
			} else {
				t.Logf("Load distribution reasonable. Max: %d, Min: %d, Ratio: %.2f", maxReq, minReq, ratio)
			}
		}
	}

	t.Log("Integration test for least connections finished.")
}

func BenchmarkBalancer(b *testing.B) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		b.Skip("Skipping integration benchmark: INTEGRATION_TEST environment variable not set")
	}

	time.Sleep(10 * time.Second)

	client := http.Client{Timeout: 3 * time.Second}
	targetURL := fmt.Sprintf("%s/api/v1/some-data", baseAddress)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := client.Get(targetURL)
		if err != nil {
			continue
		}
		if resp.Body != nil {
			resp.Body.Close()
		}
	}
}
