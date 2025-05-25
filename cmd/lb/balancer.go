package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"runtime/debug"
	"sync"
	"time"

	"github.com/roman-mazur/architecture-practice-4-template/httptools"
	"github.com/roman-mazur/architecture-practice-4-template/signal"
)

var (
	port       = flag.Int("port", 8080, "load balancer port")
	timeoutSec = flag.Int("timeout-sec", 3, "request timeout time in seconds")
	https      = flag.Bool("https", false, "whether backends support HTTPs")

	traceEnabled = flag.Bool("trace", false, "whether to include tracing information into responses")
)

type Server struct {
	URL          *url.URL
	ActiveConns  int64
	IsHealthy    bool
	mutex        sync.RWMutex
	ReverseProxy *httputil.ReverseProxy
}

func (s *Server) IncrementActiveConns() {
	s.mutex.Lock()
	s.ActiveConns++
	s.mutex.Unlock()
}

func (s *Server) DecrementActiveConns() {
	s.mutex.Lock()
	if s.ActiveConns > 0 {
		s.ActiveConns--
	}
	s.mutex.Unlock()
}

func (s *Server) GetActiveConns() int64 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.ActiveConns
}

func (s *Server) SetHealth(status bool) {
	s.mutex.Lock()
	s.IsHealthy = status
	s.mutex.Unlock()
}

func (s *Server) GetHealth() bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.IsHealthy
}

var (
	timeout           time.Duration
	serverDefaultURLs = []string{
		"server1:8080",
		"server2:8080",
		"server3:8080",
	}
	servers     []*Server
	globalMutex sync.RWMutex
)

func scheme() string {
	if *https {
		return "https"
	}
	return "http"
}

func checkServerHealth(s *Server) bool {
	healthURL := fmt.Sprintf("%s://%s/health", s.URL.Scheme, s.URL.Host)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", healthURL, nil)
	if err != nil {
		log.Printf("Error creating health check request for %s (%s): %v", s.URL.Host, healthURL, err)
		return false
	}

	healthCheckClient := http.Client{Timeout: timeout}
	resp, err := healthCheckClient.Do(req)

	if err != nil {
		log.Printf("Health check failed for %s (%s): %v", s.URL.Host, healthURL, err)
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("Health check for %s (%s) returned status %d, expected %d", s.URL.Host, healthURL, resp.StatusCode, http.StatusOK)
		return false
	}
	return true
}

func forward(dst *Server, rw http.ResponseWriter, r *http.Request) error {
	dst.IncrementActiveConns()
	log.Printf("Balancer: Forwarding to %s, active connections now: %d, for request: %s", dst.URL.Host, dst.GetActiveConns(), r.URL.Path)

	defer func() {
		dst.DecrementActiveConns()
		log.Printf("Balancer: Finished request for %s, active connections now: %d, for request: %s", dst.URL.Host, dst.GetActiveConns(), r.URL.Path)
	}()

	if *traceEnabled {
		rw.Header().Set("lb-from", dst.URL.Host)
	}

	log.Printf("Balancer: About to call ReverseProxy.ServeHTTP for %s on %s", r.URL.Path, dst.URL.Host)
	dst.ReverseProxy.ServeHTTP(rw, r)
	log.Printf("Balancer: Returned from ReverseProxy.ServeHTTP for %s on %s", r.URL.Path, dst.URL.Host)
	return nil
}

func selectLeastLoadedServer() *Server {
	globalMutex.RLock()
	defer globalMutex.RUnlock()

	var selected *Server
	minConns := int64(-1)

	for _, server := range servers {
		if server.GetHealth() {
			serverConns := server.GetActiveConns()
			if selected == nil || serverConns < minConns {
				selected = server
				minConns = serverConns
			}
		}
	}
	return selected
}

func startHealthChecks(wg *sync.WaitGroup) {
	globalMutex.RLock()
	serversToMonitor := make([]*Server, len(servers))
	copy(serversToMonitor, servers)
	globalMutex.RUnlock()

	for _, server := range serversToMonitor {
		wg.Add(1)
		go func(s *Server) {
			initialStatus := checkServerHealth(s)
			s.SetHealth(initialStatus)
			log.Printf("Initial health check: %s healthy: %t, active connections: %d", s.URL.Host, s.GetHealth(), s.GetActiveConns())
			wg.Done()

			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					currentStatus := s.GetHealth()
					newStatus := checkServerHealth(s)
					if newStatus != currentStatus {
						log.Printf("Health status change: %s from %t to %t", s.URL.Host, currentStatus, newStatus)
					}
					s.SetHealth(newStatus)
				}
			}
		}(server)
	}
}

func main() {
	flag.Parse()
	timeout = time.Duration(*timeoutSec) * time.Second

	servers = make([]*Server, 0, len(serverDefaultURLs))
	for _, serverURLStr := range serverDefaultURLs {
		fullServerURL := fmt.Sprintf("%s://%s", scheme(), serverURLStr)
		parsedURL, err := url.Parse(fullServerURL)
		if err != nil {
			log.Fatalf("Error parsing server URL %s: %v", fullServerURL, err)
		}

		proxy := httputil.NewSingleHostReverseProxy(parsedURL)
		originalDirector := proxy.Director
		proxy.Director = func(req *http.Request) {
			originalDirector(req)
			req.Host = parsedURL.Host
		}

		proxy.Transport = &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			ForceAttemptHTTP2:     false,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   10,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		}

		proxy.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, err error) {
			log.Printf("[PROXY ERROR] Target: %s, Request: %s %s, Error: %v", parsedURL.Host, req.Method, req.URL.Path, err)
			if rw.Header().Get("X-Balancer-Response-Sent") == "" {
				rw.Header().Set("X-Balancer-Response-Sent", "true")
				if err == context.Canceled || err == context.DeadlineExceeded || err == http.ErrAbortHandler {
					log.Printf("ReverseProxy error likely client abort/cancel or request timeout for host %s: %v", parsedURL.Host, err)
				} else {
					log.Printf("Sending 502 Bad Gateway to client due to ReverseProxy error to host %s: %v", parsedURL.Host, err)
					http.Error(rw, fmt.Sprintf("Bad Gateway: Error connecting to backend server %s", parsedURL.Host), http.StatusBadGateway)
				}
			} else {
				log.Printf("Headers already sent, cannot send error response for host %s: %v", parsedURL.Host, err)
			}
		}

		servers = append(servers, &Server{
			URL:          parsedURL,
			ActiveConns:  0,
			IsHealthy:    false,
			ReverseProxy: proxy,
		})
	}

	var initialHealthCheckWg sync.WaitGroup
	startHealthChecks(&initialHealthCheckWg)

	log.Println("Waiting for initial health checks to complete...")
	initialHealthCheckWg.Wait()
	log.Println("Initial health checks completed.")

	frontend := httptools.CreateServer(*port, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		defer func() {
			if rcv := recover(); rcv != nil {
				log.Printf("PANIC in balancer handler: %v\n%s", rcv, string(debug.Stack()))
				if rw.Header().Get("X-Balancer-Response-Sent") == "" {
					http.Error(rw, "Internal Server Error", http.StatusInternalServerError)
				}
			}
		}()

		log.Printf("Balancer HTTP Handler: Received request for %s from %s", r.URL.String(), r.RemoteAddr)

		selectedServer := selectLeastLoadedServer()
		if selectedServer == nil {
			log.Printf("Balancer HTTP Handler: No healthy servers available for %s", r.URL.String())
			if rw.Header().Get("X-Balancer-Response-Sent") == "" {
				rw.Header().Set("X-Balancer-Response-Sent", "true")
				http.Error(rw, "Service unavailable: No healthy backend servers", http.StatusServiceUnavailable)
			}
			return
		}

		log.Printf("Balancer HTTP Handler: Selected server %s for request %s", selectedServer.URL.Host, r.URL.String())
		ctx, cancel := context.WithTimeout(r.Context(), time.Duration(*timeoutSec)*time.Second)
		defer cancel()

		err := forward(selectedServer, rw, r.WithContext(ctx))
		if err != nil {
			log.Printf("Balancer HTTP Handler: Forwarding function returned an error: %v for %s", err, r.URL.String())
		}
		log.Printf("Balancer HTTP Handler: Finished processing request for %s", r.URL.String())
	}))

	log.Printf("Load balancer starting on port %d...", *port)
	frontend.Start()
	signal.WaitForTerminationSignal()
	log.Println("Load balancer shutting down...")
}
