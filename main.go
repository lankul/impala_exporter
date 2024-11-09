package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ImpalaClientHost represents the structure of each client host in the JSON response
type ImpalaClientHost struct {
	Hostname              string `json:"hostname"`
	TotalConnections      int    `json:"total_connections"`
	TotalSessions         int    `json:"total_sessions"`
	TotalActiveSessions   int    `json:"total_active_sessions"`
	TotalInactiveSessions int    `json:"total_inactive_sessions"`
	InflightQueries       int    `json:"inflight_queries"`
	TotalQueries          int    `json:"total_queries"`
}

// InFlightQuery represents a single in-flight query
type InFlightQuery struct {
	Duration string `json:"duration"`
}

// ImpalaSessionsResponse represents the structure of the JSON response from Impala
type ImpalaSessionsResponse struct {
	ClientHosts []ImpalaClientHost `json:"client_hosts"`
}

// QueriesResponse represents the structure of the JSON response from Impala for in-flight queries
type QueriesResponse struct {
	InFlightQueries []InFlightQuery `json:"in_flight_queries"`
}

// Exporter collects Impala metrics
type Exporter struct {
	impalaServers         []string
	totalConnections      *prometheus.Desc
	totalSessions         *prometheus.Desc
	totalActiveSessions   *prometheus.Desc
	totalInactiveSessions *prometheus.Desc
	inflightQueries       *prometheus.Desc
	totalQueries          *prometheus.Desc
	inflightQueriesCount  *prometheus.Desc
	slowQueriesCount      map[int]*prometheus.Desc
}

// NewExporter creates a new instance of Exporter
func NewExporter(impalaServers []string) *Exporter {
	slowQueriesCount := map[int]*prometheus.Desc{
		10:  prometheus.NewDesc("impala_slow10s_queries_count", "Number of queries slower than 10 seconds", []string{"impala_server"}, nil),
		30:  prometheus.NewDesc("impala_slow30s_queries_count", "Number of queries slower than 30 seconds", []string{"impala_server"}, nil),
		60:  prometheus.NewDesc("impala_slow1m_queries_count", "Number of queries slower than 1 minute", []string{"impala_server"}, nil),
		120: prometheus.NewDesc("impala_slow2m_queries_count", "Number of queries slower than 2 minutes", []string{"impala_server"}, nil),
		180: prometheus.NewDesc("impala_slow3m_queries_count", "Number of queries slower than 3 minutes", []string{"impala_server"}, nil),
		300: prometheus.NewDesc("impala_slow5m_queries_count", "Number of queries slower than 5 minutes", []string{"impala_server"}, nil),
		600: prometheus.NewDesc("impala_slow10m_queries_count", "Number of queries slower than 10 minutes", []string{"impala_server"}, nil),
	}
	return &Exporter{
		impalaServers: impalaServers,
		totalConnections: prometheus.NewDesc(
			"impala_total_connections",
			"Total number of connections for an Impala client",
			[]string{"impala_server", "impala_client"},
			nil,
		),
		totalSessions: prometheus.NewDesc(
			"impala_total_sessions",
			"Total number of sessions for an Impala client",
			[]string{"impala_server", "impala_client"},
			nil,
		),
		totalActiveSessions: prometheus.NewDesc(
			"impala_total_active_sessions",
			"Total number of active sessions for an Impala client",
			[]string{"impala_server", "impala_client"},
			nil,
		),
		totalInactiveSessions: prometheus.NewDesc(
			"impala_total_inactive_sessions",
			"Total number of inactive sessions for an Impala client",
			[]string{"impala_server", "impala_client"},
			nil,
		),
		inflightQueries: prometheus.NewDesc(
			"impala_inflight_queries",
			"Number of inflight queries for an Impala client",
			[]string{"impala_server", "impala_client"},
			nil,
		),
		totalQueries: prometheus.NewDesc(
			"impala_total_queries",
			"Total number of queries for an Impala client",
			[]string{"impala_server", "impala_client"},
			nil,
		),
		inflightQueriesCount: prometheus.NewDesc(
			"impala_inflight_queries_count",
			"Total number of in-flight queries",
			[]string{"impala_server"},
			nil,
		),
		slowQueriesCount: slowQueriesCount,
	}
}

// Describe sends the descriptors of each metric over to the provided channel
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- e.totalConnections
	ch <- e.totalSessions
	ch <- e.totalActiveSessions
	ch <- e.totalInactiveSessions
	ch <- e.inflightQueries
	ch <- e.totalQueries
	ch <- e.inflightQueriesCount
	for _, desc := range e.slowQueriesCount {
		ch <- desc
	}
}

// ParseDuration parses the duration string to seconds
func ParseDuration(duration string) (float64, error) {
	re := regexp.MustCompile(`(?:(\d+)m)?(?:(\d+)s)?(?:(\d+)ms)?`)
	matches := re.FindStringSubmatch(duration)

	var totalSeconds float64
	if matches[1] != "" {
		minutes, _ := strconv.Atoi(matches[1])
		totalSeconds += float64(minutes * 60)
	}
	if matches[2] != "" {
		seconds, _ := strconv.Atoi(matches[2])
		totalSeconds += float64(seconds)
	}
	if matches[3] != "" {
		milliseconds, _ := strconv.Atoi(matches[3])
		totalSeconds += float64(milliseconds) / 1000
	}
	return totalSeconds, nil
}

// Collect fetches the metrics from the Impala servers and sends them over to the provided channel
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	for _, server := range e.impalaServers {
		// Collect session metrics
		url := fmt.Sprintf("http://%s/sessions?json", server)
		resp, err := http.Get(url)
		if err != nil {
			log.Printf("Error fetching sessions from %s: %v", server, err)
			continue
		}
		defer resp.Body.Close()

		var sessions ImpalaSessionsResponse
		if err := json.NewDecoder(resp.Body).Decode(&sessions); err != nil {
			log.Printf("Error decoding JSON response from %s: %v", server, err)
			continue
		}

		for _, client := range sessions.ClientHosts {
			impalaClient := client.Hostname
			ch <- prometheus.MustNewConstMetric(e.totalConnections, prometheus.GaugeValue, float64(client.TotalConnections), server, impalaClient)
			ch <- prometheus.MustNewConstMetric(e.totalSessions, prometheus.GaugeValue, float64(client.TotalSessions), server, impalaClient)
			ch <- prometheus.MustNewConstMetric(e.totalActiveSessions, prometheus.GaugeValue, float64(client.TotalActiveSessions), server, impalaClient)
			ch <- prometheus.MustNewConstMetric(e.totalInactiveSessions, prometheus.GaugeValue, float64(client.TotalInactiveSessions), server, impalaClient)
			ch <- prometheus.MustNewConstMetric(e.inflightQueries, prometheus.GaugeValue, float64(client.InflightQueries), server, impalaClient)
			ch <- prometheus.MustNewConstMetric(e.totalQueries, prometheus.GaugeValue, float64(client.TotalQueries), server, impalaClient)
		}

		// Collect query metrics
		url = fmt.Sprintf("http://%s/queries?json", server)
		resp, err = http.Get(url)
		if err != nil {
			log.Printf("Error fetching queries from %s: %v", server, err)
			continue
		}
		defer resp.Body.Close()

		var queries QueriesResponse
		if err := json.NewDecoder(resp.Body).Decode(&queries); err != nil {
			log.Printf("Error decoding JSON response from %s: %v", server, err)
			continue
		}

		// Track total in-flight queries and slow queries by duration
		ch <- prometheus.MustNewConstMetric(e.inflightQueriesCount, prometheus.GaugeValue, float64(len(queries.InFlightQueries)), server)

		slowCounts := make(map[int]float64)
		for _, query := range queries.InFlightQueries {
			durationSeconds, err := ParseDuration(query.Duration)
			if err != nil {
				log.Printf("Error parsing duration: %v", err)
				continue
			}

			for threshold := range e.slowQueriesCount {
				if durationSeconds > float64(threshold) {
					slowCounts[threshold]++
				}
			}
		}

		for threshold, count := range slowCounts {
			ch <- prometheus.MustNewConstMetric(e.slowQueriesCount[threshold], prometheus.GaugeValue, count, server)
		}
	}
}

func main() {
	// Parse the command line arguments to get the list of Impala servers and port number
	impalaServersFlag := flag.String("impala_servers", "", "Comma-separated list of Impala server addresses (e.g., 10.11.18.16:25000,10.11.18.17:25000)")
	portFlag := flag.String("port", "8080", "The port to expose metrics on")
	flag.Parse()

	if *impalaServersFlag == "" {
		log.Fatal("Please provide at least one Impala server address using the -impala_servers flag.")
	}

	// Split the comma-separated string into a slice of server addresses
	impalaServers := strings.Split(*impalaServersFlag, ",")

	exporter := NewExporter(impalaServers)
	prometheus.MustRegister(exporter)

	http.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{
		Addr:         fmt.Sprintf(":%s", *portFlag),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	fmt.Printf("Starting server on %s/metrics\n", srv.Addr)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("Error starting HTTP server: %v", err)
	}
}
