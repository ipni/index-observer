package task

import (
	"net/http"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/urfave/cli/v2"
)

var metricRegistry = prometheus.NewRegistry()

var (
	providerCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "provider_count",
		Help: "Number of providers",
	}, []string{"indexer"})
)

func bindMetrics() error {
	// The private go-level metrics live in private.
	if err := metricRegistry.Register(collectors.NewGoCollector()); err != nil {
		return err
	}
	if err := metricRegistry.Register(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})); err != nil {
		return err
	}

	metrics := []prometheus.Collector{
		providerCount,
	}
	for _, m := range metrics {
		if err := metricRegistry.Register(m); err != nil {
			return err
		}
	}
	return nil
}

func StartMetrics(c *cli.Context) error {
	bindMetrics()
	handler := promhttp.HandlerFor(metricRegistry, promhttp.HandlerOpts{Registry: metricRegistry})
	port := c.String("port")
	if !strings.Contains(port, ":") {
		port = ":" + port
	}
	s := http.Server{
		Addr:    port,
		Handler: handler,
	}
	go s.ListenAndServe()

	go func() {
		<-c.Context.Done()
		s.Shutdown(c.Context)
	}()
	return nil
}
