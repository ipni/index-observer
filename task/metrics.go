package task

import (
	"net/http"
	"net/http/pprof"
	"runtime"
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
	providerChainLengths prometheus.Metric
	providerEntryChunks  prometheus.Metric
	providerEntryLengths prometheus.Metric
	providerRMCounts     prometheus.Metric
	providerChangeCounts prometheus.Metric
	providerGSCount      prometheus.Metric

	filProviderRate prometheus.Metric
	filDealRate     prometheus.Metric
	filDealCount    = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "fil_deal_count",
		Help: "deals seen",
	}, []string{"type"})
)

type m_collector struct {
	m *prometheus.Metric
}

func (mc m_collector) Describe(ch chan<- *prometheus.Desc) {
	if (*mc.m) != nil {
		d := (*mc.m).Desc()
		ch <- d
	}
}

func (mc m_collector) Collect(ch chan<- prometheus.Metric) {
	if (*mc.m) != nil {
		ch <- (*mc.m)
	}
}

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
	metricRegistry.Register(m_collector{&providerChainLengths})
	metricRegistry.Register(m_collector{&providerEntryChunks})
	metricRegistry.Register(m_collector{&providerEntryLengths})
	metricRegistry.Register(m_collector{&providerRMCounts})
	metricRegistry.Register(m_collector{&providerChangeCounts})
	metricRegistry.Register(m_collector{&providerGSCount})
	metricRegistry.Register(m_collector{&filProviderRate})
	metricRegistry.Register(m_collector{&filDealRate})
	metricRegistry.Register(filDealCount)
	return nil
}

func StartMetrics(c *cli.Context) error {
	bindMetrics()
	mux := http.NewServeMux()
	handler := promhttp.HandlerFor(metricRegistry, promhttp.HandlerOpts{Registry: metricRegistry})
	mux.Handle("/metrics", handler)
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.HandleFunc("/debug/pprof/gc", func(w http.ResponseWriter, req *http.Request) {
		runtime.GC()
	})
	port := c.String("port")
	if !strings.Contains(port, ":") {
		port = ":" + port
	}
	s := http.Server{
		Addr:    port,
		Handler: mux,
	}
	go s.ListenAndServe()

	go func() {
		<-c.Context.Done()
		s.Shutdown(c.Context)
	}()
	return nil
}
