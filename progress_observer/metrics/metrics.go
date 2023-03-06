package metrics

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric/instrument"

	ogprometheus "github.com/prometheus/client_golang/prometheus"
	cmetric "go.opentelemetry.io/otel/metric"

	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/aggregation"
)

type Metrics struct {
	lag   instrument.Int64Histogram
	count instrument.Int64ObservableGauge

	countObservations     []countObservation
	countObservationsLock sync.Mutex
	reg                   cmetric.Registration
}

type countObservation struct {
	kind   string
	source string
	target string
	count  int
}

const (
	// TotalCount is a tag for the total number of providers known to the indexer
	TotalCount = "total"
	// IngestLag is a tag for the sum of all Lags across all known providers. In other words it says how much advertisements
	// the indexer needs to process in order to catch up with the currently known head
	IngestLag = "ingest_lag"
	// MatchCount is a tag for the number of providers that are known to both source and target indexers as well as have LastAdvertisement matching.
	MatchCount = "match"
	// UnknownCount is a tag for the number of providers that are known to the source but unknown to the target indexer
	UnknownCount = "unknown"
	// UnreachableCount is a tag for the number of providers that index-observer has failed to reach out to
	UnreachableCount = "unreachable"
)

// histogram buckets for lags
func aggregationSelector(ik metric.InstrumentKind) aggregation.Aggregation {
	if ik == metric.InstrumentKindHistogram {
		return aggregation.ExplicitBucketHistogram{
			Boundaries: []float64{0, 10, 50, 100, 200, 500, 1000, 2000, 5000, 10000},
			NoMinMax:   false,
		}
	}
	return metric.DefaultAggregationSelector(ik)
}

func Register(reg ogprometheus.Registerer) (*Metrics, error) {
	var m Metrics
	var err error
	var exporter *prometheus.Exporter
	if exporter, err = prometheus.New(
		prometheus.WithRegisterer(reg),
		prometheus.WithoutUnits(),
		prometheus.WithAggregationSelector(aggregationSelector)); err != nil {
		return nil, err
	}

	provider := metric.NewMeterProvider(metric.WithReader(exporter))
	meter := provider.Meter("ipni/index_observer")

	if m.lag, err = meter.Int64Histogram("ipni/index_observer/ingest_lag_diff",
		instrument.WithDescription("Relative difference between source and target in how far behind one is from the other in processing advertisements.")); err != nil {
		return nil, err
	}

	if m.count, err = meter.Int64ObservableGauge("ipni/index_observer/count",
		instrument.WithDescription("Tagged provider-counts by target and source indexers. Includes such metrics as the total number"+
			" of providers across the two indexers, the number of providers that have matching last advertisement cid, the number of providers unknown"+
			" to the source or target and the number of providers that are unreachable.")); err != nil {
		return nil, err
	}

	m.reg, err = meter.RegisterCallback(
		m.observe,
		m.count,
	)

	if err != nil {
		return nil, err
	}

	return &m, nil
}

func (m *Metrics) RecordLag(ctx context.Context, lag uint, source, target string) {
	m.lag.Record(ctx, int64(lag), attribute.String("source", source), attribute.String("target", target))
}

func (m *Metrics) RecordCount(count int, source, target, kind string) {
	m.countObservationsLock.Lock()
	defer m.countObservationsLock.Unlock()
	m.countObservations = append(m.countObservations, countObservation{
		kind:   kind,
		source: source,
		target: target,
		count:  count,
	})
}

func (m *Metrics) observe(ctx context.Context, observer cmetric.Observer) error {
	m.countObservationsLock.Lock()
	defer m.countObservationsLock.Unlock()

	for _, o := range m.countObservations {
		tags := []attribute.KeyValue{attribute.String("source", o.source), attribute.String("kind", o.kind)}
		if len(o.target) > 0 {
			tags = append(tags, attribute.String("target", o.target))
		}

		observer.ObserveInt64(m.count, int64(o.count), tags...)
	}

	m.countObservations = make([]countObservation, 0)
	return nil
}

func (m *Metrics) Unregister(ctx context.Context) error {
	return m.reg.Unregister()
}
