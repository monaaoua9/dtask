package worker

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	inFlight   prometheus.Gauge
	runs       *prometheus.CounterVec
	duration   *prometheus.HistogramVec
	handedBack prometheus.Counter
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		inFlight: prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "dtask", Subsystem: "worker", Name: "in_flight", Help: "Handlers running right now."}),
		runs:     prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "dtask", Subsystem: "worker", Name: "executions_total", Help: "Handler runs by type and result."}, []string{"type", "result"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "dtask", Subsystem: "worker", Name: "execution_seconds", Help: "Handler wall time.",
			Buckets: prometheus.ExponentialBuckets(0.005, 3, 10),
		}, []string{"type"}),
		handedBack: prometheus.NewCounter(prometheus.CounterOpts{Namespace: "dtask", Subsystem: "worker", Name: "released_total", Help: "Tasks given back unfinished during shutdown."}),
	}
	if reg != nil {
		reg.MustRegister(m.inFlight, m.runs, m.duration, m.handedBack)
	}
	return m
}

func (m *Metrics) started() {
	if m != nil {
		m.inFlight.Inc()
	}
}

func (m *Metrics) finished(typ string, err error, d time.Duration) {
	if m == nil {
		return
	}
	m.inFlight.Dec()
	result := "ok"
	if err != nil {
		result = "error"
	}
	m.runs.WithLabelValues(typ, result).Inc()
	m.duration.WithLabelValues(typ).Observe(d.Seconds())
}

func (m *Metrics) released() {
	if m != nil {
		m.handedBack.Inc()
	}
}
