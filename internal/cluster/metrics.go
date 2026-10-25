package cluster

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/monaaoua9/dtask/internal/fsm"
)

type Metrics struct {
	tasksSubmitted *prometheus.CounterVec
	tasksLeased    *prometheus.CounterVec
	outcomes       *prometheus.CounterVec
	fencedRejects  prometheus.Counter
	leasesExpired  prometheus.Counter
	tasksGCed      prometheus.Counter
	nodesExpired   prometheus.Counter
	tasksReleased  prometheus.Counter
	queueWait      *prometheus.HistogramVec
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		tasksSubmitted: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "dtask", Name: "tasks_submitted_total", Help: "Tasks accepted, duplicates excluded."}, []string{"queue"}),
		tasksLeased:    prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "dtask", Name: "tasks_leased_total", Help: "Leases handed to workers."}, []string{"queue"}),
		outcomes:       prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "dtask", Name: "task_outcomes_total", Help: "Worker reports by outcome."}, []string{"outcome"}),
		fencedRejects:  prometheus.NewCounter(prometheus.CounterOpts{Namespace: "dtask", Name: "fenced_requests_total", Help: "Worker calls rejected because their lease token was stale."}),
		leasesExpired:  prometheus.NewCounter(prometheus.CounterOpts{Namespace: "dtask", Name: "leases_expired_total", Help: "Leases requeued after their deadline passed."}),
		tasksGCed:      prometheus.NewCounter(prometheus.CounterOpts{Namespace: "dtask", Name: "tasks_collected_total", Help: "Finished tasks dropped after retention."}),
		nodesExpired:   prometheus.NewCounter(prometheus.CounterOpts{Namespace: "dtask", Name: "node_failover_tasks_total", Help: "Tasks requeued early because their node was declared dead."}),
		tasksReleased:  prometheus.NewCounter(prometheus.CounterOpts{Namespace: "dtask", Name: "tasks_released_total", Help: "Tasks handed back by workers during shutdown."}),
		queueWait: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "dtask", Name: "queue_wait_seconds", Help: "Time between a task becoming runnable and being leased.",
			Buckets: prometheus.ExponentialBuckets(0.001, 4, 10),
		}, []string{"queue"}),
	}
	if reg != nil {
		reg.MustRegister(m.tasksSubmitted, m.tasksLeased, m.outcomes, m.fencedRejects, m.leasesExpired,
			m.tasksGCed, m.nodesExpired, m.tasksReleased, m.queueWait)
	}
	return m
}

func (m *Metrics) submitted(queue string) {
	if m != nil {
		m.tasksSubmitted.WithLabelValues(queue).Inc()
	}
}

func (m *Metrics) leased(queue string, wait time.Duration) {
	if m == nil {
		return
	}
	m.tasksLeased.WithLabelValues(queue).Inc()
	if wait < 0 {
		wait = 0
	}
	m.queueWait.WithLabelValues(queue).Observe(wait.Seconds())
}

func (m *Metrics) outcome(kind string) {
	if m != nil {
		m.outcomes.WithLabelValues(kind).Inc()
	}
}

func (m *Metrics) fenced() {
	if m != nil {
		m.fencedRejects.Inc()
	}
}

func (m *Metrics) reaped(expired, collected int) {
	if m != nil {
		m.leasesExpired.Add(float64(expired))
		m.tasksGCed.Add(float64(collected))
	}
}

func (m *Metrics) nodeExpired(tasks int) {
	if m != nil {
		m.nodesExpired.Add(float64(tasks))
	}
}

// FSMCollector exposes queue depth by state straight from the state machine
// at scrape time, so the numbers can't drift from the truth.
type FSMCollector struct {
	f    *fsm.FSM
	now  func() time.Time
	desc *prometheus.Desc
}

func NewFSMCollector(f *fsm.FSM, now func() time.Time) *FSMCollector {
	if now == nil {
		now = time.Now
	}
	return &FSMCollector{
		f: f, now: now,
		desc: prometheus.NewDesc("dtask_tasks", "Tasks currently stored, by queue and state.", []string{"queue", "state"}, nil),
	}
}

func (c *FSMCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

func (c *FSMCollector) Collect(ch chan<- prometheus.Metric) {
	for _, q := range c.f.Stats(c.now().UnixNano()) {
		for state, n := range map[string]uint64{
			"pending": q.Pending, "delayed": q.Delayed, "leased": q.Leased,
			"succeeded": q.Succeeded, "dead": q.Dead, "canceled": q.Canceled,
		} {
			ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(n), q.Name, state)
		}
	}
}
