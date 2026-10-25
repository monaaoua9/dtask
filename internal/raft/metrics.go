package raft

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	term           prometheus.Gauge
	role           prometheus.Gauge
	commitIndex    prometheus.Gauge
	appliedIndex   prometheus.Gauge
	lastLogIndex   prometheus.Gauge
	elections      *prometheus.CounterVec
	leaderChanges  prometheus.Counter
	proposals      *prometheus.CounterVec
	commitLatency  prometheus.Histogram
	snapshots      prometheus.Counter
	snapshotsSent  prometheus.Counter
	quorumStepDown prometheus.Counter
	rpcErrors      *prometheus.CounterVec
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		term:           prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "dtask", Subsystem: "raft", Name: "term", Help: "Current Raft term."}),
		role:           prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "dtask", Subsystem: "raft", Name: "role", Help: "0 follower, 1 candidate, 2 leader."}),
		commitIndex:    prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "dtask", Subsystem: "raft", Name: "commit_index", Help: "Highest committed log index."}),
		appliedIndex:   prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "dtask", Subsystem: "raft", Name: "applied_index", Help: "Highest log index applied to the state machine."}),
		lastLogIndex:   prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "dtask", Subsystem: "raft", Name: "last_log_index", Help: "Index of the last log entry."}),
		elections:      prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "dtask", Subsystem: "raft", Name: "elections_total", Help: "Election attempts by phase and outcome."}, []string{"phase", "outcome"}),
		leaderChanges:  prometheus.NewCounter(prometheus.CounterOpts{Namespace: "dtask", Subsystem: "raft", Name: "leader_changes_total", Help: "Times this node became leader."}),
		proposals:      prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "dtask", Subsystem: "raft", Name: "proposals_total", Help: "Proposals by result."}, []string{"result"}),
		commitLatency:  prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "dtask", Subsystem: "raft", Name: "commit_latency_seconds", Help: "Propose to applied latency on the leader.", Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14)}),
		snapshots:      prometheus.NewCounter(prometheus.CounterOpts{Namespace: "dtask", Subsystem: "raft", Name: "snapshots_taken_total", Help: "Local log compactions."}),
		snapshotsSent:  prometheus.NewCounter(prometheus.CounterOpts{Namespace: "dtask", Subsystem: "raft", Name: "snapshots_sent_total", Help: "Snapshots shipped to lagging followers."}),
		quorumStepDown: prometheus.NewCounter(prometheus.CounterOpts{Namespace: "dtask", Subsystem: "raft", Name: "quorum_step_downs_total", Help: "Leader stepped down after losing contact with a majority."}),
		rpcErrors:      prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "dtask", Subsystem: "raft", Name: "rpc_errors_total", Help: "Outbound RPC failures by method."}, []string{"method"}),
	}
	if reg != nil {
		reg.MustRegister(m.term, m.role, m.commitIndex, m.appliedIndex, m.lastLogIndex, m.elections,
			m.leaderChanges, m.proposals, m.commitLatency, m.snapshots, m.snapshotsSent, m.quorumStepDown, m.rpcErrors)
	}
	return m
}

// all methods tolerate a nil receiver so tests can skip metrics entirely

func (m *Metrics) setState(term uint64, role Role, commit, applied, last uint64) {
	if m == nil {
		return
	}
	m.term.Set(float64(term))
	m.role.Set(float64(role))
	m.commitIndex.Set(float64(commit))
	m.appliedIndex.Set(float64(applied))
	m.lastLogIndex.Set(float64(last))
}

func (m *Metrics) election(phase, outcome string) {
	if m != nil {
		m.elections.WithLabelValues(phase, outcome).Inc()
	}
}

func (m *Metrics) becameLeader() {
	if m != nil {
		m.leaderChanges.Inc()
	}
}

func (m *Metrics) proposal(result string) {
	if m != nil {
		m.proposals.WithLabelValues(result).Inc()
	}
}

func (m *Metrics) observeCommit(seconds float64) {
	if m != nil {
		m.commitLatency.Observe(seconds)
	}
}

func (m *Metrics) snapshotTaken() {
	if m != nil {
		m.snapshots.Inc()
	}
}

func (m *Metrics) snapshotSent() {
	if m != nil {
		m.snapshotsSent.Inc()
	}
}

func (m *Metrics) stepDownNoQuorum() {
	if m != nil {
		m.quorumStepDown.Inc()
	}
}

func (m *Metrics) rpcError(method string) {
	if m != nil {
		m.rpcErrors.WithLabelValues(method).Inc()
	}
}
