package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"time"
)

type PromMetrics struct {
	claimed        prometheus.Counter
	published      prometheus.Counter
	publishFailed  prometheus.Counter
	reclaimed      prometheus.Counter
	publishLatency prometheus.Histogram
}

func NewPromMetrics(reg prometheus.Registerer) *PromMetrics {

	m := &PromMetrics{
		claimed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "scheduler_tasks_claimed_total",
			Help: "Number of claimed tasks",
		}),
		published: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "scheduler_tasks_published_total",
			Help: "Number of published tasks",
		}),
		publishFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "scheduler_tasks_publish_failed_total",
			Help: "Number of published tasks failed",
		}),
		reclaimed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "scheduler_tasks_reclaimed_total",
			Help: "Number of reclaimed tasks",
		}),
		publishLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "scheduler_publish_latency_seconds",
			Help:    "Latency of published tasks",
			Buckets: prometheus.DefBuckets,
		}),
	}
	reg.MustRegister(m.claimed, m.published, m.publishFailed, m.reclaimed, m.publishLatency)
	return m
}

func (m *PromMetrics) TaskClaimed(n int) {
	m.claimed.Add(float64(n))
}
func (m *PromMetrics) TaskPublished() {
	m.published.Inc()
}
func (m *PromMetrics) TaskPublishFailed() {
	m.publishFailed.Inc()
}
func (m *PromMetrics) TasksReclaimed(n int) {
	m.reclaimed.Add(float64(n))
}
func (m *PromMetrics) PublishLatency(d time.Duration) {
	m.publishLatency.Observe(d.Seconds())
}
