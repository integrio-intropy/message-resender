package controller

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Metrics holds Prometheus metrics for the message-resender controller.
type Metrics struct {
	host      string
	processed *prometheus.CounterVec
	succeeded *prometheus.CounterVec
	failed    *prometheus.CounterVec
	lastRun   *prometheus.GaugeVec
	duration  *prometheus.HistogramVec
}

var labels = []string{"queue", "host"}

// NewMetrics creates and registers all message-resender metrics.
// host identifies the broker instance (e.g. "rabbit.example.com/production"
// for RabbitMQ or "mynamespace.servicebus.windows.net" for Azure Service Bus).
func NewMetrics(host string) *Metrics {
	m := &Metrics{
		host: host,
		processed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "resend_messages_processed_total",
			Help: "Total number of messages dequeued from a DLQ.",
		}, labels),
		succeeded: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "resend_messages_succeeded_total",
			Help: "Total number of messages successfully republished to the origin queue.",
		}, labels),
		failed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "resend_messages_failed_total",
			Help: "Total number of messages that failed to republish.",
		}, labels),
		lastRun: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "resend_last_run_timestamp_seconds",
			Help: "Unix timestamp of the last completed drain run per queue.",
		}, labels),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "resend_run_duration_seconds",
			Help:    "Duration of each DLQ drain run per queue.",
			Buckets: prometheus.DefBuckets,
		}, labels),
	}

	metrics.Registry.MustRegister(
		m.processed,
		m.succeeded,
		m.failed,
		m.lastRun,
		m.duration,
	)

	return m
}

// RecordDrain records all metrics for a completed drain run.
func (m *Metrics) RecordDrain(queue string, processed, succeeded, failed int64, durationSeconds float64) {
	m.processed.WithLabelValues(queue, m.host).Add(float64(processed))
	m.succeeded.WithLabelValues(queue, m.host).Add(float64(succeeded))
	m.failed.WithLabelValues(queue, m.host).Add(float64(failed))
	m.lastRun.WithLabelValues(queue, m.host).SetToCurrentTime()
	m.duration.WithLabelValues(queue, m.host).Observe(durationSeconds)
}
