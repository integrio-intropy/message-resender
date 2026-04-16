//go:build e2e
// +build e2e

package e2e

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("ResendPolicy drain", Ordered, func() {
	var rabbitPod string

	BeforeAll(func() {
		By("waiting for RabbitMQ to be ready")
		rabbitPod = waitForRabbitMQReady()
	})

	Context("happy path drain", func() {
		const (
			policyName = "orders-drain"
			originQueue = "orders"
			dlqQueue    = "dlq-orders"
		)

		BeforeEach(func() {
			By("declaring origin and DLQ queues")
			declareQueue(rabbitPod, originQueue)
			declareQueue(rabbitPod, dlqQueue)
		})

		AfterEach(func() {
			deleteResendPolicy(policyName, namespace)
			deleteQueue(rabbitPod, dlqQueue)
			deleteQueue(rabbitPod, originQueue)
		})

		It("should drain messages from DLQ to origin queue", func() {
			By("publishing 5 messages to the DLQ")
			publishMessages(rabbitPod, dlqQueue, 5)
			Expect(getQueueMessageCount(rabbitPod, dlqQueue)).To(Equal(5))

			By("applying a ResendPolicy for the origin queue")
			applyResendPolicy(policyName, namespace, originQueue)

			By("waiting for the ResendPolicy to become Ready")
			waitForResendPolicyReady(policyName, namespace)

			By("verifying messages were moved to the origin queue")
			Eventually(func(g Gomega) {
				g.Expect(getQueueMessageCount(rabbitPod, originQueue)).To(Equal(5))
				g.Expect(getQueueMessageCount(rabbitPod, dlqQueue)).To(Equal(0))
			}, 30*time.Second, 2*time.Second).Should(Succeed())
		})
	})

	Context("empty DLQ", func() {
		const (
			policyName  = "invoices-drain"
			originQueue = "invoices"
			dlqQueue    = "dlq-invoices"
		)

		BeforeEach(func() {
			By("declaring origin and empty DLQ queues")
			declareQueue(rabbitPod, originQueue)
			declareQueue(rabbitPod, dlqQueue)
		})

		AfterEach(func() {
			deleteResendPolicy(policyName, namespace)
			deleteQueue(rabbitPod, dlqQueue)
			deleteQueue(rabbitPod, originQueue)
		})

		It("should become Ready with zero processed messages", func() {
			By("applying a ResendPolicy for a queue with an empty DLQ")
			applyResendPolicy(policyName, namespace, originQueue)

			By("waiting for the ResendPolicy to become Ready")
			waitForResendPolicyReady(policyName, namespace)

			By("verifying lastRunStats shows zero processed")
			stats := getResendPolicyLastRunStats(policyName, namespace)
			Expect(stats.Processed).To(Equal(int64(0)))
		})
	})

	Context("status reports correct counts", func() {
		const (
			policyName  = "payments-drain"
			originQueue = "payments"
			dlqQueue    = "dlq-payments"
		)

		BeforeEach(func() {
			By("declaring origin and DLQ queues")
			declareQueue(rabbitPod, originQueue)
			declareQueue(rabbitPod, dlqQueue)
		})

		AfterEach(func() {
			deleteResendPolicy(policyName, namespace)
			deleteQueue(rabbitPod, dlqQueue)
			deleteQueue(rabbitPod, originQueue)
		})

		It("should report accurate lastRunStats after draining", func() {
			By("publishing 3 messages to the DLQ")
			publishMessages(rabbitPod, dlqQueue, 3)

			By("applying a ResendPolicy")
			applyResendPolicy(policyName, namespace, originQueue)

			By("waiting for the ResendPolicy to become Ready")
			waitForResendPolicyReady(policyName, namespace)

			By("verifying lastRunStats matches exactly")
			Eventually(func(g Gomega) {
				stats := getResendPolicyLastRunStats(policyName, namespace)
				g.Expect(stats.Processed).To(Equal(int64(3)))
				g.Expect(stats.Succeeded).To(Equal(int64(3)))
				g.Expect(stats.Failed).To(Equal(int64(0)))
			}, 30*time.Second, 2*time.Second).Should(Succeed())
		})
	})
})
