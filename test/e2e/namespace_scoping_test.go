//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/integrio-intropy/message-resender/test/utils"
)

var _ = Describe("Namespace scoping", Ordered, func() {
	var rabbitPod string

	BeforeAll(func() {
		By("waiting for RabbitMQ to be ready")
		rabbitPod = waitForRabbitMQReady()

		By("creating unwatched namespace")
		cmd := exec.Command("kubectl", "create", "ns", unwatchedNamespace)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to create unwatched namespace")

		By("labeling the unwatched namespace with restricted security policy")
		cmd = exec.Command("kubectl", "label", "--overwrite", "ns", unwatchedNamespace,
			"pod-security.kubernetes.io/enforce=restricted")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to label unwatched namespace")
	})

	AfterAll(func() {
		By("removing unwatched namespace")
		cmd := exec.Command("kubectl", "delete", "ns", unwatchedNamespace, "--ignore-not-found")
		_, _ = utils.Run(cmd)
	})

	Context("watched namespace is reconciled", func() {
		const (
			policyName  = "scope-test"
			originQueue = "scope-origin"
			dlqQueue    = "dlq-scope-origin"
		)

		BeforeEach(func() {
			declareQueue(rabbitPod, originQueue)
			declareQueue(rabbitPod, dlqQueue)
		})

		AfterEach(func() {
			deleteResendPolicy(policyName, watchedNamespace)
			deleteQueue(rabbitPod, dlqQueue)
			deleteQueue(rabbitPod, originQueue)
		})

		It("should drain messages for a ResendPolicy in the watched namespace", func() {
			By("publishing 3 messages to the DLQ")
			publishMessages(rabbitPod, dlqQueue, 3)

			By("applying a ResendPolicy in the watched namespace")
			applyResendPolicy(policyName, watchedNamespace, originQueue)

			By("waiting for the ResendPolicy to become Ready")
			waitForResendPolicyReady(policyName, watchedNamespace)

			By("verifying messages were drained")
			Eventually(func(g Gomega) {
				g.Expect(getQueueMessageCount(rabbitPod, originQueue)).To(Equal(3))
				g.Expect(getQueueMessageCount(rabbitPod, dlqQueue)).To(Equal(0))
			}, 30*time.Second, 2*time.Second).Should(Succeed())
		})
	})

	Context("unwatched namespace is ignored", func() {
		const (
			policyName  = "unwatched-test"
			originQueue = "unwatched-origin"
			dlqQueue    = "dlq-unwatched-origin"
		)

		BeforeEach(func() {
			declareQueue(rabbitPod, originQueue)
			declareQueue(rabbitPod, dlqQueue)
		})

		AfterEach(func() {
			deleteResendPolicy(policyName, unwatchedNamespace)
			deleteQueue(rabbitPod, dlqQueue)
			deleteQueue(rabbitPod, originQueue)
		})

		It("should not reconcile a ResendPolicy in an unwatched namespace", func() {
			By("publishing 2 messages to the DLQ")
			publishMessages(rabbitPod, dlqQueue, 2)

			By("applying a ResendPolicy in the unwatched namespace")
			applyResendPolicy(policyName, unwatchedNamespace, originQueue)

			By("waiting 20s (4x drain interval) to confirm no reconciliation")
			Consistently(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "resendpolicy", policyName,
					"-n", unwatchedNamespace,
					"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}",
				)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(output)).To(BeEmpty(),
					"ResendPolicy in unwatched namespace should not have Ready condition")
			}, 20*time.Second, 2*time.Second).Should(Succeed())

			By("verifying DLQ messages are untouched")
			Expect(getQueueMessageCount(rabbitPod, dlqQueue)).To(Equal(2))
		})
	})

	Context("RBAC enforces namespace isolation", func() {
		It("should allow access to the watched namespace", func() {
			cmd := exec.Command("kubectl", "auth", "can-i", "list",
				"resendpolicies.messaging.intropy.io",
				"-n", watchedNamespace,
				fmt.Sprintf("--as=system:serviceaccount:%s:%s", namespace, serviceAccountName),
			)
			output, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(output)).To(Equal("yes"))
		})

		It("should deny access to the unwatched namespace", func() {
			cmd := exec.Command("kubectl", "auth", "can-i", "list",
				"resendpolicies.messaging.intropy.io",
				"-n", unwatchedNamespace,
				fmt.Sprintf("--as=system:serviceaccount:%s:%s", namespace, serviceAccountName),
			)
			// kubectl auth can-i exits non-zero for "no"
			output, _ := cmd.CombinedOutput()
			Expect(strings.TrimSpace(string(output))).To(Equal("no"))
		})
	})
})
