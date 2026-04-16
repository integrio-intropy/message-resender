//go:build e2e
// +build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/gomega"

	"github.com/integrio-intropy/message-resender/test/utils"
)

// getRabbitMQPodName returns the name of the running RabbitMQ pod.
func getRabbitMQPodName() string {
	cmd := exec.Command("kubectl", "get", "pods",
		"-l", "app=rabbitmq",
		"-n", namespace,
		"-o", "jsonpath={.items[0].metadata.name}",
	)
	output, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to get RabbitMQ pod name")
	return strings.TrimSpace(output)
}

// waitForRabbitMQReady polls until the RabbitMQ pod is Ready.
func waitForRabbitMQReady() string {
	var podName string
	Eventually(func(g Gomega) {
		podName = getRabbitMQPodName()
		cmd := exec.Command("kubectl", "get", "pod", podName,
			"-n", namespace,
			"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}",
		)
		output, err := utils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(output).To(Equal("True"), "RabbitMQ pod not ready")
	}, 3*time.Minute, 2*time.Second).Should(Succeed())
	return podName
}

// rabbitmqAdmin runs rabbitmqadmin inside the RabbitMQ pod.
func rabbitmqAdmin(podName string, args ...string) (string, error) {
	execArgs := []string{"exec", podName, "-n", namespace, "--", "rabbitmqadmin"}
	execArgs = append(execArgs, args...)
	cmd := exec.Command("kubectl", execArgs...)
	return utils.Run(cmd)
}

// declareQueue declares a durable queue via rabbitmqadmin.
func declareQueue(podName, queueName string) {
	_, err := rabbitmqAdmin(podName, "declare", "queue", fmt.Sprintf("name=%s", queueName), "durable=true")
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to declare queue %s", queueName)
}

// publishMessages publishes count test messages to the given queue.
func publishMessages(podName, queueName string, count int) {
	for i := range count {
		_, err := rabbitmqAdmin(podName, "publish",
			fmt.Sprintf("routing_key=%s", queueName),
			fmt.Sprintf("payload=test-message-%d", i),
		)
		ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to publish message %d to %s", i, queueName)
	}
}

// getQueueMessageCount returns the number of messages in a queue using the management HTTP API.
func getQueueMessageCount(podName, queueName string) int {
	cmd := exec.Command("kubectl", "exec", podName, "-n", namespace, "--",
		"curl", "-s", "-u", "guest:guest",
		fmt.Sprintf("http://localhost:15672/api/queues/%%2f/%s", queueName),
	)
	output, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to query queue %s", queueName)

	var result struct {
		Messages int `json:"messages"`
	}
	ExpectWithOffset(1, json.Unmarshal([]byte(output), &result)).To(Succeed(), "Failed to parse queue info for %s", queueName)
	return result.Messages
}

// queueExists checks whether a queue exists in RabbitMQ.
func queueExists(podName, queueName string) bool {
	cmd := exec.Command("kubectl", "exec", podName, "-n", namespace, "--",
		"curl", "-s", "-o", "/dev/null", "-w", "%{http_code}",
		"-u", "guest:guest",
		fmt.Sprintf("http://localhost:15672/api/queues/%%2f/%s", queueName),
	)
	output, err := utils.Run(cmd)
	if err != nil {
		return false
	}
	return strings.TrimSpace(output) == "200"
}

// deleteQueue deletes a queue from RabbitMQ.
func deleteQueue(podName, queueName string) {
	if queueExists(podName, queueName) {
		_, _ = rabbitmqAdmin(podName, "delete", "queue", fmt.Sprintf("name=%s", queueName))
	}
}

// applyResendPolicy creates a ResendPolicy CR via kubectl in the given namespace.
func applyResendPolicy(name, ns, queueName string) {
	yaml := fmt.Sprintf(`apiVersion: messaging.intropy.io/v1
kind: ResendPolicy
metadata:
  name: %s
  namespace: %s
spec:
  queueName: %s
`, name, ns, queueName)

	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(yaml)
	_, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to apply ResendPolicy %s", name)
}

// deleteResendPolicy deletes a ResendPolicy CR in the given namespace.
func deleteResendPolicy(name, ns string) {
	cmd := exec.Command("kubectl", "delete", "resendpolicy", name,
		"-n", ns, "--ignore-not-found",
	)
	_, _ = utils.Run(cmd)
}

// waitForResendPolicyReady polls until the ResendPolicy has Ready=True.
func waitForResendPolicyReady(name, ns string) {
	Eventually(func(g Gomega) {
		cmd := exec.Command("kubectl", "get", "resendpolicy", name,
			"-n", ns,
			"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}",
		)
		output, err := utils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(strings.TrimSpace(output)).To(Equal("True"),
			"ResendPolicy %s not Ready", name)
	}, 60*time.Second, 2*time.Second).Should(Succeed())
}

// resendPolicyStats holds the lastRunStats from a ResendPolicy status.
type resendPolicyStats struct {
	Processed int64
	Succeeded int64
	Failed    int64
}

// getResendPolicyLastRunStats returns the lastRunStats from the ResendPolicy status.
func getResendPolicyLastRunStats(name, ns string) resendPolicyStats {
	cmd := exec.Command("kubectl", "get", "resendpolicy", name,
		"-n", ns,
		"-o", "jsonpath={.status.lastRunStats.processed} {.status.lastRunStats.succeeded} {.status.lastRunStats.failed}",
	)
	output, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to get lastRunStats for %s", name)

	parts := strings.Fields(strings.TrimSpace(output))
	ExpectWithOffset(1, parts).To(HaveLen(3), "Expected 3 stats fields, got: %q", output)

	processed, err := strconv.ParseInt(parts[0], 10, 64)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	succeeded, err := strconv.ParseInt(parts[1], 10, 64)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	failed, err := strconv.ParseInt(parts[2], 10, 64)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())

	return resendPolicyStats{
		Processed: processed,
		Succeeded: succeeded,
		Failed:    failed,
	}
}
