package controller

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	messagingv1 "github.com/integrio-intropy/message-resender/api/v1"
	"github.com/integrio-intropy/message-resender/internal/broker"
)

// fakeBroker implements broker.Broker for testing.
type fakeBroker struct {
	result broker.DrainResult
	err    error
}

func (f *fakeBroker) Drain(_ context.Context, _ broker.QueueInfo, _ time.Time) (broker.DrainResult, error) {
	return f.result, f.err
}

func (f *fakeBroker) Close() error { return nil }

var _ = Describe("ResendPolicy Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}
		resendpolicy := &messagingv1.ResendPolicy{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind ResendPolicy")
			err := k8sClient.Get(ctx, typeNamespacedName, resendpolicy)
			if err != nil && errors.IsNotFound(err) {
				resource := &messagingv1.ResendPolicy{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: "default",
					},
					Spec: messagingv1.ResendPolicySpec{
						QueueName: "test-queue",
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			resource := &messagingv1.ResendPolicy{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance ResendPolicy")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})

		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := &ResendPolicyReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
				Broker: &fakeBroker{
					result: broker.DrainResult{Processed: 3, Succeeded: 3},
				},
				DrainInterval:       5 * time.Minute,
				MaxConcurrentDrains: 10,
			}

			result, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(5 * time.Minute))

			By("Checking status was updated")
			var updated messagingv1.ResendPolicy
			Expect(k8sClient.Get(ctx, typeNamespacedName, &updated)).To(Succeed())
			Expect(updated.Status.LastRunTime).NotTo(BeNil())
			Expect(updated.Status.LastRunStats).NotTo(BeNil())
			Expect(updated.Status.LastRunStats.Processed).To(Equal(int64(3)))
			Expect(updated.Status.LastRunStats.Succeeded).To(Equal(int64(3)))
			Expect(updated.Status.Conditions).To(HaveLen(1))
			Expect(updated.Status.Conditions[0].Type).To(Equal("Ready"))
			Expect(updated.Status.Conditions[0].Status).To(Equal(metav1.ConditionTrue))
		})

		It("should set Ready=False on drain error", func() {
			controllerReconciler := &ResendPolicyReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
				Broker: &fakeBroker{
					err: context.DeadlineExceeded,
				},
				DrainInterval:       5 * time.Minute,
				MaxConcurrentDrains: 10,
			}

			result, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(5 * time.Minute))

			var updated messagingv1.ResendPolicy
			Expect(k8sClient.Get(ctx, typeNamespacedName, &updated)).To(Succeed())
			Expect(updated.Status.Conditions).To(HaveLen(1))
			Expect(updated.Status.Conditions[0].Type).To(Equal("Ready"))
			Expect(updated.Status.Conditions[0].Status).To(Equal(metav1.ConditionFalse))
			Expect(updated.Status.Conditions[0].Reason).To(Equal("DrainFailed"))
		})
	})
})
