package controller

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlcontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	messagingv1 "github.com/integrio-intropy/message-resender/api/v1"
	"github.com/integrio-intropy/message-resender/internal/broker"
)

// ResendPolicyReconciler reconciles a ResendPolicy object.
type ResendPolicyReconciler struct {
	client.Client
	Scheme              *runtime.Scheme
	Broker              broker.Broker
	DrainInterval       time.Duration
	MaxConcurrentDrains int
	Metrics             *Metrics
}

// +kubebuilder:rbac:groups=messaging.intropy.io,resources=resendpolicies,verbs=get;list;watch
// +kubebuilder:rbac:groups=messaging.intropy.io,resources=resendpolicies/status,verbs=get;update;patch

// Reconcile drains the dead-letter queue for the given ResendPolicy and updates its status.
func (r *ResendPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var policy messagingv1.ResendPolicy
	if err := r.Get(ctx, req.NamespacedName, &policy); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	queueInfo := broker.QueueInfo{
		QueueName:   policy.Spec.QueueName,
		DLQOverride: policy.Spec.DLQName,
	}

	runStart := time.Now()
	log.V(1).Info("starting drain", "queue", queueInfo.QueueName)

	result, drainErr := r.Broker.Drain(ctx, queueInfo, runStart)
	drainDuration := time.Since(runStart).Seconds()

	if r.Metrics != nil {
		r.Metrics.RecordDrain(queueInfo.QueueName, result.Processed, result.Succeeded, result.Failed, drainDuration)
	}

	// Always update status, even on error.
	now := metav1.Now()
	policy.Status.ObservedGeneration = policy.Generation
	policy.Status.LastRunTime = &now
	policy.Status.LastRunStats = &messagingv1.QueueStats{
		Processed: result.Processed,
		Succeeded: result.Succeeded,
		Failed:    result.Failed,
	}

	if drainErr != nil {
		log.Error(drainErr, "drain failed", "queue", queueInfo.QueueName)
		meta.SetStatusCondition(&policy.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionFalse,
			ObservedGeneration: policy.Generation,
			Reason:             "DrainFailed",
			Message:            drainErr.Error(),
		})
	} else {
		log.V(1).Info("drain completed",
			"queue", queueInfo.QueueName,
			"processed", result.Processed,
			"succeeded", result.Succeeded,
			"failed", result.Failed)
		meta.SetStatusCondition(&policy.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionTrue,
			ObservedGeneration: policy.Generation,
			Reason:             "DrainSucceeded",
			Message: fmt.Sprintf("Processed %d messages (%d succeeded, %d failed)",
				result.Processed, result.Succeeded, result.Failed),
		})
	}

	if err := r.Status().Update(ctx, &policy); err != nil {
		log.Error(err, "failed to update ResendPolicy status")
	}

	return ctrl.Result{RequeueAfter: r.DrainInterval}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ResendPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&messagingv1.ResendPolicy{}).
		Named("resendpolicy").
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		WithOptions(ctrlcontroller.Options{
			MaxConcurrentReconciles: r.MaxConcurrentDrains,
		}).
		Complete(r)
}
