package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ResendPolicySpec defines the desired state of ResendPolicy.
type ResendPolicySpec struct {
	// queueName is the origin queue whose dead-letter queue should be drained.
	// +kubebuilder:validation:MinLength=1
	QueueName string `json:"queueName"`

	// dlqName overrides the operator-level dlqPrefix for this queue.
	// When omitted, the DLQ name is derived as <dlqPrefix><queueName>.
	// +optional
	DLQName string `json:"dlqName,omitempty"`
}

// QueueStats reports message counts from the last drain cycle.
type QueueStats struct {
	// processed is the total number of messages picked up from the DLQ.
	Processed int64 `json:"processed"`

	// succeeded is the number of messages republished successfully.
	Succeeded int64 `json:"succeeded"`

	// failed is the number of messages that could not be republished.
	Failed int64 `json:"failed"`
}

// ResendPolicyStatus defines the observed state of ResendPolicy.
type ResendPolicyStatus struct {
	// conditions represent the current state of the ResendPolicy.
	// Known condition types: "Ready".
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// observedGeneration is the most recent generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// lastRunTime is the timestamp of the most recent completed drain cycle.
	// +optional
	LastRunTime *metav1.Time `json:"lastRunTime,omitempty"`

	// lastRunStats reports message counts from the most recent drain cycle.
	// +optional
	LastRunStats *QueueStats `json:"lastRunStats,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Queue",type=string,JSONPath=`.spec.queueName`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Last Run",type=date,JSONPath=`.status.lastRunTime`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ResendPolicy is the Schema for the resendpolicies API.
// It registers a single queue for periodic dead-letter draining
// across any supported message broker (RabbitMQ, Azure Service Bus).
type ResendPolicy struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of ResendPolicy
	// +required
	Spec ResendPolicySpec `json:"spec"`

	// status defines the observed state of ResendPolicy
	// +optional
	Status ResendPolicyStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// ResendPolicyList contains a list of ResendPolicy
type ResendPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []ResendPolicy `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ResendPolicy{}, &ResendPolicyList{})
}
