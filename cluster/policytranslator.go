package cluster

import (
	"fmt"

	"github.com/weaveworks/flux/policy"
	"github.com/weaveworks/flux/resource"
)

// An Annotation change indicates how an annotation should be changed
type AnnotationChange struct {
	AnnotationKey string
	// AnnotationValue is the value to set the annotation to, nil indicates delete
	AnnotationValue *string
}

func (ac AnnotationChange) String() string {
	var value string
	if ac.AnnotationValue != nil {
		value = *ac.AnnotationValue
	}
	return fmt.Sprintf("%s=%s", ac.AnnotationKey, value)
}

type PolicyTranslator interface {
	// GetAnnotationChangesForPolicyUpdate translates a policy update into annotation updates
	GetAnnotationChangesForPolicyUpdate(workload resource.Workload, update policy.Update) ([]AnnotationChange, error)
	// GetPolicyUpdateForAnnotationChange translates an annotation update into an annotation change
	GetPolicyUpdateForAnnotationChange(change AnnotationChange) (policy.Update, error)
}
