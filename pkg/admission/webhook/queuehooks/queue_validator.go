// Copyright 2025 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package queuehooks

import (
	"context"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	v2 "github.com/kai-scheduler/KAI-scheduler/pkg/apis/scheduling/v2"
	"github.com/kai-scheduler/KAI-scheduler/pkg/common/constants"
)

var queueValidatorLog = logf.Log.WithName("queue-validator")

const missingResourcesError = "resources must be specified"

type QueueValidator interface {
	ValidateCreate(ctx context.Context, obj *v2.Queue) (warnings admission.Warnings, err error)
	ValidateUpdate(ctx context.Context, oldObj, newObj *v2.Queue) (warnings admission.Warnings, err error)
	ValidateDelete(ctx context.Context, obj *v2.Queue) (warnings admission.Warnings, err error)
}

type queueValidator struct {
	kubeClient            client.Client
	enableQuotaValidation bool
}

func NewQueueValidator(kubeClient client.Client, enableQuotaValidation bool) QueueValidator {
	return &queueValidator{
		kubeClient:            kubeClient,
		enableQuotaValidation: enableQuotaValidation,
	}
}

func (v *queueValidator) ValidateCreate(ctx context.Context, queue *v2.Queue) (admission.Warnings, error) {
	queueValidatorLog.Info("validate create", "name", queue.Name)

	if queue.Spec.Resources == nil {
		return []string{missingResourcesError}, fmt.Errorf(missingResourcesError)
	}

	if !v.enableQuotaValidation || queue.Spec.ParentQueue == "" {
		return nil, nil
	}

	return v.validateParentChildQuota(ctx, queue)
}

func (v *queueValidator) ValidateUpdate(ctx context.Context, oldQueue, newQueue *v2.Queue) (admission.Warnings, error) {
	queueValidatorLog.Info("validate update", "name", newQueue.Name)

	if newQueue.Spec.Resources == nil {
		return []string{missingResourcesError}, fmt.Errorf(missingResourcesError)
	}

	if !v.enableQuotaValidation {
		return nil, nil
	}

	var warnings admission.Warnings

	if newQueue.Spec.ParentQueue != "" {
		parentWarnings, err := v.validateParentChildQuota(ctx, newQueue)
		if err != nil {
			return parentWarnings, err
		}
		warnings = append(warnings, parentWarnings...)
	}

	if len(oldQueue.Status.ChildQueues) > 0 {
		childWarnings, err := v.validateChildrenQuotaSum(ctx, newQueue)
		if err != nil {
			return childWarnings, err
		}
		warnings = append(warnings, childWarnings...)
	}

	return warnings, nil
}

func (v *queueValidator) ValidateDelete(ctx context.Context, queue *v2.Queue) (admission.Warnings, error) {
	queueValidatorLog.Info("validate delete", "name", queue.Name)

	if len(queue.Status.ChildQueues) > 0 {
		return nil, fmt.Errorf("cannot delete queue %s: it has child queues %v", queue.Name, queue.Status.ChildQueues)
	}

	return nil, nil
}

func (v *queueValidator) validateParentChildQuota(ctx context.Context, childQueue *v2.Queue) (admission.Warnings, error) {
	parentQueue := &v2.Queue{}
	err := v.kubeClient.Get(ctx, client.ObjectKey{Name: childQueue.Spec.ParentQueue}, parentQueue)
	if err != nil {
		return nil, fmt.Errorf("failed to get parent queue %s: %w", childQueue.Spec.ParentQueue, err)
	}

	if parentQueue.Spec.Resources == nil {
		return nil, fmt.Errorf("parent queue %s has no resources defined", parentQueue.Name)
	}

	var warnings []string

	childCPU := childQueue.Spec.Resources.CPU.Quota
	parentCPU := parentQueue.Spec.Resources.CPU.Quota
	childGPU := childQueue.Spec.Resources.GPU.Quota
	parentGPU := parentQueue.Spec.Resources.GPU.Quota
	childMemory := childQueue.Spec.Resources.Memory.Quota
	parentMemory := parentQueue.Spec.Resources.Memory.Quota

	if quotaExceeds(childCPU, parentCPU) {
		warnings = append(warnings, fmt.Sprintf("child queue CPU quota (%.0f) exceeds parent queue %s CPU quota (%.0f)",
			childCPU, parentQueue.Name, parentCPU))
	}

	if quotaExceeds(childGPU, parentGPU) {
		warnings = append(warnings, fmt.Sprintf("child queue GPU quota (%.2f) exceeds parent queue %s GPU quota (%.2f)",
			childGPU, parentQueue.Name, parentGPU))
	}

	if quotaExceeds(childMemory, parentMemory) {
		warnings = append(warnings, fmt.Sprintf("child queue Memory quota (%.0f) exceeds parent queue %s Memory quota (%.0f)",
			childMemory, parentQueue.Name, parentMemory))
	}

	totalChildrenCPU := addQuota(0, childCPU)
	totalChildrenGPU := addQuota(0, childGPU)
	totalChildrenMemory := addQuota(0, childMemory)
	for _, childName := range parentQueue.Status.ChildQueues {
		if childName == childQueue.Name {
			continue
		}

		existingChild := &v2.Queue{}
		if err := v.kubeClient.Get(ctx, client.ObjectKey{Name: childName}, existingChild); err != nil {
			queueValidatorLog.Error(err, "failed to get child queue", "child", childName)
			continue
		}

		if existingChild.Spec.Resources != nil {
			totalChildrenCPU = addQuota(totalChildrenCPU, existingChild.Spec.Resources.CPU.Quota)
			totalChildrenGPU = addQuota(totalChildrenGPU, existingChild.Spec.Resources.GPU.Quota)
			totalChildrenMemory = addQuota(totalChildrenMemory, existingChild.Spec.Resources.Memory.Quota)
		}
	}

	if quotaExceeds(totalChildrenCPU, parentCPU) {
		warnings = append(warnings, fmt.Sprintf("total children CPU quota (%.0f) exceeds parent queue %s CPU quota (%.0f)",
			totalChildrenCPU, parentQueue.Name, parentCPU))
	}

	if quotaExceeds(totalChildrenGPU, parentGPU) {
		warnings = append(warnings, fmt.Sprintf("total children GPU quota (%.2f) exceeds parent queue %s GPU quota (%.2f)",
			totalChildrenGPU, parentQueue.Name, parentGPU))
	}

	if quotaExceeds(totalChildrenMemory, parentMemory) {
		warnings = append(warnings, fmt.Sprintf("total children Memory quota (%.0f) exceeds parent queue %s Memory quota (%.0f)",
			totalChildrenMemory, parentQueue.Name, parentMemory))
	}

	return warnings, nil
}

func (v *queueValidator) validateChildrenQuotaSum(ctx context.Context, parentQueue *v2.Queue) (admission.Warnings, error) {
	if parentQueue.Spec.Resources == nil {
		return nil, fmt.Errorf("parent queue %s has no resources defined", parentQueue.Name)
	}

	var warnings []string
	var totalChildrenCPU, totalChildrenGPU, totalChildrenMemory float64

	parentCPU := parentQueue.Spec.Resources.CPU.Quota
	parentGPU := parentQueue.Spec.Resources.GPU.Quota
	parentMemory := parentQueue.Spec.Resources.Memory.Quota

	for _, childName := range parentQueue.Status.ChildQueues {
		child := &v2.Queue{}
		if err := v.kubeClient.Get(ctx, client.ObjectKey{Name: childName}, child); err != nil {
			queueValidatorLog.Error(err, "failed to get child queue", "child", childName)
			continue
		}

		if child.Spec.Resources == nil {
			continue
		}

		totalChildrenCPU = addQuota(totalChildrenCPU, child.Spec.Resources.CPU.Quota)
		totalChildrenGPU = addQuota(totalChildrenGPU, child.Spec.Resources.GPU.Quota)
		totalChildrenMemory = addQuota(totalChildrenMemory, child.Spec.Resources.Memory.Quota)

		if quotaExceeds(child.Spec.Resources.CPU.Quota, parentCPU) {
			warnings = append(warnings, fmt.Sprintf("child queue %s CPU quota (%.0f) exceeds parent CPU quota (%.0f)",
				childName, child.Spec.Resources.CPU.Quota, parentCPU))
		}
	}

	if quotaExceeds(totalChildrenCPU, parentCPU) {
		warnings = append(warnings, fmt.Sprintf("total children CPU quota (%.0f) exceeds parent CPU quota (%.0f)",
			totalChildrenCPU, parentCPU))
	}

	if quotaExceeds(totalChildrenGPU, parentGPU) {
		warnings = append(warnings, fmt.Sprintf("total children GPU quota (%.2f) exceeds parent GPU quota (%.2f)",
			totalChildrenGPU, parentGPU))
	}

	if quotaExceeds(totalChildrenMemory, parentMemory) {
		warnings = append(warnings, fmt.Sprintf("total children Memory quota (%.0f) exceeds parent Memory quota (%.0f)",
			totalChildrenMemory, parentMemory))
	}

	return warnings, nil
}

// quotaExceeds treats -1 as unlimited: nothing exceeds an unlimited parent,
// and an unlimited quota always exceeds a finite parent.
func quotaExceeds(quota, parentQuota float64) bool {
	if parentQuota == constants.UnlimitedResourceQuantity {
		return false
	}
	if quota == constants.UnlimitedResourceQuantity {
		return true
	}
	return quota > parentQuota
}

// addQuota is absorbing for -1: any unlimited term makes the total unlimited.
func addQuota(total, quota float64) float64 {
	if total == constants.UnlimitedResourceQuantity || quota == constants.UnlimitedResourceQuantity {
		return constants.UnlimitedResourceQuantity
	}
	return total + quota
}
