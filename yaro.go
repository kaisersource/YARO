


package main

import (
    "fmt"
    "github.com/operator-framework/operator-sdk/pkg/sdk"
    "github.com/operator-framework/operator-sdk/pkg/util/k8sutil"
    appsv1 "k8s.io/api/apps/v1"
    corev1 "k8s.io/api/core/v1"
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
    "k8s.io/apimachinery/pkg/labels"
    "k8s.io/apimachinery/pkg/types"
)

// RedisCluster is the custom resource.
type RedisCluster struct {
    metav1.TypeMeta   `json:",inline"`
    metav1.ObjectMeta `json:"metadata"`
    Spec              RedisClusterSpec   `json:"spec"`
    Status            RedisClusterStatus `json:"status,omitempty"`
}

// RedisClusterSpec is the spec for a RedisCluster resource.
type RedisClusterSpec struct {
    Size int32 `json:"size"`
}

// RedisClusterStatus is the status for a RedisCluster resource.
type RedisClusterStatus struct {
    Nodes []string `json:"nodes"`
}

// RedisClusterHandler is an implementation of the RedisClusterHandler interface.
type RedisClusterHandler struct {}

// NewHandler returns a new instance of the RedisClusterHandler.
func NewHandler() sdk.Handler {
    return &RedisClusterHandler{}
}

// Handle handles the RedisCluster custom resource.
func (h *RedisClusterHandler) Handle(ctx sdk.Context, event sdk.Event) error {
    switch o := event.Object.(type) {
    case *RedisCluster:
        return h.handleRedisCluster(ctx, o)
    case *appsv1.Deployment:
        return h.handleDeployment(ctx, o)
    }
    return nil
}

// handleRedisCluster handles the RedisCluster custom resource.
func (h *RedisClusterHandler) handleRedisCluster(ctx sdk.Context, cluster *RedisCluster) error {
    // Get the namespace for the custom resource
    namespace, err := k8sutil.GetWatchNamespace()
    if err != nil {
        return err
    }

    // Create a deployment for the Redis cluster
    name := cluster.ObjectMeta.Name
    replicas := cluster.Spec.Size
    labels := map[string]string{"app": name, "controller": name}
    deployment := &appsv1.Deployment{
        ObjectMeta: metav1.ObjectMeta{
            Name:      name,
            Namespace: namespace,
            Labels:    labels,
        },
        Spec: appsv1.DeploymentSpec{
            Replicas: &replicas,
            Selector: &metav1.LabelSelector{
                MatchLabels: labels,
            },
            Template: corev1.PodTemplateSpec{
                ObjectMeta: metav1.ObjectMeta{
                    Labels: labels,
                },
                Spec: corev1.PodSpec{
                    Containers: []corev1.Container{{
                        Name:  "redis",
                        Image: "redis:latest",
                    }},
                },
            },
        },
    }
    
    // Update the status of the custom resource
    err = updateRedisClusterStatus(ctx, namespace, name, deployment.Status.Replicas)
    if err != nil {
        return err
    }

    // Create/update the deployment
    err = sdk.Create(deployment)
    if err != nil {
        return err
    }

    return nil
}

// handleDeployment handles the Deployment custom resource.
func (h *RedisClusterHandler) handleDeployment(ctx sdk.Context, deployment *appsv1.Deployment) error {
    // Get the namespace for the custom resource
    namespace, err := k8sutil.GetWatchNamespace()
    if err != nil {
        return err
    }

    // Get the labels for the deployment
    labels := deployment.Spec.Selector.MatchLabels

    // Get the corresponding RedisCluster
    cluster := &RedisCluster{}
    err = sdk.Get(cluster, namespace, labels["controller"])
    if err != nil {
        return err
    }

    // Update the status of the custom resource
    err = updateRedisClusterStatus(ctx, namespace, labels["controller"], deployment.Status.Replicas)
    if err != nil {
        return err
    }

    // Perform the automatic failover
    err = performAutomaticFailover(ctx, deployment)
    if err != nil {
        return err
    }

    return nil
}

// updateRedisClusterStatus updates the status of the RedisCluster custom resource.
func updateRedisClusterStatus(ctx sdk.Context, namespace, name string, replicas *int32) error {
    // Get the RedisCluster
    cluster := &RedisCluster{}
    err := sdk.Get(cluster, namespace, name)
    if err != nil {
        return err
    }

    // Update the status of the custom resource
    cluster.Status.Nodes = make([]string, *replicas)
    for i := 0; i < int(*replicas); i++ {
        cluster.Status.Nodes[i] = fmt.Sprintf("%s-%d", name, i)
    }

    err = sdk.Update(cluster)
    if err != nil {
        return err
    }

    return nil
}

// performAutomaticFailover performs the automatic failover for the Redis cluster.
func performAutomaticFailover(ctx sdk.Context, deployment *appsv1.Deployment) error {
    // Get the namespace for the custom resource
    namespace, err := k8sutil.GetWatchNamespace()
    if err != nil {
        return err
    }

    // Get the labels for the deployment
    labels := deployment.Spec.Selector.MatchLabels

    // Get the corresponding RedisCluster
    cluster := &RedisCluster{}
    err = sdk.Get(cluster, namespace, labels["controller"])
    if err != nil {
        return err
    }

    // Get the pods for the deployment
    selector := labels.AsSelector()
    pods, err := ctx.GetClientset().CoreV1().Pods(namespace).List(metav1.ListOptions{LabelSelector: selector.String()})
    if err != nil {
        return err
    }

    // Perform the automatic failover
    for _, pod := range pods.Items {
        // Check if the pod is ready
        ready := false
        for _, condition := range pod.Status.Conditions {
            if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
                ready = true
                break
            }
        }

        // If the pod is not ready, delete it
        if !ready {
            err = ctx.GetClientset().CoreV1().Pods(namespace).Delete(pod.Name, &metav1.DeleteOptions{})
            if err != nil {
                return err
            }
        }
    }

    return nil
}
