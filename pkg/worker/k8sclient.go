package worker

import (
	"fmt"
	"os"
	"path/filepath"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// NewKubeClient builds a kubernetes.Interface, preferring in-cluster
// ServiceAccount credentials and falling back to a local kubeconfig file
// (honoring $KUBECONFIG, then ~/.kube/config) when not running inside a
// cluster. NodeSentinel implements this independently of NodeVault.
func NewKubeClient() (kubernetes.Interface, error) {
	restCfg, err := rest.InClusterConfig()
	if err != nil {
		restCfg, err = buildOutOfClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("load kube config: %w", err)
		}
	}

	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("build k8s client: %w", err)
	}
	return clientset, nil
}

func buildOutOfClusterConfig() (*rest.Config, error) {
	kubeconfigPath := os.Getenv("KUBECONFIG")
	if kubeconfigPath == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("resolve home dir: %w", err)
		}
		kubeconfigPath = filepath.Join(home, ".kube", "config")
	}
	return clientcmd.BuildConfigFromFlags("", kubeconfigPath)
}
