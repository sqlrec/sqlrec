# install docker first
# refer to https://minikube.sigs.k8s.io/docs/start/
if command -v minikube >/dev/null 2>&1; then
  echo 'exists minikube'
else
  curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
  sudo install minikube-linux-amd64 /usr/local/bin/minikube && rm minikube-linux-amd64
fi

minikube start --driver=docker --cpus='no-limit' --memory='no-limit' --mount --mount-string="${HOME}:${HOME}"
minikube addons enable storage-provisioner-rancher
#edit pv data path by: kubectl edit configmap/local-path-config -n local-path-storage

alias kubectl="minikube kubectl --"
echo 'alias kubectl="minikube kubectl --"' >> ~/.bash_profile

# refer to https://helm.sh/docs/intro/install/
if command -v helm >/dev/null 2>&1; then
  echo 'exists helm'
else
  curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
fi
