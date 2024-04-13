# install docker first
# refer to https://minikube.sigs.k8s.io/docs/start/
if command -v minikube >/dev/null 2>&1; then
  echo 'skip install minikube'
else
  curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
  sudo install minikube-linux-amd64 /usr/local/bin/minikube && rm minikube-linux-amd64
  echo 'alias kubectl="minikube kubectl --"' >> ~/.bash_profile
fi

minikube start --driver=docker --cpus='no-limit' --memory='no-limit' --mount --mount-string="${HOME}:${HOME}"
minikube addons enable storage-provisioner-rancher
alias kubectl="minikube kubectl --"

# todo
# edit pv data path by: kubectl edit configmap/local-path-config -n local-path-storage, then restart local-path-provisioner pod

# refer to https://helm.sh/docs/intro/install/
if command -v helm >/dev/null 2>&1; then
  echo 'skip install helm'
else
  curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
fi
