#!/bin/bash

bash ./minikube/deploy.sh
bash ./local-path-storage/deploy.sh
bash ./helm/deploy.sh
