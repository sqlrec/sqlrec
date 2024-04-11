#!/bin/bash
shopt -s expand_aliases
source ~/.bash_profile

kubectl create namespace sqlrec

bash ./minio/deploy.sh sqlrec
bash ./juicefs/deploy.sh sqlrec
bash ./kafka/deploy.sh sqlrec
bash ./redis/deploy.sh sqlrec
bash ./hms/deploy.sh sqlrec
bash ./kyuubi/deploy.sh sqlrec

