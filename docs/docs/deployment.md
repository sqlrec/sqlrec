# 服务部署
SqlRec目前支持AMD64的Linux系统，后续会支持MacOS。注意，部署需要至少32GB的内存、256GB磁盘空间、可靠的互联网连接（如果使用加速器，注意使用tun模式）。

按下述命令部署SqlRec系统：
```bash
# clone sqlrec repository
git clone https://github.com/antgroup/sqlrec.git
cd ./sqlrec/deploy

# deploy minikube
./deploy_minikube.sh

# verify pod status, wait all pod ready
alias kubectl="minikube kubectl --"
kubectl get pod --ALL

# download resource
./download_resource.sh

# deploy sqlrec and dependencies services
./deploy_components.sh

# verify pod status, wait all pod ready
kubectl get pod --ALL

# verify sqlrec service
cd ..
bash ./bin/beeline.sh
```
注意：
- 上述基于minikube的部署方案仅用于测试，生产环境需要先部署可靠的大数据基础设施，然后参考deploy下的脚本初始化数据库、部署SqlRec deployment
- 如果需要重新部署，可以先通过minikube delete删除集群
- 有一些组件没有默认部署，比如kyuubi、jupyter等，如果需要，可以在deploy目录执行对应的部署脚本，比如`bash ./kyuubi/deploy.sh`
- 可以在env.sh自定义密码、网络端口等参数