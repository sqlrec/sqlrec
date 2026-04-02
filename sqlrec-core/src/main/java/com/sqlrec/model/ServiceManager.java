package com.sqlrec.model;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.model.ModelController;
import com.sqlrec.common.model.ServiceConfig;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.entity.Checkpoint;
import com.sqlrec.entity.Model;
import com.sqlrec.entity.Service;
import com.sqlrec.k8s.K8sManager;
import com.sqlrec.sql.parser.SqlCreateService;
import com.sqlrec.utils.DbUtils;
import com.sqlrec.utils.ObjCache;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class ServiceManager {
    private static final Logger log = LoggerFactory.getLogger(ServiceManager.class);

    private static final ConcurrentHashMap<String, ObjCache<ServiceConfig>> serviceConfigCacheMap = new ConcurrentHashMap<>();

    public static ServiceConfig getServiceConfig(String serviceName) {
        ObjCache<ServiceConfig> cache = serviceConfigCacheMap.computeIfAbsent(serviceName,
                name -> new ObjCache<>(
                        SqlRecConfigs.SCHEMA_CACHE_EXPIRE.getValue() * 1000L,
                        SqlRecConfigs.ASYNC_SCHEMA_UPDATE.getValue(),
                        oldConfig -> {
                            try {
                                Service service = DbUtils.getService(name);
                                if (service == null) {
                                    return null;
                                }
                                return ModelEntityConverter.convertToServiceConfig(service);
                            } catch (Exception e) {
                                log.error("Failed to get service config for service: {}", name, e);
                                return null;
                            }
                        }
                )
        );
        return cache.getObj();
    }

    public static boolean isServiceOperationCompleted(String serviceName) {
        Service service = DbUtils.getService(serviceName);
        if (service == null) {
            throw new IllegalArgumentException("Service not found: " + serviceName);
        }

        String k8sYaml = service.getYaml();
        if (k8sYaml == null || k8sYaml.isEmpty()) {
            return true;
        }

        return K8sManager.isDeploymentReady(k8sYaml);
    }

    public static String createService(SqlCreateService sqlCreateService) throws Exception {
        ServiceConfig serviceConfig = ModelEntityConverter.convertToServiceConf(sqlCreateService);

        Model modelEntity = DbUtils.getModel(serviceConfig.modelName);
        if (modelEntity == null) {
            throw new IllegalArgumentException("model not exists: " + serviceConfig.modelName);
        }

        Checkpoint checkpoint = DbUtils.getCheckpoint(serviceConfig.modelName, serviceConfig.checkpointName);
        if (checkpoint == null) {
            throw new IllegalArgumentException("checkpoint not exists: " + serviceConfig.checkpointName + " for model " + serviceConfig.modelName);
        }
        if (!Consts.CHECKPOINT_STATUS_SUCCEEDED.equals(checkpoint.getStatus())) {
            throw new IllegalArgumentException("checkpoint status is not succeeded: " + checkpoint.getStatus() + " for checkpoint " + serviceConfig.checkpointName + " for model " + serviceConfig.modelName);
        }
        if (!Consts.CHECKPOINT_TYPE_EXPORT.equals(checkpoint.getCheckpointType())) {
            throw new IllegalArgumentException("service only supports export checkpoint");
        }

        ModelController modelController = ModelControllerFactory.getModelController(serviceConfig.modelConfig);
        if (modelController == null) {
            throw new IllegalArgumentException("Model controller not found for model name: " + serviceConfig.modelConfig.modelName);
        }

        String serviceUrl = modelController.getServiceUrl(serviceConfig.modelConfig, serviceConfig);
        String k8sYaml = modelController.getServiceK8sYaml(serviceConfig.modelConfig, serviceConfig);
        if (!StringUtils.isEmpty(k8sYaml)) {
            k8sYaml = ModelManager.injectPodConfig(k8sYaml, serviceConfig.modelConfig, serviceConfig.params);
            K8sManager.applyYaml(k8sYaml);
        }

        Service service = new Service();
        service.setName(serviceConfig.serviceName);
        service.setModelName(serviceConfig.modelName);
        service.setModelDdl(checkpoint.getModelDdl());
        service.setCheckpointName(serviceConfig.checkpointName);
        service.setDdl(CompileManager.getSqlStr(sqlCreateService));
        service.setYaml(k8sYaml);
        service.setUrl(serviceUrl);
        service.setCreatedAt(System.currentTimeMillis());
        service.setUpdatedAt(System.currentTimeMillis());
        service.setIfNotExists(sqlCreateService.isIfNotExists());

        if (sqlCreateService.isIfNotExists()) {
            DbUtils.insertService(service);
        } else {
            DbUtils.upsertService(service);
        }

        return serviceConfig.serviceName;
    }

    public static void deleteService(String serviceName) {
        Service service = DbUtils.getService(serviceName);
        if (service == null) {
            throw new IllegalArgumentException("service not exists: " + serviceName);
        }
        if (!StringUtils.isEmpty(service.getYaml())) {
            K8sManager.deleteYaml(service.getYaml());
        }
        DbUtils.deleteService(serviceName);
    }
}
