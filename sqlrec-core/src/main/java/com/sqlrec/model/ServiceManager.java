package com.sqlrec.model;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.model.ModelController;
import com.sqlrec.common.model.ServiceConfig;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.entity.Checkpoint;
import com.sqlrec.entity.Model;
import com.sqlrec.entity.Service;
import com.sqlrec.k8s.K8sManager;
import com.sqlrec.sql.parser.SqlCreateService;
import com.sqlrec.utils.ObjCache;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class ServiceManager {
    private static final Logger log = LoggerFactory.getLogger(ServiceManager.class);

    private static final ConcurrentHashMap<String, ObjCache<ServiceConfig>> serviceConfigCacheMap = new ConcurrentHashMap<>();

    public static void invalidateCache() {
        serviceConfigCacheMap.values().forEach(ObjCache::invalidate);
    }

    public static ServiceConfig getServiceConfig(String serviceName) {
        ObjCache<ServiceConfig> cache = serviceConfigCacheMap.computeIfAbsent(serviceName,
                name -> new ObjCache<>(
                        SqlRecConfigs.SCHEMA_CACHE_EXPIRE.getValue() * 1000L,
                        SqlRecConfigs.ASYNC_SCHEMA_UPDATE.getValue(),
                        oldConfig -> {
                            try {
                                MetadataAccess db = MetadataAccessFactory.getInstance();
                                Service service = db.getService(name);
                                if (service == null) {
                                    return null;
                                }
                                return ModelEntityConverter.convertToServiceConfig(service);
                            } catch (Exception e) {
                                throw new RuntimeException("Failed to get service config for service: " + name, e);
                            }
                        }
                )
        );
        return cache.getObj();
    }

    public static boolean isServiceOperationCompleted(String serviceName) {
        MetadataAccess db = MetadataAccessFactory.getInstance();
        Service service = db.getService(serviceName);
        if (service == null) {
            throw new IllegalArgumentException("Service not found: " + serviceName);
        }

        String k8sYaml = service.getYaml();
        return K8sManager.isDeploymentReadyFromYaml(k8sYaml);
    }

    public static String createService(SqlCreateService sqlCreateService) throws Exception {
        MetadataAccess db = MetadataAccessFactory.getInstance();
        ServiceConfig serviceConfig = ModelEntityConverter.convertToServiceConf(sqlCreateService);

        Service existingService = db.getService(serviceConfig.getServiceName());
        if (existingService != null) {
            if (sqlCreateService.isIfNotExists()) {
                return serviceConfig.getServiceName();
            }
        }

        Service service = saveServiceInDb(sqlCreateService, db, false, false);
        K8sManager.applyYaml(service.getYaml());

        return serviceConfig.getServiceName();
    }

    public static Service saveServiceInDb(
            SqlCreateService sqlCreateService,
            MetadataAccess db,
            boolean skipCheckpoint,
            boolean skipK8sYaml
    ) throws Exception {
        ServiceConfig serviceConfig = ModelEntityConverter.convertToServiceConf(sqlCreateService);

        Model modelEntity = db.getModel(serviceConfig.getModelName());
        if (modelEntity == null) {
            throw new IllegalArgumentException("model not exists: " + serviceConfig.getModelName());
        }
        serviceConfig.setModelConfig(ModelEntityConverter.convertToModel(modelEntity.getDdl()));

        String modelDdl = modelEntity.getDdl();
        if (StringUtils.isNotEmpty(serviceConfig.getCheckpointName()) && !skipCheckpoint) {
            Checkpoint checkpoint = db.getCheckpoint(serviceConfig.getModelName(), serviceConfig.getCheckpointName());
            if (checkpoint == null) {
                throw new IllegalArgumentException("checkpoint not exists: " + serviceConfig.getCheckpointName() + " for model " + serviceConfig.getModelName());
            }
            if (!Consts.CHECKPOINT_STATUS_SUCCEEDED.equals(checkpoint.getStatus())) {
                throw new IllegalArgumentException("checkpoint status is not succeeded: " + checkpoint.getStatus() + " for checkpoint " + serviceConfig.getCheckpointName() + " for model " + serviceConfig.getModelName());
            }
            if (!Consts.CHECKPOINT_TYPE_EXPORT.equals(checkpoint.getCheckpointType())) {
                throw new IllegalArgumentException("service only supports export checkpoint");
            }
            modelDdl = checkpoint.getModelDdl();
        }

        ModelController modelController = ModelControllerFactory.getModelController(serviceConfig.getModelConfig());
        if (modelController == null) {
            throw new IllegalArgumentException("Model controller not found for model name: " + serviceConfig.getModelConfig().getModelName());
        }

        String serviceUrl = modelController.getServiceUrl(serviceConfig.getModelConfig(), serviceConfig);
        String k8sYaml = "";
        if (!skipK8sYaml) {
            k8sYaml = modelController.getServiceK8sYaml(serviceConfig.getModelConfig(), serviceConfig);
            if (!StringUtils.isEmpty(k8sYaml)) {
                k8sYaml = ModelManager.injectPodConfig(k8sYaml, serviceConfig.getModelConfig(), serviceConfig.getParams());
            }
        }

        Service service = new Service();
        service.setName(serviceConfig.getServiceName());
        service.setModelName(serviceConfig.getModelName());
        service.setModelDdl(modelDdl);
        service.setCheckpointName(serviceConfig.getCheckpointName());
        service.setDdl(CompileManager.getSqlStr(sqlCreateService));
        service.setYaml(k8sYaml);
        service.setUrl(serviceUrl);
        service.setCreatedAt(System.currentTimeMillis());
        service.setUpdatedAt(System.currentTimeMillis());
        service.setIfNotExists(sqlCreateService.isIfNotExists());

        db.upsertService(service);
        return service;
    }

    public static void deleteService(String serviceName) {
        MetadataAccess db = MetadataAccessFactory.getInstance();
        Service service = db.getService(serviceName);
        if (service == null) {
            throw new IllegalArgumentException("service not exists: " + serviceName);
        }
        if (!StringUtils.isEmpty(service.getYaml())) {
            K8sManager.deleteYaml(service.getYaml());
        }
        db.deleteService(serviceName);
    }
}
