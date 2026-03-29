package com.sqlrec.model;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.model.ServiceConfig;
import com.sqlrec.entity.Service;
import com.sqlrec.k8s.K8sManager;
import com.sqlrec.utils.DbUtils;
import com.sqlrec.utils.ObjCache;
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
}
