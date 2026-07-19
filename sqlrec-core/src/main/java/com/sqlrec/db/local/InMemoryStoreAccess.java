package com.sqlrec.db.local;

import com.sqlrec.common.utils.JsonUtils;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.db.StoreAccess;
import com.sqlrec.entity.*;
import com.sqlrec.sql.parser.SqlCreateApi;
import com.sqlrec.sql.parser.SqlCreateModel;
import com.sqlrec.sql.parser.SqlCreateSqlFunction;
import org.apache.calcite.sql.SqlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class InMemoryStoreAccess implements StoreAccess {
    private static final Logger log = LoggerFactory.getLogger(InMemoryStoreAccess.class);

    private final Map<String, SqlFunction> sqlFunctionMap = new ConcurrentHashMap<>();
    private final Map<String, SqlApi> sqlApiMap = new ConcurrentHashMap<>();
    private final Map<String, Model> modelMap = new ConcurrentHashMap<>();
    private final Map<String, Checkpoint> checkpointMap = new ConcurrentHashMap<>();
    private final Map<String, Service> serviceMap = new ConcurrentHashMap<>();

    public InMemoryStoreAccess(List<List<SqlNode>> sqlFunctionNodeGroups,
                               List<SqlNode> apiNodes,
                               List<SqlNode> modelNodes) {
        initSqlFunctions(sqlFunctionNodeGroups);
        initApis(apiNodes);
        initModels(modelNodes);
    }

    private void initSqlFunctions(List<List<SqlNode>> sqlFunctionNodeGroups) {
        for (List<SqlNode> nodeGroup : sqlFunctionNodeGroups) {
            if (nodeGroup.isEmpty()) {
                throw new RuntimeException("SQL function node group is empty");
            }
            SqlNode firstNode = nodeGroup.get(0);
            if (!(firstNode instanceof SqlCreateSqlFunction)) {
                throw new RuntimeException("Expected SqlCreateSqlFunction but got " + firstNode.getClass().getSimpleName());
            }
            SqlCreateSqlFunction createFunc = (SqlCreateSqlFunction) firstNode;
            String funcName = createFunc.getFuncName().getSimple().toUpperCase();
            List<String> sqlList = new ArrayList<>();
            for (SqlNode node : nodeGroup) {
                sqlList.add(CompileManager.getSqlStr(node));
            }
            SqlFunction sqlFunction = new SqlFunction();
            sqlFunction.setName(funcName);
            sqlFunction.setSqlList(JsonUtils.toJson(sqlList));
            sqlFunction.setCreatedAt(System.currentTimeMillis());
            sqlFunction.setUpdatedAt(System.currentTimeMillis());
            sqlFunctionMap.put(funcName, sqlFunction);
        }
    }

    private void initApis(List<SqlNode> apiNodes) {
        for (SqlNode node : apiNodes) {
            if (!(node instanceof SqlCreateApi)) {
                throw new RuntimeException("Expected SqlCreateApi but got " + node.getClass().getSimpleName());
            }
            SqlCreateApi createApi = (SqlCreateApi) node;
            SqlApi sqlApi = new SqlApi();
            sqlApi.setName(createApi.getApiName());
            sqlApi.setFunctionName(createApi.getFuncName().toUpperCase());
            sqlApi.setCreatedAt(System.currentTimeMillis());
            sqlApi.setUpdatedAt(System.currentTimeMillis());
            sqlApiMap.put(sqlApi.getName(), sqlApi);
        }
    }

    private void initModels(List<SqlNode> modelNodes) {
        for (SqlNode node : modelNodes) {
            if (!(node instanceof SqlCreateModel)) {
                throw new RuntimeException("Expected SqlCreateModel but got " + node.getClass().getSimpleName());
            }
            SqlCreateModel createModel = (SqlCreateModel) node;
            Model model = new Model();
            model.setName(createModel.getModelName().getSimple());
            model.setDdl(CompileManager.getSqlStr(node));
            model.setCreatedAt(System.currentTimeMillis());
            model.setUpdatedAt(System.currentTimeMillis());
            modelMap.put(model.getName(), model);
        }
    }

    private static String checkpointKey(String modelName, String checkpointName) {
        return modelName + ":" + checkpointName;
    }

    @Override
    public List<SqlFunction> getSqlFunctionList() {
        return new ArrayList<>(sqlFunctionMap.values());
    }

    @Override
    public SqlFunction getSqlFunction(String name) {
        return sqlFunctionMap.get(name.toUpperCase());
    }

    @Override
    public void insertSqlFunction(SqlFunction sqlFunction) {
        sqlFunction.setName(sqlFunction.getName().toUpperCase());
        if (sqlFunctionMap.putIfAbsent(sqlFunction.getName(), sqlFunction) != null) {
            throw new RuntimeException("SqlFunction already exists: " + sqlFunction.getName());
        }
    }

    @Override
    public void upsertSqlFunction(SqlFunction sqlFunction) {
        sqlFunction.setName(sqlFunction.getName().toUpperCase());
        sqlFunctionMap.put(sqlFunction.getName(), sqlFunction);
    }

    @Override
    public void deleteSqlFunction(String name) {
        sqlFunctionMap.remove(name.toUpperCase());
    }

    @Override
    public List<SqlApi> getSqlApiList() {
        return new ArrayList<>(sqlApiMap.values());
    }

    @Override
    public SqlApi getSqlApi(String name) {
        return sqlApiMap.get(name);
    }

    @Override
    public void insertSqlApi(SqlApi sqlApi) {
        if (sqlApiMap.putIfAbsent(sqlApi.getName(), sqlApi) != null) {
            throw new RuntimeException("SqlApi already exists: " + sqlApi.getName());
        }
    }

    @Override
    public void upsertSqlApi(SqlApi sqlApi) {
        sqlApiMap.put(sqlApi.getName(), sqlApi);
    }

    @Override
    public void deleteSqlApi(String name) {
        sqlApiMap.remove(name);
    }

    @Override
    public List<SqlApi> getSqlApiListByFunctionName(String functionName) {
        return sqlApiMap.values().stream()
                .filter(api -> api.getFunctionName().equalsIgnoreCase(functionName))
                .collect(Collectors.toList());
    }

    @Override
    public List<Model> getModelList() {
        return new ArrayList<>(modelMap.values());
    }

    @Override
    public Model getModel(String name) {
        return modelMap.get(name);
    }

    @Override
    public void insertModel(Model model) {
        if (modelMap.putIfAbsent(model.getName(), model) != null) {
            throw new RuntimeException("Model already exists: " + model.getName());
        }
    }

    @Override
    public void upsertModel(Model model) {
        modelMap.put(model.getName(), model);
    }

    @Override
    public void deleteModel(String name) {
        modelMap.remove(name);
    }

    @Override
    public List<Checkpoint> getCheckpointListByModelName(String modelName) {
        return checkpointMap.values().stream()
                .filter(cp -> cp.getModelName() != null && cp.getModelName().equals(modelName))
                .collect(Collectors.toList());
    }

    @Override
    public int getCheckpointCountByModelName(String modelName) {
        return (int) checkpointMap.values().stream()
                .filter(cp -> cp.getModelName().equals(modelName))
                .count();
    }

    @Override
    public List<Checkpoint> getCheckpointListByModelNamePaged(String modelName, int page, int pageSize) {
        return checkpointMap.values().stream()
                .filter(cp -> cp.getModelName().equals(modelName))
                .sorted(Comparator.comparingLong(Checkpoint::getCreatedAt).reversed())
                .skip((long) (page - 1) * pageSize)
                .limit(pageSize)
                .collect(Collectors.toList());
    }

    @Override
    public Checkpoint getCheckpoint(String modelName, String checkpointName) {
        return checkpointMap.get(checkpointKey(modelName, checkpointName));
    }

    @Override
    public void upsertCheckpoint(Checkpoint checkpoint) {
        checkpointMap.put(checkpointKey(checkpoint.getModelName(), checkpoint.getCheckpointName()), checkpoint);
    }

    @Override
    public void insertCheckpoint(Checkpoint checkpoint) {
        String key = checkpointKey(checkpoint.getModelName(), checkpoint.getCheckpointName());
        if (checkpointMap.putIfAbsent(key, checkpoint) != null) {
            throw new RuntimeException("Checkpoint already exists: " + key);
        }
    }

    @Override
    public void deleteCheckpoint(String modelName, String checkpointName) {
        checkpointMap.remove(checkpointKey(modelName, checkpointName));
    }

    @Override
    public void deleteCheckpointByModelName(String modelName) {
        checkpointMap.entrySet().removeIf(entry -> entry.getValue().getModelName().equals(modelName));
    }

    @Override
    public List<Service> getServiceList() {
        return new ArrayList<>(serviceMap.values());
    }

    @Override
    public Service getService(String name) {
        return serviceMap.get(name);
    }

    @Override
    public List<Service> getServiceListByModelName(String modelName) {
        return serviceMap.values().stream()
                .filter(svc -> svc.getModelName() != null && svc.getModelName().equals(modelName))
                .collect(Collectors.toList());
    }

    @Override
    public List<Service> getServiceListByCheckpoint(String modelName, String checkpointName) {
        return serviceMap.values().stream()
                .filter(svc -> svc.getModelName() != null && svc.getModelName().equals(modelName)
                        && svc.getCheckpointName() != null && svc.getCheckpointName().equals(checkpointName))
                .collect(Collectors.toList());
    }

    @Override
    public void insertService(Service service) {
        if (serviceMap.putIfAbsent(service.getName(), service) != null) {
            throw new RuntimeException("Service already exists: " + service.getName());
        }
    }

    @Override
    public void upsertService(Service service) {
        serviceMap.put(service.getName(), service);
    }

    @Override
    public void deleteService(String name) {
        serviceMap.remove(name);
    }
}
