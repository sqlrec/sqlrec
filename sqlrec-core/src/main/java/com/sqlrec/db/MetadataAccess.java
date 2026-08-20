package com.sqlrec.db;

import com.sqlrec.common.utils.ResourceNames;
import com.sqlrec.entity.Checkpoint;
import com.sqlrec.entity.Model;
import com.sqlrec.entity.Service;
import com.sqlrec.entity.SqlApi;
import com.sqlrec.entity.SqlFunction;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.List;

/**
 * Facade of metadata storage. Resource names (sql function / api / model / service /
 * checkpoint) are case-insensitive; this facade is the last line of defense that
 * normalizes names before delegating to the underlying {@link StoreAccess}, so
 * implementations do not need to care about name casing.
 */
public class MetadataAccess {

    private final SchemaAccess schemaAccess;
    private final StoreAccess storeAccess;
    private final HdfsAccess hdfsAccess;

    public MetadataAccess(SchemaAccess schemaAccess, StoreAccess storeAccess, HdfsAccess hdfsAccess) {
        this.schemaAccess = schemaAccess;
        this.storeAccess = storeAccess;
        this.hdfsAccess = hdfsAccess;
    }

    public List<String> getDatabases() throws Exception {
        return schemaAccess.getDatabases();
    }

    public List<Table> getTables(String database) throws Exception {
        return schemaAccess.getTables(database);
    }

    public Table getTable(String database, String tableName) throws Exception {
        return schemaAccess.getTable(database, tableName);
    }

    public List<Function> getFunctions(String database) throws Exception {
        return schemaAccess.getFunctions(database);
    }

    public Function getFunction(String database, String funName) throws Exception {
        return schemaAccess.getFunction(database, funName);
    }

    public long getTableUpdateTime(String database, String table) {
        return schemaAccess.getTableUpdateTime(database, table);
    }

    public List<String> getPartitionPaths(String database, String table, String partitionFilter) throws Exception {
        return schemaAccess.getPartitionPaths(database, table, partitionFilter);
    }

    public List<SqlFunction> getSqlFunctionList() {
        return storeAccess.getSqlFunctionList();
    }

    public SqlFunction getSqlFunction(String name) {
        return storeAccess.getSqlFunction(ResourceNames.normalize(name));
    }

    public void insertSqlFunction(SqlFunction sqlFunction) {
        sqlFunction.setName(ResourceNames.normalize(sqlFunction.getName()));
        storeAccess.insertSqlFunction(sqlFunction);
    }

    public void upsertSqlFunction(SqlFunction sqlFunction) {
        sqlFunction.setName(ResourceNames.normalize(sqlFunction.getName()));
        storeAccess.upsertSqlFunction(sqlFunction);
    }

    public void deleteSqlFunction(String name) {
        storeAccess.deleteSqlFunction(ResourceNames.normalize(name));
    }

    public List<SqlApi> getSqlApiList() {
        return storeAccess.getSqlApiList();
    }

    public SqlApi getSqlApi(String name) {
        return storeAccess.getSqlApi(ResourceNames.normalize(name));
    }

    public void insertSqlApi(SqlApi sqlApi) {
        sqlApi.setName(ResourceNames.normalize(sqlApi.getName()));
        sqlApi.setFunctionName(ResourceNames.normalize(sqlApi.getFunctionName()));
        storeAccess.insertSqlApi(sqlApi);
    }

    public void upsertSqlApi(SqlApi sqlApi) {
        sqlApi.setName(ResourceNames.normalize(sqlApi.getName()));
        sqlApi.setFunctionName(ResourceNames.normalize(sqlApi.getFunctionName()));
        storeAccess.upsertSqlApi(sqlApi);
    }

    public void deleteSqlApi(String name) {
        storeAccess.deleteSqlApi(ResourceNames.normalize(name));
    }

    public List<SqlApi> getSqlApiListByFunctionName(String functionName) {
        return storeAccess.getSqlApiListByFunctionName(ResourceNames.normalize(functionName));
    }

    public List<Model> getModelList() {
        return storeAccess.getModelList();
    }

    public Model getModel(String name) {
        return storeAccess.getModel(ResourceNames.normalize(name));
    }

    public void insertModel(Model model) {
        model.setName(ResourceNames.normalize(model.getName()));
        storeAccess.insertModel(model);
    }

    public void upsertModel(Model model) {
        model.setName(ResourceNames.normalize(model.getName()));
        storeAccess.upsertModel(model);
    }

    public void deleteModel(String name) {
        storeAccess.deleteModel(ResourceNames.normalize(name));
    }

    public List<Checkpoint> getCheckpointListByModelName(String modelName) {
        return storeAccess.getCheckpointListByModelName(ResourceNames.normalize(modelName));
    }

    public int getCheckpointCountByModelName(String modelName) {
        return storeAccess.getCheckpointCountByModelName(ResourceNames.normalize(modelName));
    }

    public List<Checkpoint> getCheckpointListByModelNamePaged(String modelName, int page, int pageSize) {
        return storeAccess.getCheckpointListByModelNamePaged(ResourceNames.normalize(modelName), page, pageSize);
    }

    public Checkpoint getCheckpoint(String modelName, String checkpointName) {
        return storeAccess.getCheckpoint(ResourceNames.normalize(modelName), ResourceNames.normalize(checkpointName));
    }

    public void upsertCheckpoint(Checkpoint checkpoint) {
        checkpoint.setModelName(ResourceNames.normalize(checkpoint.getModelName()));
        checkpoint.setCheckpointName(ResourceNames.normalize(checkpoint.getCheckpointName()));
        storeAccess.upsertCheckpoint(checkpoint);
    }

    public void insertCheckpoint(Checkpoint checkpoint) {
        checkpoint.setModelName(ResourceNames.normalize(checkpoint.getModelName()));
        checkpoint.setCheckpointName(ResourceNames.normalize(checkpoint.getCheckpointName()));
        storeAccess.insertCheckpoint(checkpoint);
    }

    public void deleteCheckpoint(String modelName, String checkpointName) {
        storeAccess.deleteCheckpoint(ResourceNames.normalize(modelName), ResourceNames.normalize(checkpointName));
    }

    public void deleteCheckpointByModelName(String modelName) {
        storeAccess.deleteCheckpointByModelName(ResourceNames.normalize(modelName));
    }

    public List<Service> getServiceList() {
        return storeAccess.getServiceList();
    }

    public Service getService(String name) {
        return storeAccess.getService(ResourceNames.normalize(name));
    }

    public List<Service> getServiceListByModelName(String modelName) {
        return storeAccess.getServiceListByModelName(ResourceNames.normalize(modelName));
    }

    public List<Service> getServiceListByCheckpoint(String modelName, String checkpointName) {
        return storeAccess.getServiceListByCheckpoint(
                ResourceNames.normalize(modelName), ResourceNames.normalize(checkpointName));
    }

    public void insertService(Service service) {
        normalizeService(service);
        storeAccess.insertService(service);
    }

    public void upsertService(Service service) {
        normalizeService(service);
        storeAccess.upsertService(service);
    }

    public void deleteService(String name) {
        storeAccess.deleteService(ResourceNames.normalize(name));
    }

    private static void normalizeService(Service service) {
        service.setName(ResourceNames.normalize(service.getName()));
        service.setModelName(ResourceNames.normalize(service.getModelName()));
        service.setCheckpointName(ResourceNames.normalize(service.getCheckpointName()));
    }

    public boolean hdfsPathExists(String hdfsPath) {
        return hdfsAccess.pathExists(hdfsPath);
    }

    public void hdfsDeletePath(String hdfsPath) {
        hdfsAccess.deletePath(hdfsPath);
    }
}
