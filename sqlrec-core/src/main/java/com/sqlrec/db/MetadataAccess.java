package com.sqlrec.db;

import com.sqlrec.entity.Checkpoint;
import com.sqlrec.entity.Model;
import com.sqlrec.entity.Service;
import com.sqlrec.entity.SqlApi;
import com.sqlrec.entity.SqlFunction;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.List;

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
        return storeAccess.getSqlFunction(name);
    }

    public void insertSqlFunction(SqlFunction sqlFunction) {
        storeAccess.insertSqlFunction(sqlFunction);
    }

    public void upsertSqlFunction(SqlFunction sqlFunction) {
        storeAccess.upsertSqlFunction(sqlFunction);
    }

    public void deleteSqlFunction(String name) {
        storeAccess.deleteSqlFunction(name);
    }

    public List<SqlApi> getSqlApiList() {
        return storeAccess.getSqlApiList();
    }

    public SqlApi getSqlApi(String name) {
        return storeAccess.getSqlApi(name);
    }

    public void insertSqlApi(SqlApi sqlApi) {
        storeAccess.insertSqlApi(sqlApi);
    }

    public void upsertSqlApi(SqlApi sqlApi) {
        storeAccess.upsertSqlApi(sqlApi);
    }

    public void deleteSqlApi(String name) {
        storeAccess.deleteSqlApi(name);
    }

    public List<SqlApi> getSqlApiListByFunctionName(String functionName) {
        return storeAccess.getSqlApiListByFunctionName(functionName);
    }

    public List<Model> getModelList() {
        return storeAccess.getModelList();
    }

    public Model getModel(String name) {
        return storeAccess.getModel(name);
    }

    public void insertModel(Model model) {
        storeAccess.insertModel(model);
    }

    public void upsertModel(Model model) {
        storeAccess.upsertModel(model);
    }

    public void deleteModel(String name) {
        storeAccess.deleteModel(name);
    }

    public List<Checkpoint> getCheckpointListByModelName(String modelName) {
        return storeAccess.getCheckpointListByModelName(modelName);
    }

    public int getCheckpointCountByModelName(String modelName) {
        return storeAccess.getCheckpointCountByModelName(modelName);
    }

    public List<Checkpoint> getCheckpointListByModelNamePaged(String modelName, int page, int pageSize) {
        return storeAccess.getCheckpointListByModelNamePaged(modelName, page, pageSize);
    }

    public Checkpoint getCheckpoint(String modelName, String checkpointName) {
        return storeAccess.getCheckpoint(modelName, checkpointName);
    }

    public void upsertCheckpoint(Checkpoint checkpoint) {
        storeAccess.upsertCheckpoint(checkpoint);
    }

    public void insertCheckpoint(Checkpoint checkpoint) {
        storeAccess.insertCheckpoint(checkpoint);
    }

    public void deleteCheckpoint(String modelName, String checkpointName) {
        storeAccess.deleteCheckpoint(modelName, checkpointName);
    }

    public void deleteCheckpointByModelName(String modelName) {
        storeAccess.deleteCheckpointByModelName(modelName);
    }

    public List<Service> getServiceList() {
        return storeAccess.getServiceList();
    }

    public Service getService(String name) {
        return storeAccess.getService(name);
    }

    public List<Service> getServiceListByModelName(String modelName) {
        return storeAccess.getServiceListByModelName(modelName);
    }

    public List<Service> getServiceListByCheckpoint(String modelName, String checkpointName) {
        return storeAccess.getServiceListByCheckpoint(modelName, checkpointName);
    }

    public void insertService(Service service) {
        storeAccess.insertService(service);
    }

    public void upsertService(Service service) {
        storeAccess.upsertService(service);
    }

    public void deleteService(String name) {
        storeAccess.deleteService(name);
    }

    public boolean hdfsPathExists(String hdfsPath) {
        return hdfsAccess.pathExists(hdfsPath);
    }

    public void hdfsDeletePath(String hdfsPath) {
        hdfsAccess.deletePath(hdfsPath);
    }
}
