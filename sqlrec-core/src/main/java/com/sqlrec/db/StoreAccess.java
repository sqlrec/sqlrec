package com.sqlrec.db;

import com.sqlrec.entity.Checkpoint;
import com.sqlrec.entity.Model;
import com.sqlrec.entity.Service;
import com.sqlrec.entity.SqlApi;
import com.sqlrec.entity.SqlFunction;

import java.util.List;

public interface StoreAccess {

    List<SqlFunction> getSqlFunctionList();

    SqlFunction getSqlFunction(String name);

    void insertSqlFunction(SqlFunction sqlFunction);

    void upsertSqlFunction(SqlFunction sqlFunction);

    void deleteSqlFunction(String name);

    List<SqlApi> getSqlApiList();

    SqlApi getSqlApi(String name);

    void insertSqlApi(SqlApi sqlApi);

    void upsertSqlApi(SqlApi sqlApi);

    void deleteSqlApi(String name);

    List<SqlApi> getSqlApiListByFunctionName(String functionName);

    List<Model> getModelList();

    Model getModel(String name);

    void insertModel(Model model);

    void upsertModel(Model model);

    void deleteModel(String name);

    List<Checkpoint> getCheckpointListByModelName(String modelName);

    int getCheckpointCountByModelName(String modelName);

    List<Checkpoint> getCheckpointListByModelNamePaged(String modelName, int page, int pageSize);

    Checkpoint getCheckpoint(String modelName, String checkpointName);

    void upsertCheckpoint(Checkpoint checkpoint);

    void insertCheckpoint(Checkpoint checkpoint);

    void deleteCheckpoint(String modelName, String checkpointName);

    void deleteCheckpointByModelName(String modelName);

    List<Service> getServiceList();

    Service getService(String name);

    List<Service> getServiceListByModelName(String modelName);

    List<Service> getServiceListByCheckpoint(String modelName, String checkpointName);

    void insertService(Service service);

    void upsertService(Service service);

    void deleteService(String name);
}
