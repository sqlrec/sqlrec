package com.sqlrec.db.remote;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.db.StoreAccess;
import com.sqlrec.entity.Checkpoint;
import com.sqlrec.entity.Model;
import com.sqlrec.entity.Service;
import com.sqlrec.entity.SqlApi;
import com.sqlrec.entity.SqlFunction;
import org.apache.ibatis.datasource.pooled.PooledDataSource;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

public class DbStoreAccess implements StoreAccess {

    private volatile SqlSessionFactory sqlSessionFactory;

    public SqlSessionFactory getSqlSessionFactory() {
        if (sqlSessionFactory == null) {
            synchronized (this) {
                if (sqlSessionFactory == null) {
                    sqlSessionFactory = createSqlSessionFactory();
                }
            }
        }
        return sqlSessionFactory;
    }

    private SqlSessionFactory createSqlSessionFactory() {
        PooledDataSource dataSource = new PooledDataSource();
        dataSource.setDriver(SqlRecConfigs.DB_DRIVER.getValue());
        dataSource.setUrl(SqlRecConfigs.DB_URL.getValue());
        dataSource.setUsername(SqlRecConfigs.DB_USER.getValue());
        dataSource.setPassword(SqlRecConfigs.DB_PASSWORD.getValue());
        dataSource.setPoolMaximumActiveConnections(20);
        dataSource.setPoolMaximumIdleConnections(10);
        dataSource.setPoolMaximumCheckoutTime(20000);
        dataSource.setPoolTimeToWait(20000);
        dataSource.setPoolPingEnabled(true);
        dataSource.setPoolPingQuery("SELECT 1");
        dataSource.setPoolPingConnectionsNotUsedFor(3600000);

        TransactionFactory transactionFactory = new JdbcTransactionFactory();
        Environment environment = new Environment(
                "sqlrec",
                transactionFactory,
                dataSource
        );

        Configuration configuration = new Configuration(environment);
        configuration.setMapUnderscoreToCamelCase(true);
        configuration.addMapper(DbMapper.class);

        return new SqlSessionFactoryBuilder().build(configuration);
    }

    private <T> T execute(Function<DbMapper, T> operation) {
        try (SqlSession sqlSession = getSqlSessionFactory().openSession()) {
            return operation.apply(sqlSession.getMapper(DbMapper.class));
        }
    }

    private void executeVoid(Consumer<DbMapper> operation) {
        try (SqlSession sqlSession = getSqlSessionFactory().openSession()) {
            operation.accept(sqlSession.getMapper(DbMapper.class));
            sqlSession.commit();
        }
    }

    @Override
    public List<SqlFunction> getSqlFunctionList() {
        return execute(DbMapper::getSqlFunctionList);
    }

    @Override
    public SqlFunction getSqlFunction(String name) {
        return execute(dbMapper -> dbMapper.getSqlFunction(name.toUpperCase()));
    }

    @Override
    public void insertSqlFunction(SqlFunction sqlFunction) {
        sqlFunction.setName(sqlFunction.getName().toUpperCase());
        executeVoid(dbMapper -> dbMapper.insertSqlFunction(sqlFunction));
    }

    @Override
    public void upsertSqlFunction(SqlFunction sqlFunction) {
        sqlFunction.setName(sqlFunction.getName().toUpperCase());
        executeVoid(dbMapper -> dbMapper.upsertSqlFunction(sqlFunction));
    }

    @Override
    public void deleteSqlFunction(String name) {
        executeVoid(dbMapper -> dbMapper.deleteSqlFunction(name.toUpperCase()));
    }

    @Override
    public List<SqlApi> getSqlApiList() {
        return execute(DbMapper::getSqlApiList);
    }

    @Override
    public SqlApi getSqlApi(String name) {
        return execute(dbMapper -> dbMapper.getSqlApi(name));
    }

    @Override
    public void insertSqlApi(SqlApi sqlApi) {
        executeVoid(dbMapper -> dbMapper.insertSqlApi(sqlApi));
    }

    @Override
    public void upsertSqlApi(SqlApi sqlApi) {
        executeVoid(dbMapper -> dbMapper.upsertSqlApi(sqlApi));
    }

    @Override
    public void deleteSqlApi(String name) {
        executeVoid(dbMapper -> dbMapper.deleteSqlApi(name));
    }

    @Override
    public List<SqlApi> getSqlApiListByFunctionName(String functionName) {
        return execute(dbMapper -> dbMapper.getSqlApiListByFunctionName(functionName.toUpperCase()));
    }

    @Override
    public List<Model> getModelList() {
        return execute(DbMapper::getModelList);
    }

    @Override
    public Model getModel(String name) {
        return execute(dbMapper -> dbMapper.getModel(name));
    }

    @Override
    public void insertModel(Model model) {
        executeVoid(dbMapper -> dbMapper.insertModel(model));
    }

    @Override
    public void upsertModel(Model model) {
        executeVoid(dbMapper -> dbMapper.upsertModel(model));
    }

    @Override
    public void deleteModel(String name) {
        executeVoid(dbMapper -> dbMapper.deleteModel(name));
    }

    @Override
    public List<Checkpoint> getCheckpointListByModelName(String modelName) {
        return execute(dbMapper -> dbMapper.getCheckpointListByModelName(modelName));
    }

    @Override
    public int getCheckpointCountByModelName(String modelName) {
        return execute(dbMapper -> dbMapper.getCheckpointCountByModelName(modelName));
    }

    @Override
    public List<Checkpoint> getCheckpointListByModelNamePaged(String modelName, int page, int pageSize) {
        int offset = (page - 1) * pageSize;
        return execute(dbMapper -> dbMapper.getCheckpointListByModelNamePaged(modelName, pageSize, offset));
    }

    @Override
    public Checkpoint getCheckpoint(String modelName, String checkpointName) {
        return execute(dbMapper -> dbMapper.getCheckpoint(modelName, checkpointName));
    }

    @Override
    public void upsertCheckpoint(Checkpoint checkpoint) {
        executeVoid(dbMapper -> dbMapper.upsertCheckpoint(checkpoint));
    }

    @Override
    public void insertCheckpoint(Checkpoint checkpoint) {
        executeVoid(dbMapper -> dbMapper.insertCheckpoint(checkpoint));
    }

    @Override
    public void deleteCheckpoint(String modelName, String checkpointName) {
        executeVoid(dbMapper -> dbMapper.deleteCheckpoint(modelName, checkpointName));
    }

    @Override
    public void deleteCheckpointByModelName(String modelName) {
        executeVoid(dbMapper -> dbMapper.deleteCheckpointByModelName(modelName));
    }

    @Override
    public List<Service> getServiceList() {
        return execute(DbMapper::getServiceList);
    }

    @Override
    public Service getService(String name) {
        return execute(dbMapper -> dbMapper.getService(name));
    }

    @Override
    public List<Service> getServiceListByModelName(String modelName) {
        return execute(dbMapper -> dbMapper.getServiceListByModelName(modelName));
    }

    @Override
    public List<Service> getServiceListByCheckpoint(String modelName, String checkpointName) {
        return execute(dbMapper -> dbMapper.getServiceListByCheckpoint(modelName, checkpointName));
    }

    @Override
    public void insertService(Service service) {
        executeVoid(dbMapper -> dbMapper.insertService(service));
    }

    @Override
    public void upsertService(Service service) {
        executeVoid(dbMapper -> dbMapper.upsertService(service));
    }

    @Override
    public void deleteService(String name) {
        executeVoid(dbMapper -> dbMapper.deleteService(name));
    }
}
