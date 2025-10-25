package com.sqlrec.utils;

import com.sqlrec.common.config.SqlRecConfigs;
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

public class DbUtils {
    private static volatile SqlSessionFactory sqlSessionFactory;

    public static SqlSessionFactory getSqlSessionFactory() {
        if (sqlSessionFactory == null) {
            synchronized (DbUtils.class) {
                if (sqlSessionFactory == null) {
                    sqlSessionFactory = createSqlSessionFactory();
                }
            }
        }
        return sqlSessionFactory;
    }

    private static SqlSessionFactory createSqlSessionFactory() {
        PooledDataSource dataSource = new PooledDataSource();
        dataSource.setDriver(SqlRecConfigs.DB_DRIVER.getValue());
        dataSource.setUrl(SqlRecConfigs.DB_URL.getValue());
        dataSource.setUsername(SqlRecConfigs.DB_USER.getValue());
        dataSource.setPassword(SqlRecConfigs.DB_PASSWORD.getValue());

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

    private static  <T> T execute(Function<DbMapper, T> operation) {
        try (SqlSession sqlSession = getSqlSessionFactory().openSession()) {
            return operation.apply(sqlSession.getMapper(DbMapper.class));
        }
    }

    private static void executeVoid(Consumer<DbMapper> operation) {
        try (SqlSession sqlSession = getSqlSessionFactory().openSession()) {
            operation.accept(sqlSession.getMapper(DbMapper.class));
            sqlSession.commit();
        }
    }

    public static List<SqlFunction> getSqlFunctionList() {
        return execute(DbMapper::getSqlFunctionList);
    }

    public static SqlFunction getSqlFunction(String name) {
        return execute(dbMapper -> dbMapper.getSqlFunction(name.toUpperCase()));
    }

    public static void insertSqlFunction(SqlFunction sqlFunction) {
        sqlFunction.setName(sqlFunction.getName().toUpperCase());
        executeVoid(dbMapper -> dbMapper.insertSqlFunction(sqlFunction));
    }

    public static void upsertSqlFunction(SqlFunction sqlFunction) {
        sqlFunction.setName(sqlFunction.getName().toUpperCase());
        executeVoid(dbMapper -> dbMapper.upsertSqlFunction(sqlFunction));
    }

    public static void deleteSqlFunction(String name) {
        executeVoid(dbMapper -> dbMapper.deleteSqlFunction(name.toUpperCase()));
    }

    public static List<SqlApi> getSqlApiList() {
        return execute(DbMapper::getSqlApiList);
    }

    public static SqlApi getSqlApi(String name) {
        return execute(dbMapper -> dbMapper.getSqlApi(name));
    }

    public static void insertSqlApi(SqlApi sqlApi) {
        executeVoid(dbMapper -> dbMapper.insertSqlApi(sqlApi));
    }

    public static void upsertSqlApi(SqlApi sqlApi) {
        executeVoid(dbMapper -> dbMapper.upsertSqlApi(sqlApi));
    }

    public static void deleteSqlApi(String name) {
        executeVoid(dbMapper -> dbMapper.deleteSqlApi(name));
    }
}
