package com.sqlrec.utils;

import com.sqlrec.entity.SqlApi;
import com.sqlrec.entity.SqlFunction;
import com.sqlrec.entity.Model;
import com.sqlrec.entity.Checkpoint;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface DbMapper {
    @Select("SELECT * FROM sql_function")
    List<SqlFunction> getSqlFunctionList();

    @Select("SELECT * FROM sql_function WHERE name = #{name}")
    SqlFunction getSqlFunction(String name);

    @Insert("INSERT INTO sql_function " +
            "(name, sql_list, created_at, updated_at) " +
            "VALUES (#{name}, #{sqlList}, #{createdAt}, #{updatedAt})")
    void insertSqlFunction(SqlFunction sqlFunction);

    @Insert("INSERT INTO sql_function " +
            "(name, sql_list, created_at, updated_at) " +
            "VALUES (#{name}, #{sqlList}, #{createdAt}, #{updatedAt}) " +
            "ON CONFLICT (name) DO UPDATE SET sql_list = #{sqlList}, updated_at = #{updatedAt}")
    void upsertSqlFunction(SqlFunction sqlFunction);

    @Delete("DELETE FROM sql_function WHERE name = #{name}")
    void deleteSqlFunction(String name);

    @Select("SELECT * FROM sql_api")
    List<SqlApi> getSqlApiList();

    @Select("SELECT * FROM sql_api WHERE name = #{name}")
    SqlApi getSqlApi(String name);

    @Insert("INSERT INTO sql_api " +
            "(name, function_name, created_at, updated_at) " +
            "VALUES (#{name}, #{functionName}, #{createdAt}, #{updatedAt})")
    void insertSqlApi(SqlApi sqlApi);

    @Insert("INSERT INTO sql_api " +
            "(name, function_name, created_at, updated_at) " +
            "VALUES (#{name}, #{functionName}, #{createdAt}, #{updatedAt}) " +
            "ON CONFLICT (name) DO UPDATE SET function_name = #{functionName}, updated_at = #{updatedAt}")
    void upsertSqlApi(SqlApi sqlApi);

    @Delete("DELETE FROM sql_api WHERE name = #{name}")
    void deleteSqlApi(String name);

    @Select("SELECT * FROM model")
    List<Model> getModelList();

    @Select("SELECT * FROM model WHERE name = #{name}")
    Model getModel(String name);

    @Insert("INSERT INTO model " +
            "(name, ddl, created_at, updated_at) " +
            "VALUES (#{name}, #{ddl}, #{createdAt}, #{updatedAt})")
    void insertModel(Model model);

    @Insert("INSERT INTO model " +
            "(name, ddl, created_at, updated_at) " +
            "VALUES (#{name}, #{ddl}, #{createdAt}, #{updatedAt}) " +
            "ON CONFLICT (name) DO UPDATE SET ddl = #{ddl}, updated_at = #{updatedAt}")
    void upsertModel(Model model);

    @Delete("DELETE FROM model WHERE name = #{name}")
    void deleteModel(String name);

    @Select("SELECT * FROM checkpoint WHERE model_name = #{modelName}")
    List<Checkpoint> getCheckpointListByModelName(String modelName);

    @Select("SELECT * FROM checkpoint WHERE model_name = #{modelName} AND checkpoint_name = #{checkpointName}")
    Checkpoint getCheckpoint(@Param("modelName") String modelName, @Param("checkpointName") String checkpointName);

    @Insert("INSERT INTO checkpoint " +
            "(model_name, checkpoint_name, ddl, yaml, checkpoint_type, status, created_at, updated_at) " +
            "VALUES (#{modelName}, #{checkpointName}, #{ddl}, #{yaml}, #{checkpointType}, #{status}, #{createdAt}, #{updatedAt}) " +
            "ON CONFLICT (model_name, checkpoint_name) DO UPDATE SET ddl = #{ddl}, yaml = #{yaml}, checkpoint_type = #{checkpointType}, status = #{status}, updated_at = #{updatedAt}")
    void upsertCheckpoint(Checkpoint checkpoint);

    @Delete("DELETE FROM checkpoint WHERE model_name = #{modelName} AND checkpoint_name = #{checkpointName}")
    void deleteCheckpoint(@Param("modelName") String modelName, @Param("checkpointName") String checkpointName);

    @Delete("DELETE FROM checkpoint WHERE model_name = #{modelName}")
    void deleteCheckpointByModelName(String modelName);
}
