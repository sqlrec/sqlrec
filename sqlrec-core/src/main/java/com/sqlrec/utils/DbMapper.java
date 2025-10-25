package com.sqlrec.utils;

import com.sqlrec.entity.SqlApi;
import com.sqlrec.entity.SqlFunction;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
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
}
