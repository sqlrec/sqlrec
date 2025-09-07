package com.sqlrec.utils;

import com.sqlrec.entity.SqlApi;
import com.sqlrec.entity.SqlFunction;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;

public interface DbMapper {
    @Select("SELECT * FROM sql_function")
    List<SqlFunction> getSqlFunctionList();

    @Select("SELECT * FROM sql_function WHERE name = #{name}")
    SqlFunction getSqlFunction(String name);

    @Insert("INSERT INTO sql_function (name, sql_list) VALUES (#{name}, #{sqlList})")
    void insertSqlFunction(SqlFunction sqlFunction);

    @Insert("INSERT INTO sql_function (name, sql_list) VALUES (#{name}, #{sqlList}) ON DUPLICATE KEY UPDATE sql_list = #{sqlList}")
    void upsertSqlFunction(SqlFunction sqlFunction);

    @Update("UPDATE sql_function SET sql_list = #{sqlList} WHERE name = #{name}")
    void updateSqlFunction(SqlFunction sqlFunction);

    @Delete("DELETE FROM sql_function WHERE name = #{name}")
    void deleteSqlFunction(String name);

    @Select("SELECT * FROM sql_api")
    List<SqlApi> getSqlApiList();

    @Select("SELECT * FROM sql_api WHERE name = #{name}")
    SqlApi getSqlApi(String name);

    @Insert("INSERT INTO sql_api (name, function_name) VALUES (#{name}, #{functionName})")
    void insertSqlApi(SqlApi sqlApi);

    @Insert("INSERT INTO sql_api (name, function_name) VALUES (#{name}, #{functionName}) ON DUPLICATE KEY UPDATE function_name = #{functionName}")
    void upsertSqlApi(SqlApi sqlApi);

    @Update("UPDATE sql_api SET function_name = #{functionName} WHERE name = #{name}")
    void updateSqlApi(SqlApi sqlApi);

    @Delete("DELETE FROM sql_api WHERE name = #{name}")
    void deleteSqlApi(String name);
}
