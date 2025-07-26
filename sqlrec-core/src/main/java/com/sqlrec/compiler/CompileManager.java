package com.sqlrec.compiler;

import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.runtime.CacheTableBindable;
import com.sqlrec.runtime.CallFunctionBindable;
import com.sqlrec.runtime.FunctionBindable;
import com.sqlrec.sql.parser.*;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class CompileManager {
    private static Map<String, FunctionBindable> functionBindableMap = new ConcurrentHashMap<>();

    public static SqlNode parseFlinkSql(String sql) throws Exception {
        SqlParser.Config parserConfig = SqlParser.config()
                .withConformance(SqlConformanceEnum.DEFAULT)
                .withParserFactory(FlinkSqlParserImpl.FACTORY);
        SqlParser parser = SqlParser.create(sql, parserConfig);
        return parser.parseQuery();
    }

    public static boolean isFlinkSqlCompilable(SqlNode flinkSqlNode) {
        if (flinkSqlNode instanceof SqlCreateSqlFunction) {
            return false;
        }
        if (flinkSqlNode instanceof SqlReturn) {
            return false;
        }
        if (flinkSqlNode instanceof SqlDefineInputTable) {
            return false;
        }
        if (flinkSqlNode instanceof SqlCreateApi) {
            return false;
        }

        if (flinkSqlNode instanceof SqlCallSqlFunction) {
            return true;
        }
        if (flinkSqlNode instanceof SqlCache) {
            return true;
        }

        // todo add sql type check, only crud not on hive is compilable
        return true;
    }

    public static BindableInterface compileSql(SqlNode flinkSqlNode, CalciteSchema schema) throws Exception {
        if (!isFlinkSqlCompilable(flinkSqlNode)) {
            throw new Exception("sql is not compilable");
        }

        if (flinkSqlNode instanceof SqlCallSqlFunction) {
            return getCallSqlFunctionBindable((SqlCallSqlFunction) flinkSqlNode, schema);
        }
        if (flinkSqlNode instanceof SqlCache) {
            return getCacheBindable((SqlCache) flinkSqlNode, schema);
        }

        return getNormalSqlBindable(getSqlStr(flinkSqlNode), schema);
    }

    private static BindableInterface getCallSqlFunctionBindable(SqlCallSqlFunction callSqlFunction, CalciteSchema schema) throws Exception {
        String functionName = callSqlFunction.getFuncName().getSimple();
        List<String> inputTableList = callSqlFunction.getInputTableList()
                .stream()
                .map(SqlIdentifier::getSimple)
                .collect(Collectors.toList());
        FunctionBindable sqlFunctionBindable = compileSqlFunction(functionName);
        return new CallFunctionBindable(functionName, inputTableList, sqlFunctionBindable);
    }

    private static BindableInterface getCacheBindable(SqlCache cache, CalciteSchema schema) throws Exception {
        String tableName = cache.getTableName().getSimple();

        SqlCallSqlFunction callSqlFunction = cache.getCallSqlFunction();
        if (callSqlFunction != null) {
            BindableInterface bindableInterface = getCallSqlFunctionBindable(callSqlFunction, schema);
            return new CacheTableBindable(tableName, bindableInterface);
        }

        SqlSelect select = cache.getSelect();
        if (select != null) {
            BindableInterface bindableInterface = getNormalSqlBindable(getSqlStr(select), schema);
            return new CacheTableBindable(tableName, bindableInterface);
        }

        throw new Exception("cache sql obj is invalid");
    }

    private static BindableInterface getNormalSqlBindable(String sqlStr, CalciteSchema schema) throws Exception {
        return NormalSqlCompiler.getNormalSqlBindable(sqlStr, schema);
    }

    private static String getSqlStr(SqlNode sqlNode) {
        return sqlNode.toSqlString(AnsiSqlDialect.DEFAULT).getSql();
    }

    public static FunctionBindable compileSqlFunction(String functionName) {
        if (functionBindableMap.containsKey(functionName)) {
            return functionBindableMap.get(functionName);
        }
        // todo get function define from db
        throw new RuntimeException("function not found: " + functionName);
    }

    public static FunctionBindable compileSqlFunction(String functionName, List<String> sqlList) throws Exception {
        FunctionCompiler functionCompiler = new FunctionCompiler();
        functionCompiler.compileAllSql(sqlList);
        if (functionCompiler.isFunctionCompileFinish()) {
            functionBindableMap.put(functionName, functionCompiler.getFunctionBindable());
            return functionCompiler.getFunctionBindable();
        }
        throw new RuntimeException("function compile failed");
    }

    public static SqlValidator createSqlValidate(CalciteSchema schema) {
        return NormalSqlCompiler.createSqlValidate(schema);
    }
}
