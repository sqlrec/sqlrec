package com.sqlrec.connectors;

import com.sqlrec.compiler.CompileManager;
import com.sqlrec.compiler.NormalSqlCompiler;
import com.sqlrec.node.SqlRecJoin;
import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.runtime.CalciteBindable;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.schema.HmsSchema;
import org.apache.calcite.adapter.enumerable.EnumerableInterpreter;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.sqlrec.connectors.TestRedisTable.getListRedisTable;
import static com.sqlrec.connectors.TestRedisTable.getRedisTable;

@Tag("integration")
public class TestJoin {
    @Test
    public void testJoin() throws Exception {
        Map<String, Table> tableMap = new HashMap<>();
        tableMap.put("t1", getRedisTable());
        tableMap.put("t2", getListRedisTable());

        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(NormalSqlCompiler.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return tableMap;
            }
        });
        HmsSchema.setGlobalSchema(schema);

        List<String> sqlList = Arrays.asList(
                "cache table t3 as select id from t2 where id = 1",
                "select t3.id, t2.name from t3 join t2 on t3.id = t2.id limit 1"
        );

        for (String sql : sqlList) {
            System.out.println("\n" + sql);
            SqlNode flinkSqlNode = CompileManager.parseFlinkSql(sql);
            BindableInterface bindable = CompileManager.compileSql(flinkSqlNode, schema, NormalSqlCompiler.DEFAULT_SCHEMA_NAME);

            if (sql.contains("join")) {
                assert bindable instanceof CalciteBindable;
                CalciteBindable calciteBindable = (CalciteBindable) bindable;
                RelNode bestExp = calciteBindable.getBestExp();
                assert bestExp instanceof EnumerableInterpreter;
                RelNode project = ((EnumerableInterpreter) bestExp).getInput();
                assert project instanceof Bindables.BindableProject;
                RelNode sort = ((Bindables.BindableProject) project).getInput();
                assert sort instanceof Bindables.BindableSort;
                RelNode join = ((Bindables.BindableSort) sort).getInput();
                assert join instanceof SqlRecJoin;
                SqlRecJoin sqlRecJoin = (SqlRecJoin) join;
                assert sqlRecJoin.getLimit() == 1;
                assert sqlRecJoin.getProjectList().equals(Arrays.asList(0, 2));
            }

            Enumerable enumerable = bindable.bind(schema, new ExecuteContextImpl());
            if (enumerable != null) {
                List<Object[]> results = enumerable.toList();
                for (Object[] result : results) {
                    System.out.println(java.util.Arrays.toString(result));
                }
            } else {
                System.out.println("no result");
            }
        }
    }
}
