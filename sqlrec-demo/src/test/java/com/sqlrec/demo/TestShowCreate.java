package com.sqlrec.demo;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.utils.HiveTableUtils;
import com.sqlrec.common.utils.JsonUtils;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.entity.Model;
import com.sqlrec.entity.Service;
import com.sqlrec.entity.SqlApi;
import com.sqlrec.entity.SqlFunction;
import org.apache.calcite.sql.SqlNode;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class TestShowCreate {

    private static String sqlDir;

    @BeforeAll
    static void setUp() throws URISyntaxException {
        String moduleDir = Paths.get(TestShowCreate.class.getProtectionDomain()
                .getCodeSource().getLocation().toURI()).getParent().getParent().toString();
        sqlDir = Paths.get(moduleDir, "src", "main", "sql").toString();
        SqlRecConfigs.SQL_SCHEMA_DIR.setDefaultValue(sqlDir);
    }

    @Test
    void testShowCreateTable() throws Exception {
        MetadataAccess db = MetadataAccessFactory.getInstance();
        List<Table> tables = db.getTables("default");
        assertFalse(tables.isEmpty(), "should have tables loaded from sql files");

        Table genreHotItem = tables.stream()
                .filter(t -> t.getTableName().equals("genre_hot_item"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("genre_hot_item not found"));

        String connector = HiveTableUtils.getTableConnector(genreHotItem);
        assertEquals("redis", connector, "connector should be redis");

        String url = HiveTableUtils.getFlinkTableOptions(genreHotItem).get("url");
        assertEquals("redis://192.168.1.5:32379/0", url);

        String sourceContent = Files.readString(Paths.get(sqlDir, "table", "genre_hot_item.sql")).trim();
        SqlNode sourceNode = CompileManager.parseFlinkSql(stripTrailingSemicolon(sourceContent));
        String normalizedSource = CompileManager.getSqlStr(sourceNode);
        String tableName = genreHotItem.getTableName();
        assertTrue(normalizedSource.toLowerCase().contains(tableName),
                "source file should contain table name " + tableName);
    }

    @Test
    void testShowCreateUdf() throws Exception {
        MetadataAccess db = MetadataAccessFactory.getInstance();
        List<Function> functions = db.getFunctions("default");
        assertFalse(functions.isEmpty(), "should have udf functions loaded from sql files");

        Function demoScalarUdf = functions.stream()
                .filter(f -> f.getFunctionName().equals("demo_scalar_udf"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("demo_scalar_udf not found"));

        assertEquals("com.sqlrec.demo.udf.DemoScalarUdf", demoScalarUdf.getClassName());

        String sourceContent = Files.readString(Paths.get(sqlDir, "udf", "DemoScalarUdf.sql")).trim();
        assertTrue(sourceContent.contains(demoScalarUdf.getClassName()),
                "source file should contain class name " + demoScalarUdf.getClassName());
    }

    @Test
    void testShowCreateModel() throws Exception {
        MetadataAccess db = MetadataAccessFactory.getInstance();
        Model rankModel = db.getModel("rank_model");
        assertNotNull(rankModel, "rank_model should exist");
        assertNotNull(rankModel.getDdl(), "model ddl should not be null");

        String sourceContent = Files.readString(Paths.get(sqlDir, "model", "rank_model.sql")).trim();
        SqlNode sourceNode = CompileManager.parseFlinkSql(stripTrailingSemicolon(sourceContent));
        String normalizedSource = CompileManager.getSqlStr(sourceNode);

        assertEquals(normalizedSource, rankModel.getDdl(),
                "model ddl should match source file");
    }

    @Test
    void testShowCreateService() throws Exception {
        MetadataAccess db = MetadataAccessFactory.getInstance();
        Service rankService = db.getService("rank_service");
        assertNotNull(rankService, "rank_service should exist");
        assertNotNull(rankService.getDdl(), "service ddl should not be null");

        String sourceContent = Files.readString(Paths.get(sqlDir, "service", "rank_service.sql")).trim();
        SqlNode sourceNode = CompileManager.parseFlinkSql(stripTrailingSemicolon(sourceContent));
        String normalizedSource = CompileManager.getSqlStr(sourceNode);

        assertEquals(normalizedSource, rankService.getDdl(),
                "service ddl should match source file");
    }

    @Test
    void testShowCreateSqlFunction() throws Exception {
        MetadataAccess db = MetadataAccessFactory.getInstance();
        SqlFunction recallFun = db.getSqlFunction("RECALL_FUN");
        assertNotNull(recallFun, "recall_fun should exist");

        String sourceContent = Files.readString(Paths.get(sqlDir, "function", "recall_fun.sql")).trim();
        List<String> sourceSqlList = splitStatements(sourceContent);
        List<String> storedSqlList = JsonUtils.parseStringList(recallFun.getSqlList());

        assertEquals(sourceSqlList.size(), storedSqlList.size(),
                "sql function statement count should match");
        for (int i = 0; i < sourceSqlList.size(); i++) {
            SqlNode sourceNode = CompileManager.parseFlinkSql(sourceSqlList.get(i));
            String normalizedSource = CompileManager.getSqlStr(sourceNode);
            assertEquals(normalizedSource, storedSqlList.get(i),
                    "sql function statement " + i + " should match");
        }
    }

    @Test
    void testShowCreateApi() throws Exception {
        MetadataAccess db = MetadataAccessFactory.getInstance();
        SqlApi mainRecApi = db.getSqlApi("main_rec");
        assertNotNull(mainRecApi, "main_rec api should exist");
        assertEquals("MAIN_REC", mainRecApi.getFunctionName(),
                "api function name should match");

        String sourceContent = Files.readString(Paths.get(sqlDir, "api", "main_rec.sql")).trim();
        assertTrue(sourceContent.toLowerCase().contains("main_rec"),
                "source file should contain api name");
    }

    private static List<String> splitStatements(String content) {
        return java.util.Arrays.stream(content.split(";"))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();
    }

    private static String stripTrailingSemicolon(String sql) {
        String trimmed = sql.trim();
        if (trimmed.endsWith(";")) {
            return trimmed.substring(0, trimmed.length() - 1).trim();
        }
        return trimmed;
    }
}
