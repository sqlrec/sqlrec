package com.sqlrec.connectors.filesystem.handler;

import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.connectors.filesystem.config.FileSystemConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class FileSystemDataLoaderTest {
    @Test
    void csvParsesEachColumnTypeOnceAndKeepsInvalidCellsNull(@TempDir Path directory) throws Exception {
        Path file = directory.resolve("values.csv");
        Files.writeString(file, "id,amount,ignored\n1,12.340,value\nbad,not-decimal,value\n2,,value\n");
        FieldSchema id = spy(new FieldSchema("id", "INTEGER"));
        FieldSchema amount = spy(new FieldSchema("amount", "DECIMAL(8,3)"));
        FieldSchema ignored = spy(new FieldSchema("ignored", "INVALID_TYPE"));
        FileSystemConfig config = new FileSystemConfig();
        config.path = file.toString();
        config.format = "csv";
        config.fieldSchemas = List.of(id, amount, ignored);

        List<Object[]> rows = new FileSystemDataLoader(config).load();
        assertEquals(3, rows.size());
        assertArrayEquals(new Object[]{1, new BigDecimal("12.340"), null}, rows.get(0));
        assertArrayEquals(new Object[]{null, null, null}, rows.get(1));
        assertArrayEquals(new Object[]{2, null, null}, rows.get(2));
        for (FieldSchema field : config.fieldSchemas) verify(field, times(1)).getType();
    }
}
