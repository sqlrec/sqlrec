package com.sqlrec.schema;

import org.apache.calcite.schema.Table;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;

@Tag("integration")
public class HmsSchemaUpdateTest {
    @Test
    void testTableUpdate() {
        HmsSchema hmsSchema = new HmsSchema("default");
        Map<String, Table> tableMap1 = hmsSchema.computeTableMap(null);
        Map<String, Table> tableMap2 = hmsSchema.computeTableMap(tableMap1);

        assert tableMap1.size() == tableMap2.size();
        for (String table : tableMap1.keySet()) {
            assert tableMap2.containsKey(table);
            assert tableMap1.get(table).equals(tableMap2.get(table));
        }
    }
}
