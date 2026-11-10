package com.sqlrec.runtime;

import com.sqlrec.common.utils.DataTypeUtils;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.junit.jupiter.api.Test;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SqlFunctionBindableStructureTest {
    @Test
    void constructorDetachesCallerOwnedListsAndInputEntries() throws Exception {
        List<RelDataTypeField> fields = new ArrayList<>(DataTypeUtils.getStringTypeField("value"));
        Map.Entry<String, List<RelDataTypeField>> input = new AbstractMap.SimpleEntry<>("input", fields);
        List<Map.Entry<String, List<RelDataTypeField>>> inputs = new ArrayList<>(List.of(input));
        List<BindableInterface> nodes = new ArrayList<>(List.of(new ReturnBindable((BindableInterface) null)));
        SqlFunctionBindable function = new SqlFunctionBindable(inputs, nodes, fields);

        inputs.clear();
        nodes.clear();
        input.setValue(List.of());
        fields.clear();
        function.init();

        assertEquals(1, function.getBindableList().size());
        assertEquals(1, function.getInputTables().size());
        assertEquals(1, function.getInputTables().getFirst().getValue().size());
        assertEquals(1, function.getReturnDataFields().size());
    }

    @Test
    void initializedStructureAndDerivedCollectionsCannotBeChanged() throws Exception {
        Set<String> reads = new HashSet<>(Set.of("external_table"));
        BindableInterface source = mock(BindableInterface.class);
        when(source.getReadTables()).thenReturn(reads);
        when(source.getWriteTables()).thenReturn(Set.of("output_table"));
        when(source.isParallelizable()).thenReturn(true);
        when(source.getDependencySqlFuncName()).thenReturn(Set.of("child"));
        when(source.getAllDependSqlFunctionMap()).thenReturn(Map.of("leaf", "leaf"));
        SqlFunctionBindable function = new SqlFunctionBindable(List.of(), List.of(source), null);
        function.addInputTable("input", DataTypeUtils.getStringTypeField("value"));
        function.addBindable(new ReturnBindable((BindableInterface) null));
        function.setReturnDataFields(DataTypeUtils.getStringTypeField("value"));
        function.setFunName("parent");
        function.init();
        reads.clear();

        assertEquals(Set.of("external_table"), function.getReadTables());
        assertEquals(Set.of(0), function.getBindableDependency().get(1));
        assertThrows(UnsupportedOperationException.class, () -> function.getBindableList().clear());
        assertThrows(UnsupportedOperationException.class, () -> function.getInputTables().clear());
        assertThrows(UnsupportedOperationException.class, () -> function.getInputTables().getFirst().setValue(List.of()));
        assertThrows(UnsupportedOperationException.class, () -> function.getInputTables().getFirst().getValue().clear());
        assertThrows(UnsupportedOperationException.class, () -> function.getReturnDataFields().clear());
        assertThrows(UnsupportedOperationException.class, () -> function.getReadTables().clear());
        assertThrows(UnsupportedOperationException.class, () -> function.getWriteTables().clear());
        assertThrows(UnsupportedOperationException.class, () -> function.getBindableDependency().put(2, Set.of()));
        assertThrows(UnsupportedOperationException.class, () -> function.getBindableDependency().get(1).clear());
        assertThrows(UnsupportedOperationException.class, () -> function.getAllDependSqlFunctionMap().clear());
        assertThrows(IllegalStateException.class, () -> function.addBindable(source));
        assertThrows(IllegalStateException.class, () -> function.addInputTable("extra", List.of()));
        assertThrows(IllegalStateException.class, () -> function.setReturnDataFields(List.of()));
        assertThrows(IllegalStateException.class, () -> function.setFunName("other"));
        assertThrows(IllegalStateException.class, function::init);
    }
}
