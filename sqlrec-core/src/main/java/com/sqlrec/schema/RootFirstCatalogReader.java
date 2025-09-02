package com.sqlrec.schema;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.validate.SqlNameMatchers;

import java.util.List;
import java.util.Objects;

public class RootFirstCatalogReader extends CalciteCatalogReader {
    public RootFirstCatalogReader(CalciteSchema rootSchema,
                                List<String> defaultSchema, RelDataTypeFactory typeFactory, CalciteConnectionConfig config) {
        super(rootSchema, SqlNameMatchers.withCaseSensitive(config != null && config.caseSensitive()),
                ImmutableList.of(
                        ImmutableList.of(),
                        Objects.requireNonNull(defaultSchema, "defaultSchema")
                ),
                typeFactory, config);
    }
}
