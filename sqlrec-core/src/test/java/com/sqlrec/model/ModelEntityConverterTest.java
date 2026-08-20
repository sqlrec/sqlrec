package com.sqlrec.model;

import com.sqlrec.compiler.CompileManager;
import com.sqlrec.common.model.ModelConf;
import com.sqlrec.sql.parser.SqlCreateModel;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Model/service names are normalized (lower case, back-quotes stripped) when
 * extracted from DDL, so the stored name never keeps quoting or casing.
 */
class ModelEntityConverterTest {

    @Test
    void convertToModelNormalizesModelName() throws Exception {
        SqlNode node = CompileManager.parseFlinkSql("create model MyModel (uid int)");

        ModelConf modelConf = ModelEntityConverter.convertToModel((SqlCreateModel) node);

        assertEquals("mymodel", modelConf.getModelName());
    }

    @Test
    void convertToModelStripsBackQuotesAndNormalizes() throws Exception {
        SqlNode node = CompileManager.parseFlinkSql("create model `MyModel` (uid int)");

        ModelConf modelConf = ModelEntityConverter.convertToModel((SqlCreateModel) node);

        assertEquals("mymodel", modelConf.getModelName());
    }
}
