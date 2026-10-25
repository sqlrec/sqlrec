package com.sqlrec.model.common;

import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.model.gbdt.CatBoostModel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class FieldTypeUtilsTest {
    @Test
    @ResourceLock("java.util.Locale.default")
    void typeRecognitionAndModelValidationAreIndependentOfDefaultLocale() {
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(Locale.forLanguageTag("tr-TR"));
            for (String type : List.of("INT", "INTEGER", "BIGINT")) {
                assertTrue(FieldTypeUtils.isInteger(type), type);
            }
            assertTrue(FieldTypeUtils.isFloat("FLOAT"));
            assertTrue(FieldTypeUtils.isFloat("DOUBLE"));
            assertTrue(FieldTypeUtils.isString("STRING"));
            assertTrue(FieldTypeUtils.isArray("ARRAY<STRING>"));
            assertFalse(FieldTypeUtils.isInteger(null));
            assertFalse(FieldTypeUtils.isFloat(null));
            assertFalse(FieldTypeUtils.isArray(null));

            ModelConf model = new ModelConf();
            model.setInputFields(List.of(new FieldSchema("id", "BIGINT"),
                    new FieldSchema("category", "STRING"), new FieldSchema("label", "INT")));
            model.setParams(Map.of("label_columns", "label"));
            assertNull(new CatBoostModel().checkModel(model));
        } finally {
            Locale.setDefault(previous);
        }
    }
}
