package com.sqlrec.model.common;

import java.util.ArrayList;
import java.util.List;

/**
 * Field-type judgment and CSV-list parsing helpers shared by the GBDT and TZRec
 * pipeline-config generators.
 *
 * <p>Both {@code com.sqlrec.model.gbdt.PipelineConfigUtils} and
 * {@code com.sqlrec.model.tzrec.PipelineConfigUtils} previously re-implemented the same
 * {@code "float"/"double"} check, {@code "array<...>"} check and comma-separated list parsing.
 * Centralising them here removes the drift (e.g. one used {@code equals}, the other
 * {@code equalsIgnoreCase}) and gives a single place to evolve type rules.
 */
public final class FieldTypeUtils {

    private FieldTypeUtils() {
    }

    /** Returns true for {@code float} / {@code double} (case-insensitive). */
    public static boolean isFloat(String fieldType) {
        if (fieldType == null) {
            return false;
        }
        String lower = fieldType.toLowerCase();
        return lower.equals("float") || lower.equals("double");
    }

    /** Returns true for scalar {@code int} / {@code bigint} (case-insensitive). */
    public static boolean isInteger(String fieldType) {
        if (fieldType == null) {
            return false;
        }
        String lower = fieldType.toLowerCase();
        return lower.equals("int") || lower.equals("bigint");
    }

    /** Returns true for {@code array<...>} types (case-insensitive prefix match). */
    public static boolean isArray(String fieldType) {
        return fieldType != null && fieldType.toLowerCase().startsWith("array<");
    }

    /** Returns true for {@code string} (case-insensitive). */
    public static boolean isString(String fieldType) {
        return fieldType != null && fieldType.equalsIgnoreCase("string");
    }

    /**
     * Parse a comma-separated string into a list of trimmed, non-empty tokens.
     * Returns an empty list for {@code null} / empty input (never throws).
     */
    public static List<String> parseCsvList(String csv) {
        List<String> result = new ArrayList<>();
        if (csv == null || csv.isEmpty()) {
            return result;
        }
        for (String token : csv.split(",")) {
            String trimmed = token.trim();
            if (!trimmed.isEmpty()) {
                result.add(trimmed);
            }
        }
        return result;
    }
}
