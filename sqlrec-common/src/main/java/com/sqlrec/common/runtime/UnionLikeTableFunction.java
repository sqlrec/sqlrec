package com.sqlrec.common.runtime;

/**
 * Marks a table function whose output degrades safely when one of its input
 * branches is replaced by an empty table.
 */
public interface UnionLikeTableFunction {
}
