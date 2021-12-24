package com.ctrip.framework.drc.fetcher.resource.context.sql;

import java.util.List;

/**
 * @Author Slight
 * Oct 24, 2019
 */
public class DeleteBuilder implements SQLUtil {

    public final String tableName;
    public final List<String> columnNames;
    public final List<Boolean> eqBitmap;

    public DeleteBuilder(String tableName, List<String> columnNames, List<Boolean> eqBitmap) {
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.eqBitmap = eqBitmap;
    }

    public String prepare() {
        StringBuilder stringBuilder = new StringBuilder(256);
        return stringBuilder.append("DELETE FROM ").append(tableName).append(" WHERE ").append(prepareEquations(selectColumnNames(columnNames, eqBitmap), " AND ")).toString();
    }

    public String prepareWithComment(String comment) {
        StringBuilder stringBuilder = new StringBuilder(256);
        return stringBuilder.append("/*").append(comment).append("*/ ").append(prepare()).toString();
    }
}
