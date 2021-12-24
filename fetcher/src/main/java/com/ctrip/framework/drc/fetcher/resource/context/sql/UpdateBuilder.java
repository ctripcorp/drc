package com.ctrip.framework.drc.fetcher.resource.context.sql;

import java.util.List;

/**
 * @Author Slight
 * Oct 17, 2019
 */
public class UpdateBuilder implements SQLUtil {

    public final String tableName;
    public final List<String> columnNames;
    public final List<Boolean> valueBitmap;
    public final List<Boolean> eqBitmap;
    public List<Boolean> lteBitmap = null;

    public UpdateBuilder(String tableName, List<String> columnNames,
                         List<Boolean> valueBitmap, List<Boolean> eqBitmap) {
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.valueBitmap = valueBitmap;
        this.eqBitmap = eqBitmap;
    }

    public UpdateBuilder(String tableName, List<String> columnNames,
                         List<Boolean> valueBitmap, List<Boolean> eqBitmap, List<Boolean> lteBitmap) {
        this(tableName, columnNames, valueBitmap, eqBitmap);
        this.lteBitmap = lteBitmap;
    }

    public String prepare() {
        StringBuilder stringBuilder = new StringBuilder(256);
        stringBuilder.append("UPDATE ").append(tableName).append(" SET ").append(prepareEquations(selectColumnNames(columnNames, valueBitmap), ","))
                .append(" WHERE ").append(prepareEquations(selectColumnNames(columnNames, eqBitmap), " AND "));
        if (lteBitmap != null) {
            stringBuilder.append(" AND ").append(prepareExpressions(selectColumnNames(columnNames, lteBitmap), " AND ", "<="));
        }
        return stringBuilder.toString();
    }

    public String prepareWithComment(String comment) {
        StringBuilder stringBuilder = new StringBuilder(256);
        return stringBuilder.append("/*").append(comment).append("*/ ").append(prepare()).toString();
    }
}
