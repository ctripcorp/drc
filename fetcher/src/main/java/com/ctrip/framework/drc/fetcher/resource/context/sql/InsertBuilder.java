package com.ctrip.framework.drc.fetcher.resource.context.sql;

import java.util.List;

/**
 * @Author Slight
 * Oct 02, 2019
 */
public class InsertBuilder implements SQLUtil {

    public String tableName;

    public List<String> columnNames;
    public List<Boolean> columnBitmap;

    public InsertBuilder(String tableName, List<String> columnNames,
                         List<Boolean> columnBitmap) {
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.columnBitmap = columnBitmap;
    }

    public String prepareSingle() {
        StringBuilder stringBuilder = new StringBuilder(256);
        return stringBuilder.append("INSERT INTO ").append(tableName).append(" ").append(groupColumnNames(columnNames, columnBitmap))
                .append(" VALUES ").append(groupQuestionMarks(selectColumnNames(columnNames, columnBitmap).size())).toString();
    }

    public String prepareSingleWithComment(String comment) {
        StringBuilder stringBuilder = new StringBuilder(256);
        return stringBuilder.append("/*").append(comment).append("*/ ").append(prepareSingle()).toString();
    }
}
