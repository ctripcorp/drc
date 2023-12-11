package com.ctrip.framework.drc.console.param.mysql;

import java.util.List;
import java.util.Objects;

/**
 * Created by dengquanliang
 * 2023/10/17 17:04
 */
public class QueryRecordsRequest {
    private String mha;
    private String sql;
    //`column_name`
    private List<String> onUpdateColumns;
    //`column_name`
    private List<String> uniqueIndexColumns;
    private int columnSize;

    public QueryRecordsRequest() {
    }

    public QueryRecordsRequest(String mha, String sql, List<String> onUpdateColumns, List<String> uniqueIndexColumns, int columnSize) {
        this.mha = mha;
        this.sql = sql;
        this.onUpdateColumns = onUpdateColumns;
        this.uniqueIndexColumns = uniqueIndexColumns;
        this.columnSize = columnSize;
    }

    public QueryRecordsRequest(String mha, String sql, List<String> onUpdateColumns, int columnSize) {
        this.mha = mha;
        this.sql = sql;
        this.onUpdateColumns = onUpdateColumns;
        this.columnSize = columnSize;
    }

    public List<String> getUniqueIndexColumns() {
        return uniqueIndexColumns;
    }

    public void setUniqueIndexColumns(List<String> uniqueIndexColumns) {
        this.uniqueIndexColumns = uniqueIndexColumns;
    }

    public int getColumnSize() {
        return columnSize;
    }

    public void setColumnSize(int columnSize) {
        this.columnSize = columnSize;
    }

    public List<String> getOnUpdateColumns() {
        return onUpdateColumns;
    }

    public void setOnUpdateColumns(List<String> onUpdateColumns) {
        this.onUpdateColumns = onUpdateColumns;
    }

    public String getMha() {
        return mha;
    }

    public void setMha(String mha) {
        this.mha = mha;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryRecordsRequest that = (QueryRecordsRequest) o;
        return Objects.equals(mha, that.mha) && Objects.equals(sql, that.sql);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mha, sql, onUpdateColumns, columnSize);
    }
}
