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
    private List<String> onUpdateColumns;
    private int columnSize;

    public QueryRecordsRequest() {
    }

    public QueryRecordsRequest(String mha, String sql, List<String> onUpdateColumns) {
        this.mha = mha;
        this.sql = sql;
        this.onUpdateColumns = onUpdateColumns;
    }

    public QueryRecordsRequest(String mha, String sql, List<String> onUpdateColumns, int columnSize) {
        this.mha = mha;
        this.sql = sql;
        this.onUpdateColumns = onUpdateColumns;
        this.columnSize = columnSize;
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
