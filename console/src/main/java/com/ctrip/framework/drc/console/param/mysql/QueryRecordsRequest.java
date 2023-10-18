package com.ctrip.framework.drc.console.param.mysql;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/10/17 17:04
 */
public class QueryRecordsRequest {
    private String mha;
    private String sql;
    private List<String> onUpdateColumns;

    public QueryRecordsRequest() {
    }

    public QueryRecordsRequest(String mha, String sql, List<String> onUpdateColumns) {
        this.mha = mha;
        this.sql = sql;
        this.onUpdateColumns = onUpdateColumns;
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
}
