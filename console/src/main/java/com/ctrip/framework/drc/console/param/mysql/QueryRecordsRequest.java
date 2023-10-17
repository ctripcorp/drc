package com.ctrip.framework.drc.console.param.mysql;

/**
 * Created by dengquanliang
 * 2023/10/17 17:04
 */
public class QueryRecordsRequest {
    private String mha;
    private String rawSql;

    public QueryRecordsRequest() {
    }

    public QueryRecordsRequest(String mha, String rawSql) {
        this.mha = mha;
        this.rawSql = rawSql;
    }

    public String getMha() {
        return mha;
    }

    public void setMha(String mha) {
        this.mha = mha;
    }

    public String getRawSql() {
        return rawSql;
    }

    public void setRawSql(String rawSql) {
        this.rawSql = rawSql;
    }
}
