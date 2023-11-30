package com.ctrip.framework.drc.console.param.mysql;

/**
 * Created by dengquanliang
 * 2023/11/14 14:49
 */
public class MysqlWriteEntity {
    private String mha;
    private String sql;

    public MysqlWriteEntity() {
    }

    public MysqlWriteEntity(String mha, String sql) {
        this.mha = mha;
        this.sql = sql;
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
    public String toString() {
        return "MysqlWriteEntity{" +
                "mha='" + mha + '\'' +
                ", sql='" + sql + '\'' +
                '}';
    }
}
