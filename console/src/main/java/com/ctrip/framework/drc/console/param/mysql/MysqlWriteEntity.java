package com.ctrip.framework.drc.console.param.mysql;

/**
 * Created by dengquanliang
 * 2023/11/14 14:49
 */
public class MysqlWriteEntity {
    private String mha;
    private String sql;
    //0-drc_console 1-drc_write 2-drc_read
    private int accountType;

    public MysqlWriteEntity() {
    }

    public MysqlWriteEntity(String mha, String sql, int accountType) {
        this.mha = mha;
        this.sql = sql;
        this.accountType = accountType;
    }

    public int getAccountType() {
        return accountType;
    }

    public void setAccountType(int accountType) {
        this.accountType = accountType;
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
                ", accountType=" + accountType +
                '}';
    }
}
