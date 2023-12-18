package com.ctrip.framework.drc.console.param.mysql;

/**
 * Created by dengquanliang
 * 2023/12/5 14:47
 */
public class DdlHistoryEntity {
    private String mha;
    private String ddl;
    private int queryType;
    private String schemaName;
    private String tableName;

    public DdlHistoryEntity(String mha, String ddl, int queryType, String schemaName, String tableName) {
        this.mha = mha;
        this.ddl = ddl;
        this.queryType = queryType;
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public DdlHistoryEntity() {
    }

    public String getMha() {
        return mha;
    }

    public void setMha(String mha) {
        this.mha = mha;
    }

    public String getDdl() {
        return ddl;
    }

    public void setDdl(String ddl) {
        this.ddl = ddl;
    }

    public int getQueryType() {
        return queryType;
    }

    public void setQueryType(int queryType) {
        this.queryType = queryType;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String toString() {
        return "DdlHistoryEntity{" +
                "mha='" + mha + '\'' +
                ", ddl='" + ddl + '\'' +
                ", queryType=" + queryType +
                ", schemaName='" + schemaName + '\'' +
                ", tableName='" + tableName + '\'' +
                '}';
    }
}
