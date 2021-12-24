package com.ctrip.framework.drc.core.server.config.validation.dto;

import java.util.List;

public class ValidationResultDto {
    private String mhaName;
    private String gtid;
    private String schemaName;
    private String tableName;
    private String sql;
    private String expectedDc;
    private String actualDc;
    private List<String> columns;
    private List<String> beforeValues;
    private List<String> afterValues;
    private String uidName;
    private int ucsStrategyId;
    private long executeTime;

    public ValidationResultDto() {
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

    public String getMhaName() {
        return mhaName;
    }

    public void setMhaName(String mhaName) {
        this.mhaName = mhaName;
    }

    public String getGtid() {
        return gtid;
    }

    public void setGtid(String gtid) {
        this.gtid = gtid;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getExpectedDc() {
        return expectedDc;
    }

    public void setExpectedDc(String expectedDc) {
        this.expectedDc = expectedDc;
    }

    public String getActualDc() {
        return actualDc;
    }

    public void setActualDc(String actualDc) {
        this.actualDc = actualDc;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public List<String> getBeforeValues() {
        return beforeValues;
    }

    public void setBeforeValues(List<String> beforeValues) {
        this.beforeValues = beforeValues;
    }

    public List<String> getAfterValues() {
        return afterValues;
    }

    public void setAfterValues(List<String> afterValues) {
        this.afterValues = afterValues;
    }

    public String getUidName() {
        return uidName;
    }

    public void setUidName(String uidName) {
        this.uidName = uidName;
    }

    public int getUcsStrategyId() {
        return ucsStrategyId;
    }

    public void setUcsStrategyId(int ucsStrategyId) {
        this.ucsStrategyId = ucsStrategyId;
    }

    public long getExecuteTime() {
        return executeTime;
    }

    public void setExecuteTime(long executeTime) {
        this.executeTime = executeTime;
    }

    @Override
    public String toString() {
        return "ValidationResultDto{" +
                "mhaName='" + mhaName + '\'' +
                ", gtid='" + gtid + '\'' +
                ", schemaName='" + schemaName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", sql='" + sql + '\'' +
                ", expectedDc='" + expectedDc + '\'' +
                ", actualDc='" + actualDc + '\'' +
                ", columns=" + columns +
                ", beforeValues=" + beforeValues +
                ", afterValues=" + afterValues +
                ", uidName='" + uidName + '\'' +
                ", ucsStrategyId=" + ucsStrategyId +
                ", executeTime=" + executeTime +
                '}';
    }
}
