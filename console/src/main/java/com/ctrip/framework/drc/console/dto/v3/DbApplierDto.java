package com.ctrip.framework.drc.console.dto.v3;

import java.util.List;

public class DbApplierDto {
    private List<String> ips;
    private String gtidInit;
    private Integer concurrency;
    private String dbName;

    public DbApplierDto() {
    }

    public DbApplierDto(List<String> ips, String gtidInit, String dbName) {
        this.ips = ips;
        this.gtidInit = gtidInit;
        this.dbName = dbName;
    }

    public DbApplierDto(List<String> ips, String gtidInit, String dbName, Integer concurrency) {
        this.ips = ips;
        this.gtidInit = gtidInit;
        this.dbName = dbName;
        this.concurrency = concurrency;
    }

    public List<String> getIps() {
        return ips;
    }

    public String getGtidInit() {
        return gtidInit;
    }

    public Integer getConcurrency() {
        return concurrency;
    }

    public void setIps(List<String> ips) {
        this.ips = ips;
    }

    public void setGtidInit(String gtidInit) {
        this.gtidInit = gtidInit;
    }

    public void setConcurrency(Integer concurrency) {
        this.concurrency = concurrency;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getDbName() {
        return dbName;
    }

    @Override
    public String toString() {
        return "DbApplierDto{" +
                "ips=" + ips +
                ", gtidInit='" + gtidInit + '\'' +
                ", concurrency=" + concurrency +
                ", dbName='" + dbName + '\'' +
                '}';
    }
}
