package com.ctrip.framework.drc.console.dto.v3;

public class MhaDbDto {
    private Long mhaDbMappingId;
    private String mhaName;
    private String dbName;

    public MhaDbDto(Long mhaDbMappingId, String mhaName, String dbName) {
        this.mhaDbMappingId = mhaDbMappingId;
        this.mhaName = mhaName;
        this.dbName = dbName;
    }

    public String getMhaName() {
        return mhaName;
    }

    public void setMhaName(String mhaName) {
        this.mhaName = mhaName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public Long getMhaDbMappingId() {
        return mhaDbMappingId;
    }

    public void setMhaDbMappingId(Long mhaDbMappingId) {
        this.mhaDbMappingId = mhaDbMappingId;
    }

    @Override
    public String toString() {
        return "MhaDbDto{" +
                "mhaDbMappingId=" + mhaDbMappingId +
                ", mhaName='" + mhaName + '\'' +
                ", dbName='" + dbName + '\'' +
                '}';
    }
}
