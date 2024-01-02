package com.ctrip.framework.drc.console.dto.v2;

public class MhaDbDelayInfoDto extends MhaDelayInfoDto {
    private String dbName;

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    @Override
    public String toString() {
        if (dstMha == null) {
            return String.format("%s (%s): %dms", dbName, srcMha, super.getDelay());
        }
        return String.format("%s (%s->%s): %dms", dbName, srcMha, dstMha, super.getDelay());
    }
}
