package com.ctrip.framework.drc.console.vo.request;

import java.io.Serializable;
import java.util.List;

public class MqConfigDeleteRequestDto implements Serializable {
    private String mhaName;
    private List<Long> dbReplicationIdList;

    public String getMhaName() {
        return mhaName;
    }

    public void setMhaName(String mhaName) {
        this.mhaName = mhaName;
    }

    public List<Long> getDbReplicationIdList() {
        return dbReplicationIdList;
    }

    public void setDbReplicationIdList(List<Long> dbReplicationIdList) {
        this.dbReplicationIdList = dbReplicationIdList;
    }

    @Override
    public String toString() {
        return "MqConfigDeleteDto{" +
                "mhaName='" + mhaName + '\'' +
                ", dbReplicationIdList=" + dbReplicationIdList +
                '}';
    }
}
