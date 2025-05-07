package com.ctrip.framework.drc.console.vo.request;

import com.ctrip.framework.drc.core.mq.MqType;
import org.apache.commons.lang.StringUtils;
import org.springframework.util.CollectionUtils;

import java.io.Serializable;
import java.util.List;

public class MqConfigDeleteRequestDto implements Serializable {
    private String mhaName;
    private String mqType;
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

    public String getMqType() {
        return mqType;
    }

    public void setMqType(String mqType) {
        this.mqType = mqType;
    }

    public void validate() {
        if (CollectionUtils.isEmpty(this.getDbReplicationIdList())
                || StringUtils.isBlank(this.getMhaName())) {
            throw new IllegalArgumentException("mhaName or dbReplicationIdList is empty");
        }
        if (this.getDbReplicationIdList().size() != 1) {
            throw new IllegalArgumentException("batch delete not supported");
        }
        if (MqType.parse(mqType) == null) {
            throw new IllegalArgumentException("mqType illegal: " + mqType);
        }
    }

    @Override
    public String toString() {
        return "MqConfigDeleteRequestDto{" +
                "mhaName='" + mhaName + '\'' +
                ", mqType='" + mqType + '\'' +
                ", dbReplicationIdList=" + dbReplicationIdList +
                '}';
    }
}
