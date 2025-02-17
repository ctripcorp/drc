package com.ctrip.framework.drc.console.dto.v3;

import java.util.List;

public class MqLogicTableSummaryDto extends LogicTableSummaryDto {
    private String mqType;
    private String serialization;
    private boolean order;
    private String orderKey;
    private boolean persistent;
    private List<String> excludeFilterTypes;
    private long delayTime;

    public List<String> getExcludeFilterTypes() {
        return excludeFilterTypes;
    }

    public void setExcludeFilterTypes(List<String> excludeFilterTypes) {
        this.excludeFilterTypes = excludeFilterTypes;
    }

    public MqLogicTableSummaryDto(List<Long> dbReplicationIds, LogicTableConfig config) {
        super(dbReplicationIds, config);
    }

    public String getMqType() {
        return mqType;
    }

    public void setMqType(String mqType) {
        this.mqType = mqType;
    }

    public String getSerialization() {
        return serialization;
    }

    public void setSerialization(String serialization) {
        this.serialization = serialization;
    }

    public boolean isOrder() {
        return order;
    }

    public void setOrder(boolean order) {
        this.order = order;
    }

    public String getOrderKey() {
        return orderKey;
    }

    public void setOrderKey(String orderKey) {
        this.orderKey = orderKey;
    }

    public boolean isPersistent() {
        return persistent;
    }

    public void setPersistent(boolean persistent) {
        this.persistent = persistent;
    }

    public long getDelayTime() {
        return delayTime;
    }

    public void setDelayTime(long delayTime) {
        this.delayTime = delayTime;
    }
}
