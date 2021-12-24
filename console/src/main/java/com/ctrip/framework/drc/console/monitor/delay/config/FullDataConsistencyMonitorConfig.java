package com.ctrip.framework.drc.console.monitor.delay.config;

/**
 * Created by jixinwang on 2021/2/20
 */
public class FullDataConsistencyMonitorConfig extends DelayMonitorConfig {

    private int tableId;

    private String startTimestamp;

    private String endTimeStamp;

    private String mhaAName;

    private String mhaBName;

    public int getTableId() {
        return tableId;
    }

    public void setTableId(int tableId) {
        this.tableId = tableId;
    }

    public String getStartTimestamp() {
        return startTimestamp;
    }

    public void setStartTimestamp(String startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    public String getEndTimeStamp() {
        return endTimeStamp;
    }

    public void setEndTimeStamp(String endTimeStamp) {
        this.endTimeStamp = endTimeStamp;
    }

    public String getMhaAName() {
        return mhaAName;
    }

    public void setMhaAName(String mhaAName) {
        this.mhaAName = mhaAName;
    }

    public String getMhaBName() {
        return mhaBName;
    }

    public void setMhaBName(String mhaBName) {
        this.mhaBName = mhaBName;
    }
}
