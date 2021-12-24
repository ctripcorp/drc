package com.ctrip.framework.drc.console.dto;

/**
 * Created by jixinwang on 2020/7/7
 */
public class LogHandleDto {

    private String currentDcName;

    private  String currentDbClusterId;

    private String currentSql;

    public String getCurrentDcName() {
        return currentDcName;
    }

    public void setCurrentDcName(String currentDcName) {
        this.currentDcName = currentDcName;
    }

    public String getCurrentDbClusterId() {
        return currentDbClusterId;
    }

    public void setCurrentDbClusterId(String currentDbClusterId) {
        this.currentDbClusterId = currentDbClusterId;
    }

    public String getCurrentSql() {
        return currentSql;
    }

    public void setCurrentSql(String currentSql) {
        this.currentSql = currentSql;
    }
}
