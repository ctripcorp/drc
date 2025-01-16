package com.ctrip.framework.drc.console.dto.v3;

import java.util.List;

/**
 * @author yongnian
 * @create 2025/1/15 15:24
 */
public class ShardDatabaseInfoDto {
    private String dalClusterName;
    private List<String> dbNames;

    public ShardDatabaseInfoDto(String dalClusterName, List<String> dbNames) {
        this.dalClusterName = dalClusterName;
        this.dbNames = dbNames;
    }

    public List<String> getDbNames() {
        return dbNames;
    }

    public void setDbNames(List<String> dbNames) {
        this.dbNames = dbNames;
    }

    public String getDalClusterName() {
        return dalClusterName;
    }

    public void setDalClusterName(String dalClusterName) {
        this.dalClusterName = dalClusterName;
    }
}
