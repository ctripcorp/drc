package com.ctrip.framework.drc.console.pojo;

import java.util.List;

/**
 * @Author limingdong
 * @create 2021/7/8
 */
public class TableConfigs {

    private String defaultUcsShardColumn;

    private List<TableConfig> tableConfigs;

    public String getDefaultUcsShardColumn() {
        return defaultUcsShardColumn;
    }

    public void setDefaultUcsShardColumn(String defaultUcsShardColumn) {
        this.defaultUcsShardColumn = defaultUcsShardColumn;
    }

    public List<TableConfig> getTableConfigs() {
        return tableConfigs;
    }

    public void setTableConfigs(List<TableConfig> tableConfigs) {
        this.tableConfigs = tableConfigs;
    }
}
