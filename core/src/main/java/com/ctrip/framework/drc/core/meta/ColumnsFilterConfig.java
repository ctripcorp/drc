package com.ctrip.framework.drc.core.meta;

import com.ctrip.framework.drc.core.server.common.filter.column.ColumnsFilterMode;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;

/**
 * Created by jixinwang on 2022/12/21
 */
public class ColumnsFilterConfig {

    @JsonIgnore
    private String registryKey;

    private String mode;

    private String tables;

    private List<String> columns;

    public String getRegistryKey() {
        return registryKey;
    }

    public void setRegistryKey(String registryKey) {
        this.registryKey = registryKey;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getTables() {
        return tables;
    }

    public void setTables(String tables) {
        this.tables = tables;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public ColumnsFilterMode getColumnsFilterMode() {
        return ColumnsFilterMode.getColumnFilterMode(mode);
    }

    public boolean shouldFilterColumns() {
        return ColumnsFilterMode.NONE != getColumnsFilterMode();
    }
}
