package com.ctrip.framework.drc.core.config;

import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;

/**
 * Created by jixinwang on 2022/7/21
 */
public class TestConfig {

    private ApplyMode applyMode;

    private String nameFilter;

    private String rowsFilter;

    private String properties;

    public TestConfig() {
    }

    public TestConfig(ApplyMode applyMode, String rowsFilter, String properties) {
        this.applyMode = applyMode;
        this.rowsFilter = rowsFilter;
        this.properties = properties;
    }

    public ApplyMode getApplyMode() {
        return applyMode;
    }

    public void setApplyMode(ApplyMode applyMode) {
        this.applyMode = applyMode;
    }

    public String getNameFilter() {
        return nameFilter;
    }

    public void setNameFilter(String nameFilter) {
        this.nameFilter = nameFilter;
    }

    public String getRowsFilter() {
        return rowsFilter;
    }

    public void setRowsFilter(String rowsFilter) {
        this.rowsFilter = rowsFilter;
    }

    public String getProperties() {
        return properties;
    }

    public void setProperties(String properties) {
        this.properties = properties;
    }
}
