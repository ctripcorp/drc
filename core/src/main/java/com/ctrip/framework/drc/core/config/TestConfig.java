package com.ctrip.framework.drc.core.config;

import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;

/**
 * Created by jixinwang on 2022/7/21
 */
public class TestConfig {

    private ApplyMode applyMode;

    private String rowsFilter;

    public TestConfig() {
    }

    public TestConfig(ApplyMode applyMode, String rowsFilter) {
        this.applyMode = applyMode;
        this.rowsFilter = rowsFilter;
    }

    public ApplyMode getApplyMode() {
        return applyMode;
    }

    public void setApplyMode(ApplyMode applyMode) {
        this.applyMode = applyMode;
    }

    public String getRowsFilter() {
        return rowsFilter;
    }

    public void setRowsFilter(String rowsFilter) {
        this.rowsFilter = rowsFilter;
    }
}
