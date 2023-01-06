package com.ctrip.framework.drc.core.meta;

import com.ctrip.framework.drc.core.meta.ColumnsFilterConfig;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import java.util.List;

/**
 * @ClassName ApplierProperties
 * @Author haodongPan
 * @Version: $
 */
public class ApplierProperties {
    
    // rowsFilters
    private DataMediaConfig dataMediaConfig;
    
    // columnsFilters
    private List<ColumnsFilterConfig> columnsFilterConfigs;

    public DataMediaConfig getDataMediaConfig() {
        return dataMediaConfig;
    }

    public void setDataMediaConfig(DataMediaConfig dataMediaConfig) {
        this.dataMediaConfig = dataMediaConfig;
    }

    public List<ColumnsFilterConfig> getColumnsFilterConfigs() {
        return columnsFilterConfigs;
    }

    public void setColumnsFilterConfigs(
            List<ColumnsFilterConfig> columnsFilterConfigs) {
        this.columnsFilterConfigs = columnsFilterConfigs;
    }
}
