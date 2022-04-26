package com.ctrip.framework.drc.core.server.common.filter.row;

import com.ctrip.framework.drc.core.server.common.enums.RowFilterType;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class RowsFilterContext {

    private String registryKey;

    private RowFilterType filterType;

    private String filterContext;

    public RowsFilterContext(String registryKey, RowFilterType filterType, String filterContext) {
        this.registryKey = registryKey;
        this.filterType = filterType;
        this.filterContext = filterContext;
    }

    public RowFilterType getFilterType() {
        return filterType;
    }

    public String getFilterContext() {
        return filterContext;
    }

    public String getRegistryKey() {
        return registryKey;
    }

    public static RowsFilterContext from(String registryKey, RowFilterType filterType, String filterContext) {
        return new RowsFilterContext(registryKey, filterType, filterContext);
    }
}
