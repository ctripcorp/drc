package com.ctrip.framework.drc.replicator.impl.oubound.filter.row;

import com.ctrip.framework.drc.core.server.common.enums.RowFilterType;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class RowsFilterContext {

    private RowFilterType filterType;

    private String filterContext;

    public RowsFilterContext(RowFilterType filterType, String filterContext) {
        this.filterType = filterType;
        this.filterContext = filterContext;
    }

    public RowFilterType getFilterType() {
        return filterType;
    }

    public String getFilterContext() {
        return filterContext;
    }

    public static RowsFilterContext from(RowFilterType filterType, String filterContext) {
        return new RowsFilterContext(filterType, filterContext);
    }
}
