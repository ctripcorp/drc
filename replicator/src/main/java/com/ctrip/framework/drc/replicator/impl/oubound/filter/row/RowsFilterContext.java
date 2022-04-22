package com.ctrip.framework.drc.replicator.impl.oubound.filter.row;

import com.ctrip.framework.drc.core.server.common.enums.RowFilterType;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class RowsFilterContext {

    private RowFilterType filterType;

    private Object filterContext;

    public RowsFilterContext(RowFilterType filterType, Object filterContext) {
        this.filterType = filterType;
        this.filterContext = filterContext;
    }

    public RowFilterType getFilterType() {
        return filterType;
    }

    public Object getFilterContext() {
        return filterContext;
    }
}
