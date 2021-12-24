package com.ctrip.framework.drc.console.monitor.consistency.sql.page;

/**
 * Created by mingdongli
 * 2019/11/19 下午11:21.
 */
public interface PagedExecution extends ResultSizeAware {

    int LIMIT = 100;

    boolean hasMore();
}
