package com.ctrip.framework.drc.console.monitor.consistency.table;

/**
 * Created by mingdongli
 * 2019/11/12 上午11:10.
 */
public interface TableNameAware extends KeyAware, OnUpdateAware {

    String getTable();
}
