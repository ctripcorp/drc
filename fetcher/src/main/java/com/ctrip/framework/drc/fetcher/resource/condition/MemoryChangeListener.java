package com.ctrip.framework.drc.fetcher.resource.condition;

import com.ctrip.framework.drc.core.driver.binlog.LogEventCallBack;

/**
 * @Author limingdong
 * @create 2020/11/26
 */
public interface MemoryChangeListener {

    void onMemoryChange(LogEventCallBack callBack);
}
