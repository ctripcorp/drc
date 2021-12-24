package com.ctrip.framework.drc.replicator.impl.inbound.event;

import com.ctrip.framework.drc.core.driver.binlog.LogEventHandler;
import com.ctrip.xpipe.api.observer.Observable;

/**
 * @Author limingdong
 * @create 2020/1/3
 */
public interface ObservableLogEventHandler extends LogEventHandler, Observable {
}
