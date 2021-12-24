package com.ctrip.framework.drc.replicator.impl.inbound.transaction;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.server.observer.gtid.GtidObservable;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;

import java.io.Flushable;

/**
 * @Author limingdong
 * @create 2020/4/24
 */
public interface TransactionCache extends Resettable, GtidObservable, Lifecycle, Flushable {

    boolean add(LogEvent logEvent);
}
