package com.ctrip.framework.drc.replicator.impl.inbound.event;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.LogEventCallBack;
import com.ctrip.framework.drc.core.exception.dump.BinlogDumpGtidException;
import com.ctrip.framework.drc.core.server.common.filter.Filter;
import com.ctrip.framework.drc.core.server.observer.gtid.GtidObservable;
import com.ctrip.framework.drc.core.server.observer.gtid.GtidObserver;
import com.ctrip.framework.drc.core.server.observer.uuid.UuidObserver;
import com.ctrip.framework.drc.replicator.impl.inbound.filter.InboundLogEventContext;
import com.ctrip.framework.drc.core.server.common.filter.Resettable;
import com.ctrip.framework.drc.replicator.impl.inbound.filter.TransactionFlags;
import com.ctrip.framework.drc.replicator.impl.inbound.transaction.TransactionCache;
import com.ctrip.framework.drc.replicator.impl.monitor.DefaultMonitorManager;
import com.ctrip.framework.drc.replicator.impl.oubound.observer.MonitorEventObservable;
import com.ctrip.framework.drc.replicator.impl.oubound.observer.MonitorEventObserver;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * Created by @author zhuYongMing on 2019/9/18.
 */
public class ReplicatorLogEventHandler implements ObservableLogEventHandler, GtidObservable, UuidObserver, MonitorEventObservable, Resettable {

    private Filter<InboundLogEventContext> filterChain;

    private List<Observer> observers = Lists.newCopyOnWriteArrayList();

    private DefaultMonitorManager delayMonitor;

    private TransactionCache transactionCache;

    public ReplicatorLogEventHandler(TransactionCache transactionCache, DefaultMonitorManager delayMonitor, Filter<InboundLogEventContext> filterChain) {
        this.filterChain = filterChain;
        this.transactionCache = transactionCache;
        this.delayMonitor = delayMonitor;
    }

    private String currentGtid = StringUtils.EMPTY;

    private TransactionFlags flags = new TransactionFlags();

    /**
     * write and release
     */
    @Override
    public synchronized void onLogEvent(LogEvent logEvent, LogEventCallBack logEventCallBack, BinlogDumpGtidException exception) {

        InboundLogEventContext eventWithGroupFlag = new InboundLogEventContext(logEvent, logEventCallBack, flags, currentGtid);
        filterChain.doFilter(eventWithGroupFlag);
        currentGtid = eventWithGroupFlag.getGtid();
    }

    @Override
    public void addObserver(Observer observer) {
        if (observer != null && !observers.contains(observer)) {
            if (observer instanceof MonitorEventObserver) {
                delayMonitor.addObserver(observer);
            } else if (observer instanceof GtidObserver) {
                transactionCache.addObserver(observer);
            } else {
                observers.add(observer);
            }
        }
    }

    @Override
    public void removeObserver(Observer observer) {
        if (observer instanceof MonitorEventObserver) {
            delayMonitor.removeObserver(observer);
        } else if (observer instanceof GtidObserver) {
            transactionCache.removeObserver(observer);
        } else {
            observers.remove(observer);
        }
    }

    @Override
    public synchronized void update(Object args, Observable observable) {
        Filter filter = filterChain;
        do {
            if (filter instanceof UuidObserver) {
                UuidObserver uuidObserver = (UuidObserver) filter;
                uuidObserver.update(args, observable);
            }
            filter = filter.getSuccessor();
        } while (filter != null);
    }

    public String getCurrentGtid() {
        return currentGtid;
    }

    @Override
    public void reset() {
        currentGtid = StringUtils.EMPTY;
        transactionCache.reset();
        filterChain.reset();
    }

    @VisibleForTesting
    public Filter<InboundLogEventContext> getFilterChain() {
        return filterChain;
    }
}
