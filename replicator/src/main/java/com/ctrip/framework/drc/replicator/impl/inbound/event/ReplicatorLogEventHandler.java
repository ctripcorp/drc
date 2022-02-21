package com.ctrip.framework.drc.replicator.impl.inbound.event;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.LogEventCallBack;
import com.ctrip.framework.drc.core.exception.dump.BinlogDumpGtidException;
import com.ctrip.framework.drc.core.server.common.Filter;
import com.ctrip.framework.drc.core.server.observer.gtid.GtidObservable;
import com.ctrip.framework.drc.core.server.observer.gtid.GtidObserver;
import com.ctrip.framework.drc.core.server.observer.uuid.UuidObserver;
import com.ctrip.framework.drc.replicator.impl.inbound.filter.LogEventWithGroupFlag;
import com.ctrip.framework.drc.replicator.impl.inbound.transaction.Resettable;
import com.ctrip.framework.drc.replicator.impl.inbound.transaction.TransactionCache;
import com.ctrip.framework.drc.replicator.impl.monitor.DefaultMonitorManager;
import com.ctrip.framework.drc.replicator.impl.oubound.observer.MonitorEventObservable;
import com.ctrip.framework.drc.replicator.impl.oubound.observer.MonitorEventObserver;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * Created by @author zhuYongMing on 2019/9/18.
 */
public class ReplicatorLogEventHandler implements ObservableLogEventHandler, GtidObservable, UuidObserver, MonitorEventObservable, Resettable {

    private Filter<LogEventWithGroupFlag> filterChain;

    private List<Observer> observers = Lists.newCopyOnWriteArrayList();

    private DefaultMonitorManager delayMonitor;

    private TransactionCache transactionCache;

    public ReplicatorLogEventHandler(TransactionCache transactionCache, DefaultMonitorManager delayMonitor, Filter<LogEventWithGroupFlag> filterChain) {
        this.filterChain = filterChain;
        this.transactionCache = transactionCache;
        this.delayMonitor = delayMonitor;
    }

    private String currentGtid = StringUtils.EMPTY;

    private boolean inExcludeGroup = false;

    private boolean tableFiltered = false;

    private boolean transactionTableRelated = false;

    /**
     * write and release
     */
    @Override
    public synchronized void onLogEvent(LogEvent logEvent, LogEventCallBack logEventCallBack, BinlogDumpGtidException exception) {

        LogEventWithGroupFlag eventWithGroupFlag = new LogEventWithGroupFlag(logEvent, inExcludeGroup, tableFiltered, transactionTableRelated, currentGtid);

        inExcludeGroup = filterChain.doFilter(eventWithGroupFlag);

        currentGtid = eventWithGroupFlag.getGtid();
        tableFiltered = eventWithGroupFlag.isTableFiltered();
        transactionTableRelated = eventWithGroupFlag.isTransactionTableRelated();
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
        transactionCache.reset();
    }
}
