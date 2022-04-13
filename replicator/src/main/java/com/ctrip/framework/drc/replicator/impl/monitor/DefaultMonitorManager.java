package com.ctrip.framework.drc.replicator.impl.monitor;

import com.ctrip.framework.drc.core.driver.binlog.constant.QueryType;
import com.ctrip.framework.drc.core.driver.binlog.impl.DelayMonitorLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.ParsedDdlLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.UpdateRowsEvent;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.replicator.impl.oubound.observer.MonitorEventObservable;
import com.ctrip.framework.drc.replicator.impl.oubound.observer.MonitorEventObserver;
import com.ctrip.xpipe.api.observer.Observer;
import com.google.common.collect.Lists;

import java.util.List;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.DELAY_LOGGER;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.DRC_DELAY_MONITOR_TABLE_NAME;

/**
 * Created by mingdongli
 * 2019/12/13 上午9:36.
 */
public class DefaultMonitorManager implements MonitorEventObservable, MonitorManager {

    private List<Observer> observers = Lists.newCopyOnWriteArrayList();

    private boolean nextMonitorRowsEvent = false;

    private String registerKey;

    public DefaultMonitorManager(String registerKey) {
        this.registerKey = registerKey;
    }

    @Override
    public void onTableMapLogEvent(TableMapLogEvent tableMapLogEvent) {
        String tableName = tableMapLogEvent.getTableName();
        if (DRC_DELAY_MONITOR_TABLE_NAME.equalsIgnoreCase(tableName)) {
            nextMonitorRowsEvent = true;
        } else {
            nextMonitorRowsEvent = false;
        }
    }

    @Override
    public boolean onUpdateRowsEvent(UpdateRowsEvent updateRowsEvent, String gtid) {
        boolean released = false;
        if (nextMonitorRowsEvent) {
            DelayMonitorLogEvent delayMonitorLogEvent = getDelayMonitorLogEvent(updateRowsEvent, gtid);
            released = notify(delayMonitorLogEvent);
            releaseDelayMonitorLogEvent(delayMonitorLogEvent);
        }
        return released;
    }

    private DelayMonitorLogEvent getDelayMonitorLogEvent(UpdateRowsEvent updateRowsEvent, String gtid) {
        DelayMonitorLogEvent delayMonitorLogEvent = null;
        DELAY_LOGGER.debug("[Filter] with nextMonitorRowsEvent of {} with gtid {}", nextMonitorRowsEvent, gtid);
        for (Observer observer : observers) {
            if (observer instanceof MonitorEventObserver) {
                if (delayMonitorLogEvent == null) {
                    delayMonitorLogEvent = new DelayMonitorLogEvent(gtid, updateRowsEvent);
                } else {
                    delayMonitorLogEvent.retain();
                    DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.delay.refcnt", registerKey);
                }
            }
        }
        return delayMonitorLogEvent;
    }

    private boolean notify(DelayMonitorLogEvent delayMonitorLogEvent) {
        boolean notify = delayMonitorLogEvent != null;
        if (notify) {
            for (Observer observer : observers) {
                if (observer instanceof MonitorEventObserver) {
                    observer.update(delayMonitorLogEvent, this);
                }
            }
        }
        return notify;
    }

    private void releaseDelayMonitorLogEvent(DelayMonitorLogEvent delayMonitorLogEvent) {
        if (delayMonitorLogEvent != null && delayMonitorLogEvent.isNeedReleased()) {
            try {
                delayMonitorLogEvent.release();
            } catch (Exception e) {
                DELAY_LOGGER.error("[Release] DelayMonitorLogEvent error", e);
            }
        }
    }

    @Override
    public void onDdlEvent(String schema, String tableName, String ddl, QueryType queryType) {
        for (Observer observer : observers) {
            if (observer instanceof MonitorEventObserver) {
                try {
                    ParsedDdlLogEvent parsedDdlLogEvent = new ParsedDdlLogEvent(schema, tableName, ddl, queryType);
                    observer.update(parsedDdlLogEvent, this);
                } catch (Exception e) {
                    DELAY_LOGGER.error("[onDdlEvent] for {}:{} error ", schema, tableName, e);
                }
            }
        }
    }

    @Override
    public void addObserver(Observer observer) {
        if (observer != null && observer instanceof MonitorEventObserver && !observers.contains(observer)) {
            observers.add(observer);
        }
    }

    @Override
    public void removeObserver(Observer observer) {
        observers.remove(observer);
    }
}
