package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;

import java.util.List;

/**
 * @Author limingdong
 * @create 2021/10/9
 */
public interface ITransactionEvent {

    void setDdl(boolean ddl);

    List<LogEvent> getEvents();

    void setEvents(List<LogEvent> logEvents);

    boolean passFilter();
}
