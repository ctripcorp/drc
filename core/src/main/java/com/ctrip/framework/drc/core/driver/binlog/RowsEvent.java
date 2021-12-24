package com.ctrip.framework.drc.core.driver.binlog;

import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;

import java.util.List;

/**
 * @author wenchao.meng
 * <p>
 * Sep 01, 2019
 */
public interface RowsEvent extends LogEvent{

    void load(final List<TableMapLogEvent.Column> columns);
}
