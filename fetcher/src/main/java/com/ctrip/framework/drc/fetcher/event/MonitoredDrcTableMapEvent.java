package com.ctrip.framework.drc.fetcher.event;

import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.core.driver.schema.data.TableKey;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.fetcher.event.meta.MetaEvent;
import com.ctrip.framework.drc.fetcher.resource.context.SchemasHistoryContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: Haibo Shen
 * @Date: 2021/3/17
 */
public class MonitoredDrcTableMapEvent extends TableMapLogEvent implements MetaEvent.Write<SchemasHistoryContext> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public MonitoredDrcTableMapEvent() {
        logEvent();
    }

    protected void logEvent() {
        DefaultEventMonitorHolder.getInstance().logBatchEvent("db.event", "drc table map", 1, 0);
    }

    @Override
    public void involve(SchemasHistoryContext context) throws Exception {
        TableKey tableKey = TableKey.from(getSchemaName(), getTableName());
        Columns columns = Columns.from(getColumns(), getIdentifiers());
        logger.info("\ntable name: " + tableKey.toString()
                + "\ncolumn names: " + columns.getNames()
                + "\nidentifiers: " + columns.getBitmapsOfIdentifier().toString()
                + "\ncolumn name identifiers: " + getIdentifiers()
                + "\ncolumns on update: " + columns.getBitmapsOnUpdate().toString());
        context.updateSchema(tableKey, columns);
    }

}
