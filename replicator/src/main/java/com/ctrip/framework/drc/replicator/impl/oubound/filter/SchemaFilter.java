package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.FilterLogEvent;
import com.ctrip.framework.drc.core.driver.util.LogEventUtils;
import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.context.FilterChainContext;
import com.google.common.collect.Sets;

import java.util.Set;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.*;
import static com.ctrip.framework.drc.core.driver.binlog.impl.FilterLogEvent.UNKNOWN;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.DRC_MONITOR_SCHEMA_NAME;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.GTID_LOGGER;

/**
 * Created by jixinwang on 2023/11/20
 */
public abstract class SchemaFilter extends AbstractLogEventFilter<OutboundLogEventContext> {

    private final String registerKey;
    private String previousSchema;

    public SchemaFilter(FilterChainContext context) {
        registerKey = context.getRegisterKey();
    }

    @Override
    public boolean doFilter(OutboundLogEventContext value) {
        if (drc_filter_log_event == value.getEventType()) {
            handleFilterLogEvent(value);
        } else {
            handleNonFilterLogEvent(value);
        }

        return doNext(value, value.isSkipEvent());
    }

    private void handleFilterLogEvent(OutboundLogEventContext value) {
        FilterLogEvent filterLogEvent = value.readFilterEvent();
        value.setLogEvent(filterLogEvent);
        previousSchema = filterLogEvent.getSchemaNameLowerCaseV2();
        value.setInExcludeGroup(!this.concern(previousSchema, filterLogEvent.getEventCount(), filterLogEvent.isNoRowsEvent()));
        if (value.isInExcludeGroup()) {
            long nextTransactionOffset = filterLogEvent.getNextTransactionOffset();
            if (nextTransactionOffset > 0) {
                this.skipTransaction(value, nextTransactionOffset);
                GTID_LOGGER.debug("[S][{}] filter schema, {}", registerKey, previousSchema);
            }
            value.setSkipEvent(true);
        } else {
            doConcern(value);
        }
    }

    public abstract void doConcern(OutboundLogEventContext value);


    private void handleNonFilterLogEvent(OutboundLogEventContext value) {
        LogEventType eventType = value.getEventType();
        if (value.isInExcludeGroup() && !LogEventUtils.isSlaveConcerned(eventType)) {
            skipEvent(value);
            value.setSkipEvent(true);
            //skip all transaction, clear in_exclude_group
            if (xid_log_event == eventType) {
                GTID_LOGGER.debug("[Reset] in_exclude_group to false, previous schema:{}", previousSchema);
                value.setInExcludeGroup(false);
            }
        }
    }

    public abstract void skipEvent(OutboundLogEventContext value);

    protected abstract boolean concern(String schema, int eventCount, boolean noRowsEvent);

    protected abstract void skipTransaction(OutboundLogEventContext value, long nextTransactionOffset);

    public static Set<String> getSchemas(String nameFilter) {
        Set<String> schemas = Sets.newHashSet(UNKNOWN, DRC_MONITOR_SCHEMA_NAME);
        String[] schemaDotTableNames = nameFilter.split(",");

        for (String schemaDotTableName : schemaDotTableNames) {
            String[] schemaAndTable = schemaDotTableName.split("\\\\.");
            if (schemaAndTable.length > 1) {
                schemas.add(schemaAndTable[0].toLowerCase());
                continue;
            }

            String[] schemaAndTable2 = schemaDotTableName.split("\\.");
            if (schemaAndTable2.length > 1) {
                schemas.add(schemaAndTable2[0].toLowerCase());
            }
        }
        return schemas;
    }
}
