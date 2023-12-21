package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.binlog.impl.FilterLogEvent;
import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Sets;

import java.util.Set;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.drc_filter_log_event;
import static com.ctrip.framework.drc.core.driver.binlog.impl.FilterLogEvent.UNKNOWN;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.DRC_MONITOR_SCHEMA_NAME;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.GTID_LOGGER;

/**
 * Created by jixinwang on 2023/11/20
 */
public class SchemaFilter extends AbstractLogEventFilter<OutboundLogEventContext> {

    private String registerKey;

    private Set<String> schemas = Sets.newHashSet(UNKNOWN, DRC_MONITOR_SCHEMA_NAME);

    public SchemaFilter(OutboundFilterChainContext context) {
        registerKey = context.getRegisterKey();
        String nameFilter = context.getNameFilter();
        initSchemas(nameFilter);
        logger.info("[Filter][Schema] init send schemas: {} for {}", schemas, registerKey);
    }

    @Override
    public boolean doFilter(OutboundLogEventContext value) {
        if (drc_filter_log_event == value.getEventType()) {
            FilterLogEvent filterLogEvent = value.readFilterEvent();
            value.setLogEvent(filterLogEvent);
            String schema = filterLogEvent.getSchemaName();
            if (!schemas.contains(schema.toLowerCase())) {
                long nextTransactionOffset = filterLogEvent.getNextTransactionOffset();
                if (nextTransactionOffset > 0) {
                    value.skipPosition(nextTransactionOffset);
                    GTID_LOGGER.debug("[S][{}] filter schema, {}", registerKey, schema);
                }
            }
            value.setSkipEvent(true);
        }

        return doNext(value, value.isSkipEvent());
    }

    private void initSchemas(String nameFilter) {
        logger.info("[SCHEMA][ADD] for {}, nameFilter {}", registerKey, nameFilter);
        String[] schemaDotTableNames = nameFilter.split(",");

        for (String schemaDotTableName : schemaDotTableNames) {
            String[] schemaAndTable = schemaDotTableName.split("\\\\.");
            if (schemaAndTable.length > 1) {
                schemas.add(schemaAndTable[0].toLowerCase());
                logger.info("[SCHEMA][ADD] for {}, schema {}", registerKey, schemaAndTable[0]);
                continue;
            }

            String[] schemaAndTable2 = schemaDotTableName.split("\\.");
            if (schemaAndTable2.length > 1) {
                schemas.add(schemaAndTable2[0].toLowerCase());
                logger.info("[SCHEMA][ADD] for {}, schema {}", registerKey, schemaAndTable2[0]);
            }
        }
    }

    @VisibleForTesting
    protected Set<String> getSchemas() {
        return schemas;
    }
}
