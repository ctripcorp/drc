package com.ctrip.framework.drc.applier.event.mq;

import com.ctrip.framework.drc.applier.event.ApplierTableMapEvent;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.fetcher.resource.transformer.TransformerContext;

/**
 * Created by jixinwang on 2022/10/19
 */
public class MqApplierTableMapEvent extends ApplierTableMapEvent {

    @Override
    public void transformer(TransformerContext transformerContext) {

    }

    protected void logTableMapEvent() {
        DefaultEventMonitorHolder.getInstance().logBatchEvent("mq.event", "table map", 1, 0);
    }

    @Override
    protected void logEvent() {
        DefaultEventMonitorHolder.getInstance().logBatchEvent("mq.db", getSchemaName(), 1, 0);
        DefaultEventMonitorHolder.getInstance().logBatchEvent("mq.table", getSchemaName() + "." + getTableName(), 1, 0);
    }
}
