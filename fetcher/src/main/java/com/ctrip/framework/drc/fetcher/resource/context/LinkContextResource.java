package com.ctrip.framework.drc.fetcher.resource.context;

import com.ctrip.framework.drc.fetcher.system.InstanceConfig;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.schema.FixedRAMSchemasHistory;
import com.ctrip.framework.drc.core.driver.schema.SchemasHistory;

/**
 * @Author Slight
 * Sep 27, 2019
 */
public class LinkContextResource extends AbstractContext implements LinkContext {

    @InstanceConfig(path="gtidExecuted")
    public String initialGtidExecuted;

    protected FixedRAMSchemasHistory history;

    @Override
    public void doInitialize() {
        history = new FixedRAMSchemasHistory();
        updateGtidSet(new GtidSet(initialGtidExecuted));
        updateGtid("");
        logger.info("DB GTID EXECUTED: {}\n", initialGtidExecuted);
    }

    @Override
    public SchemasHistory getSchemasHistory() {
        return history;
    }
}
