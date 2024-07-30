package com.ctrip.framework.drc.replicator.impl.oubound.filter.context;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;

/**
 * @author yongnian
 */
public interface FilterChainContext {
    GtidSet getExcludedSet();

    String getRegisterKey();

    ConsumeType getConsumeType();
}
