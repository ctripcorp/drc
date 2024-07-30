package com.ctrip.framework.drc.replicator.impl.oubound.filter.context;

import com.ctrip.framework.drc.core.meta.DataMediaConfig;

/**
 * @author yongnian
 */
public interface ExtractContext extends MonitorContext {
    DataMediaConfig getDataMediaConfig();

    boolean shouldFilterRows();

    boolean shouldFilterColumns();
}
