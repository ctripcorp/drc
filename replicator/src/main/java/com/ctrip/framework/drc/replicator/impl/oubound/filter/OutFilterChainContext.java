package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.replicator.impl.oubound.filter.context.ChannelContext;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.context.ExtractContext;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.context.FilterChainContext;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.context.MonitorContext;

/**
 * @author yongnian
 */
public interface OutFilterChainContext extends FilterChainContext, ChannelContext, ExtractContext, MonitorContext {
}
