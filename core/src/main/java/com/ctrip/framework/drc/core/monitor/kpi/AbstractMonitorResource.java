package com.ctrip.framework.drc.core.monitor.kpi;

import com.ctrip.framework.drc.core.monitor.entity.TrafficEntity;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.core.monitor.reporter.Reporter;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;

/**
 * @Author limingdong
 * @create 2020/3/17
 */
public abstract class AbstractMonitorResource extends AbstractLifecycle {

    protected long domain;

    protected String clusterName;

    protected TrafficEntity trafficEntity;

    protected Reporter hickwallReporter = DefaultReporterHolder.getInstance();

}
