package com.ctrip.framework.drc.console.monitor.delay.config;

import com.ctrip.framework.drc.core.entity.Route;

import java.util.List;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-01-03
 */
public interface DrcReplicatorWrapper {

    String getDcName();

    String getDestDcName();

    String getClusterName();

    String getMhaName();

    String getDestMhaName();

    String getIp();

    int getPort();

    List<Route> getRoutes();
}
