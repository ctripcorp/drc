package com.ctrip.framework.drc.monitor.module;

import com.ctrip.framework.drc.monitor.module.mysql.MySQLModule;

/**
 * Created by mingdongli
 * 2019/10/15 上午1:37.
 */
public interface ReplicateModule extends MySQLModule, RAModule, MonitorModule {

}
