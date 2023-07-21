package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.service.v2.impl.migrate.DbClusterCompareRes;

public interface MetaCompareService {

    String compareDrcMeta() throws Exception;

    DbClusterCompareRes compareDbCluster(String dbClusterId);

    boolean isConsistent();
}
