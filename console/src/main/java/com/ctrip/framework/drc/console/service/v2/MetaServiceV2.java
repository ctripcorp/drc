package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.service.v2.impl.migrate.DbClusterCompareRes;

public interface MetaServiceV2 {

    DbClusterCompareRes compareDbCluster(String dbClusterId);

    String compareDrcMeta() throws Exception;
    
    String getDrcInGrayMode();

}
