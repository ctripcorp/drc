package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.service.v2.impl.migrate.DbClusterCompareRes;
import com.ctrip.framework.drc.core.entity.Drc;

public interface MetaGrayService {

    Drc getDrc();

    Drc getDrc(String dcId);

    DbClusterCompareRes compareDbCluster(String dbClusterId);

    String compareDrcMeta() throws Exception;
    

}
