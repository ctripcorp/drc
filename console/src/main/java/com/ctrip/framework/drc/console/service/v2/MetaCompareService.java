package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.service.v2.impl.migrate.DbClusterCompareRes;
import com.ctrip.framework.drc.core.entity.Drc;

public interface MetaCompareService {

    String compareDrcMeta() throws Exception;

    String compareDrcMeta(Drc newDrc, Drc oldDrc);

    DbClusterCompareRes compareDbCluster(String dbClusterId);

    boolean isConsistent();

    boolean isConsistent(String res);
}
