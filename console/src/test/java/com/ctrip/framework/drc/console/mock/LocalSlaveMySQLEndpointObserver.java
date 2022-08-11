package com.ctrip.framework.drc.console.mock;

import com.ctrip.framework.drc.console.pojo.MetaKey;
import com.ctrip.framework.drc.console.task.AbstractSlaveMySQLEndpointObserver;
import com.ctrip.xpipe.api.endpoint.Endpoint;

/**
 * @Author: hbshen
 * @Date: 2021/4/28
 */
public class LocalSlaveMySQLEndpointObserver extends AbstractSlaveMySQLEndpointObserver {
    @Override
    public void clearOldEndpointResource(Endpoint endpoint) {

    }

    @Override
    public void setLocalDcName() {

    }

    @Override
    public void setLocalRegionInfo() {
        
    }

    @Override
    public void setOnlyCarePart() {

    }

    @Override
    public boolean isCare(MetaKey metaKey) {
        return localDcName.equalsIgnoreCase(metaKey.getDc());
    }

   
}
