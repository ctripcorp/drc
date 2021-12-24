package com.ctrip.framework.drc.console.monitor;

import com.ctrip.xpipe.api.endpoint.Endpoint;

/**
 * @Author: hbshen
 * @Date: 2021/4/22
 */
public interface CurrentMetaManager {

    void updateMasterMySQL(String clusterId, Endpoint endpoint);

    void updateSlaveMySQL(String clusterId, Endpoint endpoint);

    void addSlaveMySQL(String mhaName, Endpoint endpoint);
}
