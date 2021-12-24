package com.ctrip.framework.drc.console.pojo;

import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.xpipe.api.endpoint.Endpoint;

import java.util.Map;

/**
 * Created by jixinwang on 2021/8/2
 */
public class MonitorMetaInfo {

    Map<MetaKey, MySqlEndpoint> masterMySQLEndpoint;

    Map<MetaKey, MySqlEndpoint> slaveMySQLEndpoint;

    Map<MetaKey, Endpoint> masterReplicatorEndpoint;

    public Map<MetaKey, MySqlEndpoint> getMasterMySQLEndpoint() {
        return masterMySQLEndpoint;
    }

    public void setMasterMySQLEndpoint(Map<MetaKey, MySqlEndpoint> masterMySQLEndpoint) {
        this.masterMySQLEndpoint = masterMySQLEndpoint;
    }

    public Map<MetaKey, MySqlEndpoint> getSlaveMySQLEndpoint() {
        return slaveMySQLEndpoint;
    }

    public void setSlaveMySQLEndpoint(Map<MetaKey, MySqlEndpoint> slaveMySQLEndpoint) {
        this.slaveMySQLEndpoint = slaveMySQLEndpoint;
    }

    public Map<MetaKey, Endpoint> getMasterReplicatorEndpoint() {
        return masterReplicatorEndpoint;
    }

    public void setMasterReplicatorEndpoint(Map<MetaKey, Endpoint> masterReplicatorEndpoint) {
        this.masterReplicatorEndpoint = masterReplicatorEndpoint;
    }
}
