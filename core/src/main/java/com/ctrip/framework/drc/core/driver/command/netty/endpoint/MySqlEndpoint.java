package com.ctrip.framework.drc.core.driver.command.netty.endpoint;

import com.ctrip.xpipe.api.endpoint.Endpoint;

/**
 * @Author: hbshen
 * @Date: 2021/4/20
 */
public class MySqlEndpoint extends DefaultEndPoint implements Endpoint {

    private boolean master;

    public MySqlEndpoint(String ip, int port, String username, String password, boolean master) {
        super(ip, port, username, password);
        this.master = master;
    }

    public boolean isMaster() {
        return master;
    }

    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }
}
