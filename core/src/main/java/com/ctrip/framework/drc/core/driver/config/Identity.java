package com.ctrip.framework.drc.core.driver.config;

import com.ctrip.framework.drc.core.server.config.RegistryKey;
import com.ctrip.xpipe.api.endpoint.Endpoint;

import java.util.Objects;

/**
 * Created by mingdongli
 * 2019/10/11 上午9:49.
 */
public class Identity {

    private Endpoint endpoint;

    private String registryKey;

    public Endpoint getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(Endpoint endpoint) {
        this.endpoint = endpoint;
    }

    public String getRegistryKey() {
        return registryKey;
    }

    public void setRegistryKey(String registryKey) {  //only for applier request binlog
        this.registryKey = registryKey;
    }

    public void setRegistryKey(String registryKey, String mhaName) {
        this.registryKey = RegistryKey.from(registryKey, mhaName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Identity identity = (Identity) o;
        return Objects.equals(endpoint, identity.endpoint) &&
                Objects.equals(registryKey, identity.registryKey);
    }

    @Override
    public int hashCode() {

        return Objects.hash(endpoint, registryKey);
    }
}
