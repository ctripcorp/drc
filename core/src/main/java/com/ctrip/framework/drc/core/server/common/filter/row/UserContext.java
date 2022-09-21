package com.ctrip.framework.drc.core.server.common.filter.row;

import java.util.Set;

/**
 * Created by jixinwang on 2022/5/25
 */
public class UserContext {

    private String userAttr;

    private Set<String> locations;

    private boolean illegalArgument;

    private String registryKey;

    public String getUserAttr() {
        return userAttr;
    }

    public void setUserAttr(String userAttr) {
        this.userAttr = userAttr;
    }

    public Set<String> getLocations() {
        return locations;
    }

    public void setLocations(Set<String> locations) {
        this.locations = locations;
    }

    public boolean getIllegalArgument() {
        return illegalArgument;
    }

    public void setIllegalArgument(boolean illegalArgument) {
        this.illegalArgument = illegalArgument;
    }

    public String getRegistryKey() {
        return registryKey;
    }

    public void setRegistryKey(String registryKey) {
        this.registryKey = registryKey;
    }
}
