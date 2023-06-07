package com.ctrip.framework.drc.console.vo.filter;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/4/25 15:08
 */
public class QConfigDataVO {
    private int version;
    private List<String> whitelist;

    @Override
    public String toString() {
        return "QConfigDataVO{" +
                "version=" + version +
                ", whitelist=" + whitelist +
                '}';
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public List<String> getWhitelist() {
        return whitelist;
    }

    public void setWhitelist(List<String> whitelist) {
        this.whitelist = whitelist;
    }
}
