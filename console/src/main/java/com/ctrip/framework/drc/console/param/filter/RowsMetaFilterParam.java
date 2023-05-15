package com.ctrip.framework.drc.console.param.filter;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/4/24 10:24
 */
public class RowsMetaFilterParam {

    private String metaFilterName;
    private List<String> whitelist;

    @Override
    public String toString() {
        return "RowsMetaFilterParam{" +
                "metaFilterName='" + metaFilterName + '\'' +
                ", whitelist=" + whitelist +
                '}';
    }

    public String getMetaFilterName() {
        return metaFilterName;
    }

    public void setMetaFilterName(String metaFilterName) {
        this.metaFilterName = metaFilterName;
    }

    public List<String> getWhitelist() {
        return whitelist;
    }

    public void setWhitelist(List<String> whitelist) {
        this.whitelist = whitelist;
    }
}
