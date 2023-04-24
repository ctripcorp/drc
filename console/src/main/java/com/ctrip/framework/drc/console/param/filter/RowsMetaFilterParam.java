package com.ctrip.framework.drc.console.param.filter;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/4/24 10:24
 */
public class RowsMetaFilterParam {

    private List<String> whiteList;

    @Override
    public String toString() {
        return "RowsMetaFilterParam{" +
                "whiteList=" + whiteList +
                '}';
    }

    public List<String> getWhiteList() {
        return whiteList;
    }

    public void setWhiteList(List<String> whiteList) {
        this.whiteList = whiteList;
    }
}
