package com.ctrip.framework.drc.console.param.log;

import com.ctrip.framework.drc.core.http.PageReq;

/**
 * Created by dengquanliang
 * 2024/1/15 17:23
 */
public class ConflictDbBlacklistQueryParam {
    private String dbFilter;
    private Integer type;
    private PageReq pageReq;

    public String getDbFilter() {
        return dbFilter;
    }

    public void setDbFilter(String dbFilter) {
        this.dbFilter = dbFilter;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public PageReq getPageReq() {
        return pageReq;
    }

    public void setPageReq(PageReq pageReq) {
        this.pageReq = pageReq;
    }

    @Override
    public String toString() {
        return "ConflictDbBlacklistQueryParam{" +
                "dbFilter='" + dbFilter + '\'' +
                ", type=" + type +
                ", pageReq=" + pageReq +
                '}';
    }
}
