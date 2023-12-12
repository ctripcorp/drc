package com.ctrip.framework.drc.console.param.log;

import com.ctrip.framework.drc.core.http.PageReq;

/**
 * @ClassName OperationLogQueryParam
 * @Author haodongPan
 * @Date 2023/12/8 15:56
 * @Version: $
 */
public class OperationLogQueryParam {
    
    private String type;
    private String attr;
    private String operator;
    private Long beginCreateTime;
    private Long endCreatTime;
    private Integer fail;
    private PageReq pageReq;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getAttr() {
        return attr;
    }

    public void setAttr(String attr) {
        this.attr = attr;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public Long getBeginCreateTime() {
        return beginCreateTime;
    }

    public void setBeginCreateTime(Long beginCreateTime) {
        this.beginCreateTime = beginCreateTime;
    }

    public Long getEndCreatTime() {
        return endCreatTime;
    }

    public void setEndCreatTime(Long endCreatTime) {
        this.endCreatTime = endCreatTime;
    }

    public Integer getFail() {
        return fail;
    }

    public void setFail(Integer fail) {
        this.fail = fail;
    }

    public PageReq getPageReq() {
        return pageReq;
    }

    public void setPageReq(PageReq pageReq) {
        this.pageReq = pageReq;
    }
}
