package com.ctrip.framework.drc.console.monitor.consistency.cases;

import java.util.Map;

/**
 * Created by jixinwang on 2021/2/20
 */
public class QueryResultGroup {
    Map<String, Row> srcQueryResult;
    Map<String, Row> dstQueryResult;

    public QueryResultGroup() {
    }

    public QueryResultGroup(Map<String, Row> srcQueryResult, Map<String, Row> dstQueryResult) {
        this.srcQueryResult = srcQueryResult;
        this.dstQueryResult = dstQueryResult;
    }

    public Map<String, Row> getSrcQueryResult() {
        return srcQueryResult;
    }

    public void setSrcQueryResult(Map<String, Row> srcQueryResult) {
        this.srcQueryResult = srcQueryResult;
    }

    public Map<String, Row> getDstQueryResult() {
        return dstQueryResult;
    }

    public void setDstQueryResult(Map<String, Row> dstQueryResult) {
        this.dstQueryResult = dstQueryResult;
    }
}
