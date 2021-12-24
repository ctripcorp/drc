package com.ctrip.framework.drc.console.monitor.consistency.cases;

import java.sql.Timestamp;
import java.util.Map;

/**
 * Created by jixinwang on 2021/2/20
 */
public class BaseQueryResult {
    Map<String, Row> queryResult;
    Timestamp onUpdate;

    public Map<String, Row> getQueryResult() {
        return queryResult;
    }

    public void setQueryResult(Map<String, Row> queryResult) {
        this.queryResult = queryResult;
    }

    public Timestamp getOnUpdate() {
        return onUpdate;
    }

    public void setOnUpdate(Timestamp onUpdate) {
        this.onUpdate = onUpdate;
    }
}
