package com.ctrip.framework.drc.console.vo.v2;

import com.ctrip.framework.drc.core.http.ApiResult;

import java.util.Map;

/**
 * Created by dengquanliang
 * 2023/11/28 14:29
 */
public class ConflictRecordVo extends ApiResult {
    private Map<String, Object> records;

    public Map<String, Object> getRecords() {
        return records;
    }

    public void setRecords(Map<String, Object> records) {
        this.records = records;
    }
}
