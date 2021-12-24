package com.ctrip.framework.drc.console.dto;

import java.util.Map;

/**
 * Created by jixinwang on 2021/2/24
 */
public class CurrentResultSetDto {
    private String keyValue;
    private Map<String, Object> currentResult;

    public String getKeyValue() {
        return keyValue;
    }

    public void setKeyValue(String keyValue) {
        this.keyValue = keyValue;
    }

    public Map<String, Object> getCurrentResult() {
        return currentResult;
    }

    public void setCurrentResult(Map<String, Object> currentResult) {
        this.currentResult = currentResult;
    }
}
