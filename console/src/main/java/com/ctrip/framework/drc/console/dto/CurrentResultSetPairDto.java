package com.ctrip.framework.drc.console.dto;

import java.util.Map;

/**
 * Created by jixinwang on 2021/2/24
 */
public class CurrentResultSetPairDto {
    private Map<String, Object> mhaACurrentResult;
    private Map<String, Object> mhaBCurrentResult;

    public Map<String, Object> getMhaACurrentResult() {
        return mhaACurrentResult;
    }

    public void setMhaACurrentResult(Map<String, Object> mhaACurrentResult) {
        this.mhaACurrentResult = mhaACurrentResult;
    }

    public Map<String, Object> getMhaBCurrentResult() {
        return mhaBCurrentResult;
    }

    public void setMhaBCurrentResult(Map<String, Object> mhaBCurrentResult) {
        this.mhaBCurrentResult = mhaBCurrentResult;
    }
}
