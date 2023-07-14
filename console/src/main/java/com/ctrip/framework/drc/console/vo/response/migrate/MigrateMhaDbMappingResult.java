package com.ctrip.framework.drc.console.vo.response.migrate;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/6/15 18:02
 */
public class MigrateMhaDbMappingResult {
    private int total;
    private List<String> notExistMhaNames;

    public MigrateMhaDbMappingResult(int total, List<String> notExistMhaNames) {
        this.total = total;
        this.notExistMhaNames = notExistMhaNames;
    }

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public List<String> getNotExistMhaNames() {
        return notExistMhaNames;
    }

    public void setNotExistMhaNames(List<String> notExistMhaNames) {
        this.notExistMhaNames = notExistMhaNames;
    }
}
