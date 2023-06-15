package com.ctrip.framework.drc.console.vo.response.migrate;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/6/15 17:31
 */
public class MhaDbMappingResult {
    private List<String> notExistMhaNames;
    private List<String> notExistDbNames;

    public MhaDbMappingResult(List<String> notExistMhaNames, List<String> notExistDbNames) {
        this.notExistMhaNames = notExistMhaNames;
        this.notExistDbNames = notExistDbNames;
    }

    public List<String> getNotExistMhaNames() {
        return notExistMhaNames;
    }

    public void setNotExistMhaNames(List<String> notExistMhaNames) {
        this.notExistMhaNames = notExistMhaNames;
    }

    public List<String> getNotExistDbNames() {
        return notExistDbNames;
    }

    public void setNotExistDbNames(List<String> notExistDbNames) {
        this.notExistDbNames = notExistDbNames;
    }
}
