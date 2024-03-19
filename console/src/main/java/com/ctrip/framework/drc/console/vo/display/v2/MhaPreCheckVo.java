package com.ctrip.framework.drc.console.vo.display.v2;

import java.util.List;
import java.util.Map;

/**
 * Created by dengquanliang
 * 2024/2/21 14:48
 */
public class MhaPreCheckVo {
    private boolean result;
    private List<Map<String, Object>> configs;

    public boolean isResult() {
        return result;
    }

    public void setResult(boolean result) {
        this.result = result;
    }

    public List<Map<String, Object>> getConfigs() {
        return configs;
    }

    public void setConfigs(List<Map<String, Object>> configs) {
        this.configs = configs;
    }
}
