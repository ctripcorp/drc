package com.ctrip.framework.drc.core.server.config.console.dto;

import java.util.Map;

/**
 * Created by dengquanliang
 * 2025/3/21 16:10
 */
public class MhaDelayDto {

    private Map<String, Long> mhaDelay;

    public MhaDelayDto() {
    }

    public MhaDelayDto(Map<String, Long> mhaDelay) {
        this.mhaDelay = mhaDelay;
    }

    public Map<String, Long> getMhaDelay() {
        return mhaDelay;
    }

    public void setMhaDelay(Map<String, Long> mhaDelay) {
        this.mhaDelay = mhaDelay;
    }
}
