package com.ctrip.framework.drc.console.monitor.delay.config;

import com.ctrip.framework.drc.core.entity.Drc;

public interface Config {
    String getSourceType();

    String getConfig();

    Drc getDrc();

    void updateConfig();
}