package com.ctrip.framework.drc.console.mock;

import com.ctrip.xpipe.api.config.Config;
import com.ctrip.xpipe.api.config.ConfigChangeListener;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-04-30
 */
public class MockConfig implements Config {
    @Override
    public String get(String s) {
        return null;
    }

    @Override
    public String get(String s, String s1) {
        return s1;
    }

    @Override
    public void addConfigChangeListener(ConfigChangeListener configChangeListener) {

    }

    @Override
    public void removeConfigChangeListener(ConfigChangeListener configChangeListener) {

    }

    @Override
    public int getOrder() {
        return 0;
    }
}
