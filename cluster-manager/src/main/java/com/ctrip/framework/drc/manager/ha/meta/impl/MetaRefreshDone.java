package com.ctrip.framework.drc.manager.ha.meta.impl;

/**
 * @author yongnian
 * @create 2024/11/4 15:55
 */
public class MetaRefreshDone {
    private static final MetaRefreshDone INSTANCE = new MetaRefreshDone();

    public static MetaRefreshDone getInstance() {
        return INSTANCE;
    }

    private MetaRefreshDone() {
    }
}
