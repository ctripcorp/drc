package com.ctrip.framework.drc.core.server.container;

import com.ctrip.xpipe.api.lifecycle.ComponentRegistry;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by mingdongli
 * 2019/10/30 上午11:10.
 */
public class ComponentRegistryHolder {

    private static AtomicBoolean isRegistrySet = new AtomicBoolean(false);

    private static ComponentRegistry componentRegistry;

    public static void initializeRegistry(ComponentRegistry componentRegistryToSet) {
        if (!isRegistrySet.compareAndSet(false, true)) {
            return;
        }
        componentRegistry = componentRegistryToSet;
    }

    public static ComponentRegistry getComponentRegistry() {
        return componentRegistry;
    }
}
