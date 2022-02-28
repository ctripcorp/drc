package com.ctrip.framework.drc.fetcher.system;

import com.ctrip.framework.drc.fetcher.system.lifecycle.DrcLifecycle;
import com.ctrip.framework.drc.fetcher.system.lifecycle.Lifecycle;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @Author Slight
 * Sep 18, 2019
 */
public class AbstractSystem extends DrcLifecycle implements Lifecycle, ConfigLoader {

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    private String name;

    private SystemStatus status = SystemStatus.RUNNABLE;

    public SystemStatus getStatus() {
        return status;
    }

    public void setStatus(SystemStatus status) {
        this.status = status;
    }

    public Object getConfig() {
        return config;
    }

    public Class getConfigType() {
        return configType;
    }

    public void setConfig(Object config, Class configType) {
        this.config = config;
        this.configType = configType;
    }

    private Object config;
    private Class configType;

    protected Map<String, Activity> activities = new LinkedHashMap<String, Activity>();
    protected Map<String, Resource> resources = new LinkedHashMap<String, Resource>();

    @Override
    public void doInitialize() throws Exception {
        for (Resource resource : resources.values()) {
            resource.load();
            resource.initialize();
        }
        for (Activity activity : activities.values()) {
            activity.load();
            activity.initialize();
        }
    }

    @Override
    public void doStart() throws Exception {
        for (Activity activity : activities.values()) {
            activity.start();
        }
    }

    @Override
    public void doStop() throws Exception {
        for (Activity activity : activities.values()) {
            activity.stop();
        }
    }

    @Override
    public void doDispose() throws Exception {
        for (Activity activity : activities.values()) {
            activity.dispose();
        }
        for (Resource resource : resources.values()) {
            resource.dispose();
        }
    }

    public void mustShutdown() throws InterruptedException {
        while (!isDisposed()) {
            try {
                stop();
                dispose();
                break;
            } catch (Throwable t) {
                TimeUnit.SECONDS.sleep(1);
                logger.error("- UNLIKELY - fail to stop & dispose system", t);
            }
        }
    }
}
