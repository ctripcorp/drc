package com.ctrip.framework.drc.monitor.module;

import com.ctrip.framework.drc.monitor.DrcMonitorModule;
import com.ctrip.framework.drc.monitor.module.replicate.UnidirectionalReplicateModule;

/**
 * @Author limingdong
 * @create 2021/12/9
 */
public class TripUnidirectionalReplicateModule extends UnidirectionalReplicateModule {

    public TripUnidirectionalReplicateModule() {
        super();
    }

    public TripUnidirectionalReplicateModule(int srcMySQLPort, int destMySQLPort, int repPort, String registryKey) {
        super(srcMySQLPort, destMySQLPort, repPort, registryKey);
    }

    @Override
    protected DrcMonitorModule getDrcMonitorModule() {
        return new DrcMonitorModule(srcMySQLPort, destMySQLPort);
    }
}
