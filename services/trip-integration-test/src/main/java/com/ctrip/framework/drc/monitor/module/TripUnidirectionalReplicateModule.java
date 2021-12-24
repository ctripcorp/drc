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

    public TripUnidirectionalReplicateModule(int srcMySQLPort, int destMySQLPort, int metaMySQLPort, int repPort, String registryKey) {
        super(srcMySQLPort, destMySQLPort, metaMySQLPort, repPort, registryKey);
    }

    @Override
    protected DrcMonitorModule getDrcMonitorModule() {
        return new DrcMonitorModule(srcMySQLPort, destMySQLPort, metaMySQLPort);
    }
}
