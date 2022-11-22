package com.ctrip.framework.drc.manager.ha.config;

import static com.ctrip.framework.drc.core.driver.config.GlobalConfig.*;

/**
 * @Author limingdong
 * @create 2020/4/19
 */
public class ClusterZkConfig {

    public static String getClusterManagerLeaderElectPath(){
        return System.getProperty("zkClusterManagerStoragePath", "/cm/leader");
    }

    public static String getClusterManagerRegisterPath(){
        return System.getProperty("zkClusterManagerStoragePath", "/cm/servers");
    }

    public static String getClusterManagerSlotsPath(){
        return System.getProperty("zkClusterManagerStoragePath", "/cm/slots");
    }

    public static String getReplicatorLeaderLatchPath(String registryPath){
        String path = String.format("%s/%s", REPLICATOR_REGISTER_PATH, registryPath);
        return path;
    }

    public static String getApplierLeaderLatchPath(String registryPath){
        String path = String.format("%s/%s", APPLIER_REGISTER_PATH, registryPath);
        return path;
    }
}
