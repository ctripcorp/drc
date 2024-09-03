package com.ctrip.framework.drc.console.service;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-03-05
 */
public interface SwitchService {
    void switchUpdateDb(String cluster, String endpoint,boolean firstHand);

    void switchListenReplicator(String clusterId, String endpoint,boolean firstHand);
}
