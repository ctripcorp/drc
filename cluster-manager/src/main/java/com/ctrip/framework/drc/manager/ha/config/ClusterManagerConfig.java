package com.ctrip.framework.drc.manager.ha.config;

import com.ctrip.framework.drc.manager.ha.meta.RegionInfo;
import com.ctrip.xpipe.config.ZkConfig;
import com.ctrip.xpipe.tuple.Pair;

import java.util.Map;

/**
 * @Author limingdong
 * @create 2020/4/19
 */
public interface ClusterManagerConfig extends ZkConfig {

    String getConsoleAddress();

    int getClusterRefreshMilli();

    int getCheckMaxTime();

    String getClusterServerId();

    String getClusterServerIp();

    int getClusterServerPort();

    int getClusterServersRefreshMilli();

    int getSlotRefreshMilli();

    int getLeaderCheckMilli();

    Map<String, RegionInfo> getCmRegionInfos();

    Map<String, RegionInfo> getConsoleRegionInfos();

    Map<String, String> getMigrationIdc();

    String getMigrationBlackIps();

    boolean getRealtimeMetaInfo();

    boolean checkApplierProperty();

    boolean refreshWhenConsoleInvoke();

    Pair<String, Integer> getApplierMaster(String key);

    int getWaitforOffsetMilli();

    boolean validateDomain();

    int getCheckInterval();

    boolean getPeriodCheckSwitch();

    boolean getPeriodCorrectSwitch();

    boolean requestWithHeraldToken();


    int getConsoleBatchNotifySize();


    boolean getCmBatchNotifyConsoleSwitch();
}
