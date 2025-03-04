package com.ctrip.framework.drc.manager.ha.config;

import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.core.server.zookeeper.AbstractZookeeperConfig;
import com.ctrip.framework.drc.manager.ha.meta.RegionInfo;
import com.ctrip.xpipe.api.codec.GenericTypeReference;
import com.ctrip.xpipe.api.config.Config;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.config.CompositeConfig;
import com.ctrip.xpipe.config.DefaultPropertyConfig;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * @Author limingdong
 * @create 2020/4/19
 */
public class DefaultClusterManagerConfig extends AbstractZookeeperConfig implements ClusterManagerConfig {

    public static String KEY_CONSOLE_ADDRESS = "console.address";
    public static String KEY_CLUSTER_REFRESH_MILLI = "cluster.refresh.milli";
    public static String KEY_SLOT_REFRESH_MILLI = "slot.refresh.milli";
    public static String KEY_LEADER_CHECK_MILLI = "leader.check.milli";
    public static String KEY_CLUSTER_SERVERS_CHECK_MILLI = "cluster.servers.check.milli";
    public static String KEY_WAITFOR_OFFSET_MILLI = "dcchange.waitfor.offset.milli";
    public static String KEY_VALIDATE_DOMAIN = "metaserver.validate.domain";
    public static String KEY_CM_REGION_INFOS = "drc.cm.region.infos";
    public static String KEY_CONSOLE_REGION_INFOS = "drc.console.region.infos";
    public static String KEY_MIGRATION_IDC = "drc.migration.idcs";
    public static String KEY_MIGRATION_BLACK_IPS = "drc.migration.black.ips";
    public static String KEY_REALTIME_META_INFO = "drc.realtime.meta.info";
    public static String KEY_CHECK_APPLIER_PROPERTY = "check.applier.property";
    public static String KEY_REFRESH_WHEN_CONSOLE_INVOKE = "refresh.when.console.invoke";
    public static String KEY_PERIOD_META_CHECK = "period.meta.consistency.check";
    public static String KEY_PERIOD_META_CORRECT = "period.meta.consistency.correct";
    public static String KEY_HERALD_TOKEN_REQUEST_SWITCH = "herald.token.request.switch";

    public static String KEY_CONSOLE_BATCH_NOTIFY_SIZE = "console.batch.notify.size";
    public static String KEY_CM_BATCH_NOTIFY_CONSOLE_SWITCH = "cm.batch.notify.console.switch";



    public static String KEY_SERVER_ID = "clustermanager.id";
    public static String KEY_SERVER_IP = "server.ip";
    public static String KEY_SERVER_PORT = "server.port";

    private static final String KEY_INFO_CHECK_INTERVAL = "clustermanager.info.check.interval";
    private static final String KEY_INFO_CHECK_TASK_MAX_TIME = "clustermanager.info.check.max.time";

    private String defaultConsoleAddress = System.getProperty("consoleAddress", "http://localhost:8080");

    private String defaultClusterManagerId = System.getProperty(KEY_SERVER_ID, SystemConfig.LOCAL_SERVER_ADDRESS);
    private int defaultServerPort = Integer.parseInt(System.getProperty(KEY_SERVER_ID, "8080"));

    private Config serverConfig;

    private Map<String, RegionInfo> cmRegionInfos = Maps.newConcurrentMap();

    private Map<String, RegionInfo> consoleRegionInfos = Maps.newConcurrentMap();

    private Map<String, String> migrationIdcs = Maps.newConcurrentMap();

    public DefaultClusterManagerConfig(){

        CompositeConfig compositeConfig = new CompositeConfig();
        compositeConfig.addConfig(new DefaultPropertyConfig());
        serverConfig = compositeConfig;
    }


    @Override
    public int getConsoleBatchNotifySize() {
        return getIntProperty(KEY_CONSOLE_BATCH_NOTIFY_SIZE, 10);
    }

    @Override
    public boolean getCmBatchNotifyConsoleSwitch() {
        return getBooleanProperty(KEY_CM_BATCH_NOTIFY_CONSOLE_SWITCH, true);
    }


    @Override
    public String getConsoleAddress() {
        return getProperty(KEY_CONSOLE_ADDRESS, defaultConsoleAddress);
    }

    public void setDefaultConsoleAddress(String defaultConsoleAddress) {
        this.defaultConsoleAddress = defaultConsoleAddress;
    }

    public void setDefaultClusterManagerId(String defaultClusterManagerId) {
        this.defaultClusterManagerId = defaultClusterManagerId;
    }

    @Override
    public int getClusterRefreshMilli() {
        return getIntProperty(KEY_CLUSTER_REFRESH_MILLI, 60000);
    }

    @Override
    public int getSlotRefreshMilli() {
        return getIntProperty(KEY_SLOT_REFRESH_MILLI, 60000);
    }

    @Override
    public int getLeaderCheckMilli() {
        return getIntProperty(KEY_LEADER_CHECK_MILLI, 60000);
    }

    @Override
    public int getClusterServersRefreshMilli() {
        return getIntProperty(KEY_CLUSTER_SERVERS_CHECK_MILLI, 60000);
    }

    @Override
    public Map<String, RegionInfo> getCmRegionInfos() {
        if(cmRegionInfos.isEmpty()) {
            cmRegionInfos = getRegionInfoMapping(KEY_CM_REGION_INFOS);
        }
        return cmRegionInfos;
    }

    @Override
    public Map<String, RegionInfo> getConsoleRegionInfos() {
        if(consoleRegionInfos.isEmpty()) {
            consoleRegionInfos = getRegionInfoMapping(KEY_CONSOLE_REGION_INFOS);
        }
        return consoleRegionInfos;
    }

    @Override
    public Map<String, String> getMigrationIdc() {
        if(migrationIdcs.isEmpty()) {
            migrationIdcs = getMigrationIdcMapping(KEY_MIGRATION_IDC);
        }
        return migrationIdcs;
    }

    @Override
    public String getMigrationBlackIps() {
        return getProperty(KEY_MIGRATION_BLACK_IPS, StringUtils.EMPTY);
    }

    @Override
    public boolean getRealtimeMetaInfo() {
        return getBooleanProperty(KEY_REALTIME_META_INFO, false);
    }

    @Override
    public boolean checkApplierProperty() {
        return getBooleanProperty(KEY_CHECK_APPLIER_PROPERTY, false);
    }

    @Override
    public boolean refreshWhenConsoleInvoke() {
        return getBooleanProperty(KEY_REFRESH_WHEN_CONSOLE_INVOKE, false);
    }

    @Override
    public Pair<String, Integer> getApplierMaster(String key) {
        logger.debug("[getApplierMaster] for {}", key);
        String replicator = getProperty(key);
        if (StringUtils.isNotBlank(replicator)) {
            String[] ipAndPort = replicator.split(":");
            if (ipAndPort.length == 2) {
                return Pair.from(ipAndPort[0], Integer.parseInt(ipAndPort[1]));
            }
        }
        return null;
    }

    private Map<String, RegionInfo> getRegionInfoMapping(String key) {

        String regionInfoStr = getProperty(key, "{}");
        Map<String, RegionInfo> regionInfos = JsonCodec.INSTANCE.decode(regionInfoStr, new GenericTypeReference<Map<String, RegionInfo>>() {});

        Map<String, RegionInfo> result = Maps.newConcurrentMap();
        for(Map.Entry<String, RegionInfo> entry : regionInfos.entrySet()){
            result.put(entry.getKey().toLowerCase(), entry.getValue());
        }

        logger.debug("[getRegionInfos]{}", result);
        return result;
    }

    private Map<String, String> getMigrationIdcMapping(String key) {

        String migrationIdcStr = getProperty(key, "{}");
        Map<String, String> migrationIdcs = JsonCodec.INSTANCE.decode(migrationIdcStr, new GenericTypeReference<>() {});

        Map<String, String> result = Maps.newConcurrentMap();
        for(Map.Entry<String, String> entry : migrationIdcs.entrySet()){
            result.put(entry.getKey().toLowerCase(), entry.getValue());
        }

        logger.debug("[getMigrationIdcMapping]{}", result);
        return result;
    }

    @Override
    public int getWaitforOffsetMilli() {
        return getIntProperty(KEY_WAITFOR_OFFSET_MILLI, 2000);
    }

    @Override
    public boolean validateDomain() {
        return getBooleanProperty(KEY_VALIDATE_DOMAIN, true);
    }

    @Override
    public boolean getPeriodCheckSwitch() {
        return getBooleanProperty(KEY_PERIOD_META_CHECK, false);
    }

    @Override
    public boolean getPeriodCorrectSwitch() {
        return getBooleanProperty(KEY_PERIOD_META_CORRECT, false);
    }

    @Override
    public boolean requestWithHeraldToken() {
        return getBooleanProperty(KEY_HERALD_TOKEN_REQUEST_SWITCH, false);
    }

    @Override
    public int getCheckInterval() {
        return getIntProperty(KEY_INFO_CHECK_INTERVAL, 30 * 1000);
    }


    @Override
    public int getCheckMaxTime() {
        return getIntProperty(KEY_INFO_CHECK_TASK_MAX_TIME, 5 * 1000);
    }


    //from local config file
    @Override
    public String getClusterServerId() {
        return serverConfig.get(KEY_SERVER_ID, defaultClusterManagerId);
    }

    @Override
    public String getClusterServerIp() {
        return serverConfig.get(KEY_SERVER_IP, SystemConfig.LOCAL_SERVER_ADDRESS);
    }

    @Override
    public int getClusterServerPort() {
        return Integer.parseInt(serverConfig.get(KEY_SERVER_PORT, String.valueOf(defaultServerPort)));
    }

    public void setDefaultServerPort(int defaultServerPort) {
        this.defaultServerPort = defaultServerPort;
    }

    @Override
    public void onChange(String key, String oldValue, String newValue) {
        super.onChange(key, oldValue, newValue);
        cmRegionInfos = getRegionInfoMapping(KEY_CM_REGION_INFOS);
        consoleRegionInfos = getRegionInfoMapping(KEY_CONSOLE_REGION_INFOS);
    }

    @Override
    protected String getPath() {
        return ClusterZkConfig.getClusterManagerLeaderElectPath();
    }

    @Override
    public String getZkNameSpace() {
        return getZkNamespace();
    }
}
