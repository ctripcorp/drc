package com.ctrip.framework.drc.console.monitor.delay.config;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.codec.GenericTypeReference;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.config.AbstractConfigBean;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by mingdongli
 * 2019/12/19 下午3:28.
 */
@Component
public class MonitorTableSourceProvider extends AbstractConfigBean {

    private static final String DELIMITER = ",";

    private static final String EMPTY_STRING = "";

    private static final String DB_CLUSTER_MONITOR = "consistency.monitor.%s";

    private static final String DELAY_MONITOR_UPDATEDB_SWITCH = "delay.monitor.updatedb.switch";

    private static final String BTDHS_MONITOR_SWITCH = "btdhs.monitor.switch";

    private static final String UUID_MONITOR_SWITCH = "uuid.monitor.switch";
    private static final String UUID_CORRECT_SWITCH = "uuid.correct.switch";

    private static final String DATACHANGE_LAST_TIME_MONITOR_SWITCH = "datachange.last.time.monitor.switch";

    private static final String DRC_DELAY_MONITOR_SWITCH = "drc.delay.monitor.switch";

    public static final String MYSQL_DELAY_MONITOR_SWITCH = "mysql.delay.monitor.switch";

    private static final String DEFAULT_DELAY_MONITOR_SWITCH = "on";

    public static final String SWITCH_STATUS_ON = "on";

    public static final String SWITCH_STATUS_OFF = "off";

    private static final String GTID_MONITOR_SWITCH = "gtid.monitor.switch";
    private static final String GTID_MONITOR_PERIOD = "gtid.monitor.period";

    private static final int DEFAULT_GTID_MONITOR_PERIOD = 60 * 8;

    public static final String DRC_DELAY_MESUREMENT = "fx.drc.delay";

    public static final String MYSQL_DELAY_MESUREMENT = "fx.drc.delay.mysql";

    private static final String INCREMENT_ID_MONITOR_SWITCH = "increment.id.monitor.switch";

    private static final String TABLE_CONSISTENCY_MONITOR_SWITCH = "table.consistency.monitor.switch";
    private static final String TABLE_CONSISTENCY_MONITOR_PERIOD = "table.consistency.monitor.period";
    private static final int DEFAULT_TABLE_CONSISTENCY_MONITOR_PERIOD = 600;

    private static final String SWITCH_DRC_TASK_SYNC_MHA = "switch.drc.task.syncmha";
    private static final String SWITCH_SYNC_MHA_UPDATEALL = "switch.syncmha.updateall";
    private static final String DEFAULT_SWITCH_SYNC_MHA_UPDATEALL = "off";

    private static final String SWITCH_SALVE_MACHINE_OFFLINE_SYNC = "switch.slaveMachineOffline.sync";
    private static final String DEFAULT_SWITCH_SALVE_MACHINE_OFFLINE_SYNC = "off";

    private static final String SWITCH_DRC_TASK_SYNC_TABLE_CONFIG = "switch.drc.task.sync.tableconfig";

    private static final String UPDATE_CLUSTER_TABLE_SWITCH = "update.cluster.tbl.switch";

    private static final String DEFAULT_UPDATE_CLUSTER_TABLE_SWITCH = "on";

    private static final String MYSQL_MONITOR_SWITCH = "mysql.monitor.switch";

    private static final String DEFAULT_MYSQL_MONITOR_SWITCH = "on";

    private static final String BEACON_REGISTER_SWITCH = "beacon.register.switch";

    private static final String BEACON_REGISTER_MYSQL_SWITCH = "beacon.register.mysql.switch";

    private static final String BEACON_REGISTER_DELAY_SWITCH = "beacon.register.delay.switch";

    private static final String DEFAULT_BEACON_REGISTER_SWITCH = "off";

    private static final String MHA_DALCLUSTER_INFO_SWITCH = "mha.dalcluster.info.switch";

    public static final String DEFAULT_MHA_DALCLUSTER_INFO_SWITCH = "qconfig";

    private static final String UPDATE_CONSISTENCY_META_SWITCH = "update.consistency.meta.switch";

    public static final String DRC_DB_CLUSTER = "drc.dbclusters.meta";

    public static final String DEFAULT_DRC_DB_CLUSTER = "";

    public static final String BEACON_FILTER_OUT_MHA_MYSQL = "beacon.filter.out.mha.mysql";

    public static final String BEACON_FILTER_OUT_MHA_DELAY = "beacon.filter.out.mha.delay";

    public static final String BEACON_FILTER_OUT_CLUSTER = "beacon.filter.out.cluster";

    public static final String MONITOR_FILTER_OUT_MHA_MULTI_SIDE = "filter.out.monitor.multi.side";

    public static final String DEFAULT_FILTER_OUT = "";

    public static final String ALLOW_FAILOVER_SWITCH = "allow.failover.switch";

    public static final String SWITCH_CACHE_ALL_CLUSTER_NAME = "switch.cache.all.cluster.name";

    public static final String ADMIN_USER = "admin.user";

    public static final String DEFAULT_ADMIN_USER = "";

    public static final String DAL_SERVICE_TIMEOUT = "dal.service.timeout";

    public static final String DEFAULT_DAL_SERVICE_TIMEOUT = "3000";

    public static final String CONFLICT_LOG_CLEAR_SWITCH = "conflict.log.clear.switch";

    public static final String DEFAULT_CONFLICT_LOG_CLEAR_SWITCH = "off";

    public static final String CONFLICT_LOG_RESERVE_DAY = "conflict.log.reserve.day";

    public static final String DEFAULT_CONFLICT_LOG_RESERVE_DAY = "30";

    public static final String CONFLICT_BLACK_LIST = "conflict.black.list";

    public static final String DEFAULT_CONFLICT_BLACK_LIST = "";

    public static final String DATA_CONSISTENT_MONITOR_SWITCH = "data.consistent.monitor.switch";

    public static final String GENERAL_DATA_CONSISTENT_MONITOR_SWITCH = "general.data.consistent.monitor.switch";

    public static final String TRUNCATE_CONSISTENT_MONITOR_SWITCH = "truncate.consistent.monitor.switch";

    public static final String CLEAN_TABLE_DEVIATION = "clean.table.deviation";

    public static final int DEFAULT_CLEAN_TABLE_DEVIATION = 24;

    public static final String READ_USER_KEY = "drc.readuser";
    public static final String READ_PASSWORD_KEY = "drc.readpassword";
    public static final String WRITE_USER_KEY = "drc.writeuser";
    public static final String WRITE_PASSWORD_KEY = "drc.writepassword";
    public static final String MONITOR_USER_KEY = "drc.monitoruser";
    public static final String MONITOR_PASSWORD_KEY = "drc.monitorpassword";

    public static final String DATA_CONSISTENCY_CHECK_MAX_PAGE = "data.consistency.check.max.page";
    public static final int DEFAULT_DATA_CONSISTENCY_CHECK_MAX_PAGE = 24;

    public static final String DATA_CONSISTENCY_CHECK_BEGIN_TIME_OFFSET_SECOND = "data.consistency.check.begin.time.offset.second";
    public static final int DEFAULT_DATA_CONSISTENCY_CHECK_BEGIN_TIME_OFFSET_SECOND = 60;

    public static final String DATA_CONSISTENCY_CHECK_END_TIME_OFFSET_SECOND = "data.consistency.check.end.time.offset.second";
    public static final int DEFAULT_DATA_CONSISTENCY_CHECK_END_TIME_OFFSET_SECOND = 90;

    public static final String DATA_CONSISTENCY_CHECK_TIME_INTERVAL_SECOND = "data.consistency.check.time.interval.second";
    public static final int DEFAULT_DATA_CONSISTENCY_CHECK_TIME_INTERVAL_SECOND = 30;

    public static final String LISTEN_REPLICATOR_SWITCH = "listen.replicator.switch";
    public static final String LISTEN_REPLICATOR_MONITOR_SWITCH = "listen.replicator.monitor.switch";

    public static final String UNIT_VERIFICATION_MANAGER_SWITCH = "unit.verification.manager.switch";
    public static final String UID_SOURCE = "uid.source";
    public static final String UCS_STRATEGY_SOURCE = "ucs.strategy.source";
    public static final String SOURCE_QCONFIG = "qconfig";
    public static final String UNIT_RESULT_HICKWALL = "unit.result.hickwall";

    private static final String INIT_DELAY_MONITOR_RECORD_SWITCH = "init.delay.monitor.record.switch";

    private static final String DELETE_REDUNDANT_DELAY_MONITOR_RECORD_SWITCH = "delete.redundant.delay.monitor.record.switch";

    private static final String UPDATE_MONITOR_META_INFO_SWITCH = "update.monitor.meta.info.switch";

    private static final String DIFF_COUNT_LIMIT = "fullDataCheck.diffCount.limit";
    private static int DEFAULT_DIFF_COUNT_LIMIT = 100;

    private static final String APPLY_MODE_MIGRATE_SWITCH = "apply.mode.migrate.switch";
    private static final String DRC_META_XML_UPDATE_SWITCH = "drc.meta.xml.update.switch";

    private static final String SYNC_DB_INFO_SWITCH = "sync.db.info.switch";
    // allow update and delete the change
    private static final String UPDATE_DB_INFO_SWITCH = "update.db.info.switch";

    private static final String SEND_TRAFFIC_SWITCH = "send.traffic.switch";

    private static final String RELATION_COST_APPS = "relation.cost.apps";

    public String getDrcMetaXmlUpdateSwitch() {
        return getProperty(DRC_META_XML_UPDATE_SWITCH, SWITCH_STATUS_ON);
    }


    private static class ConfigServiceHolder {
        private static MonitorTableSourceProvider instance = new MonitorTableSourceProvider();
    }

    public static MonitorTableSourceProvider getInstance() {
        return ConfigServiceHolder.instance;
    }

    public String getReadUserVal() {
        return getProperty(READ_USER_KEY);
    }

    public String getReadPasswordVal() {
        return getProperty(READ_PASSWORD_KEY);
    }

    public String getWriteUserVal() {
        return getProperty(WRITE_USER_KEY);
    }

    public String getWritePasswordVal() {
        return getProperty(WRITE_PASSWORD_KEY);
    }

    public String getMonitorUserVal() {
        return getProperty(MONITOR_USER_KEY);
    }

    public String getMonitorPasswordVal() {
        return getProperty(MONITOR_PASSWORD_KEY);
    }

    /**
     * get the whole drc info
     */
    public DelayMonitorConfig getMonitorConfig(String clusterId) {
        String key = String.format(DB_CLUSTER_MONITOR, clusterId);
        String monitorConfigString = getProperty(key);
        try {
            DelayMonitorConfig monitorConfig = Codec.DEFAULT.decode(monitorConfigString, DelayMonitorConfig.class);
            return monitorConfig;
        } catch (Exception e) {
            logger.info("[Decode] for {} error", clusterId, e);
        }
        return null;
    }

    public String getDelayMonitorUpdatedbSwitch() {
        return getProperty(DELAY_MONITOR_UPDATEDB_SWITCH, SWITCH_STATUS_ON);
    }

    public String getBtdhsMonitorSwitch() {
        return getProperty(BTDHS_MONITOR_SWITCH, SWITCH_STATUS_ON);
    }

    public String getUuidMonitorSwitch() {
        return getProperty(UUID_MONITOR_SWITCH, SWITCH_STATUS_OFF);
    }

    public String getUuidCorrectSwitch() {
        return getProperty(UUID_CORRECT_SWITCH, SWITCH_STATUS_OFF);
    }

    public String getDatachangeLastTimeMonitorSwitch() {
        return getProperty(DATACHANGE_LAST_TIME_MONITOR_SWITCH, SWITCH_STATUS_ON);
    }

    public String getDelayMonitorSwitch(String measurement) {
        if (measurement.equalsIgnoreCase(DRC_DELAY_MESUREMENT)) {
            return getProperty(DRC_DELAY_MONITOR_SWITCH, DEFAULT_DELAY_MONITOR_SWITCH);
        } else if (measurement.equalsIgnoreCase(MYSQL_DELAY_MESUREMENT)) {
            return getProperty(MYSQL_DELAY_MONITOR_SWITCH, DEFAULT_DELAY_MONITOR_SWITCH);
        }
        return DEFAULT_DELAY_MONITOR_SWITCH;
    }

    Set<String> string2Set(String s) {
        if (StringUtils.isNotBlank(s)) {
            return Sets.newHashSet(s.split(DELIMITER)).stream()
                    .filter(StringUtils::isNotBlank).map(String::trim)
                    .collect(Collectors.toSet());
        }
        return Sets.newHashSet();
    }

    public String getGtidMonitorSwitch() {
        return getProperty(GTID_MONITOR_SWITCH, SWITCH_STATUS_ON);
    }

    public int getGtidMonitorPeriod() {
        return getIntProperty(GTID_MONITOR_PERIOD, DEFAULT_GTID_MONITOR_PERIOD);
    }

    public String getIncrementIdMonitorSwitch() {
        return getProperty(INCREMENT_ID_MONITOR_SWITCH, SWITCH_STATUS_OFF);
    }

    public String getTableConsistencySwitch() {
        return getProperty(TABLE_CONSISTENCY_MONITOR_SWITCH, SWITCH_STATUS_OFF);
    }

    public int getTableConsistencyMonitorPeriod() {
        return getIntProperty(TABLE_CONSISTENCY_MONITOR_PERIOD, DEFAULT_TABLE_CONSISTENCY_MONITOR_PERIOD);
    }

    public String getSyncMhaSwitch() {
        return getProperty(SWITCH_DRC_TASK_SYNC_MHA, SWITCH_STATUS_OFF);
    }

    public String getSwitchSyncMhaUpdateAll() {
        return getProperty(SWITCH_SYNC_MHA_UPDATEALL, DEFAULT_SWITCH_SYNC_MHA_UPDATEALL);
    }

    public String getSyncTableConfigSwitch() {
        return getProperty(SWITCH_DRC_TASK_SYNC_TABLE_CONFIG, SWITCH_STATUS_OFF);
    }

    public String getUpdateClusterTblSwitch() {
        return getProperty(UPDATE_CLUSTER_TABLE_SWITCH, DEFAULT_UPDATE_CLUSTER_TABLE_SWITCH);
    }

    public String getMySqlMonitorSwitch() {
        return getProperty(MYSQL_MONITOR_SWITCH, DEFAULT_MYSQL_MONITOR_SWITCH);
    }

    public String getBeaconRegisterSwitch() {
        return getProperty(BEACON_REGISTER_SWITCH, SWITCH_STATUS_OFF);
    }

    public String getBeaconRegisterMySqlSwitch() {
        return getProperty(BEACON_REGISTER_MYSQL_SWITCH, DEFAULT_BEACON_REGISTER_SWITCH);
    }

    public String getBeaconRegisterDelaySwitch() {
        return getProperty(BEACON_REGISTER_DELAY_SWITCH, DEFAULT_BEACON_REGISTER_SWITCH);
    }

    public String getMhaDalclusterInfoSwitch() {
        return getProperty(MHA_DALCLUSTER_INFO_SWITCH, DEFAULT_MHA_DALCLUSTER_INFO_SWITCH);
    }

    public String getMetaData() {
        return getProperty(DRC_DB_CLUSTER, DEFAULT_DRC_DB_CLUSTER);
    }

    public String[] getBeaconFilterOutMhaForMysql() {
        String mhaString = getProperty(BEACON_FILTER_OUT_MHA_MYSQL, DEFAULT_FILTER_OUT);
        if (mhaString.isEmpty()) {
            return ArrayUtils.EMPTY_STRING_ARRAY;
        }
        return mhaString.split(",");
    }

    public String[] getBeaconFilterOutMhaForDelay() {
        String mhaString = getProperty(BEACON_FILTER_OUT_MHA_DELAY, DEFAULT_FILTER_OUT);
        if (mhaString.isEmpty()) {
            return ArrayUtils.EMPTY_STRING_ARRAY;
        }
        return mhaString.split(",");
    }

    public String[] getBeaconFilterOutCluster() {
        String clusterString = getProperty(BEACON_FILTER_OUT_CLUSTER, DEFAULT_FILTER_OUT);
        if (clusterString.isEmpty()) {
            return ArrayUtils.EMPTY_STRING_ARRAY;
        }
        return clusterString.split(",");
    }

    public String[] getFilterOutMhasForMultiSideMonitor() {
        String mhaString = getProperty(MONITOR_FILTER_OUT_MHA_MULTI_SIDE, DEFAULT_FILTER_OUT);
        if (mhaString.isEmpty()) {
            return ArrayUtils.EMPTY_STRING_ARRAY;
        }
        return mhaString.split(",");
    }

    public String getAllowFailoverSwitch() {
        return getProperty(ALLOW_FAILOVER_SWITCH, SWITCH_STATUS_OFF);
    }

    public Set<String> getAdminUser() {
        return string2Set(getProperty(ADMIN_USER, DEFAULT_ADMIN_USER));
    }

    public int getDalServiceTimeout() {
        return Integer.parseInt(getProperty(DAL_SERVICE_TIMEOUT, DEFAULT_DAL_SERVICE_TIMEOUT));
    }

    public String getConflictLogClearSwitch() {
        return getProperty(CONFLICT_LOG_CLEAR_SWITCH, DEFAULT_CONFLICT_LOG_CLEAR_SWITCH);
    }

    public int getConflictLogReserveDay() {
        return Integer.parseInt(getProperty(CONFLICT_LOG_RESERVE_DAY, DEFAULT_CONFLICT_LOG_RESERVE_DAY));
    }

    public Set<String> getConflictBlackList() {
        return string2Set(getProperty(CONFLICT_BLACK_LIST, DEFAULT_CONFLICT_BLACK_LIST));
    }

    public String getDataConsistentMonitorSwitch() {
        return getProperty(DATA_CONSISTENT_MONITOR_SWITCH, SWITCH_STATUS_OFF);
    }

    public String getGeneralDataConsistentMonitorSwitch() {
        return getProperty(GENERAL_DATA_CONSISTENT_MONITOR_SWITCH, SWITCH_STATUS_ON);
    }

    public String getTruncateConsistentMonitorSwitch() {
        return getProperty(TRUNCATE_CONSISTENT_MONITOR_SWITCH, SWITCH_STATUS_OFF);
    }

    public String getUpdateConsistencyMetaSwitch() {
        return getProperty(UPDATE_CONSISTENCY_META_SWITCH, SWITCH_STATUS_OFF);
    }

    public int getCleanTableDeviation() {
        String deviationStr = getProperty(CLEAN_TABLE_DEVIATION);
        try {
            return Integer.parseInt(deviationStr);
        } catch (NumberFormatException e) {
            logger.error("error parse {}, ", deviationStr, e);
        }
        return DEFAULT_CLEAN_TABLE_DEVIATION;
    }

    public int getDataConsistencyCheckMaxPage() {
        return getIntProperty(DATA_CONSISTENCY_CHECK_MAX_PAGE, DEFAULT_DATA_CONSISTENCY_CHECK_MAX_PAGE);
    }

    public int getDataConsistencyCheckBeginTimeOffsetSecond() {
        return getIntProperty(DATA_CONSISTENCY_CHECK_BEGIN_TIME_OFFSET_SECOND, DEFAULT_DATA_CONSISTENCY_CHECK_BEGIN_TIME_OFFSET_SECOND);
    }

    public int getDataConsistencyCheckEndTimeOffsetSecond() {
        return getIntProperty(DATA_CONSISTENCY_CHECK_END_TIME_OFFSET_SECOND, DEFAULT_DATA_CONSISTENCY_CHECK_END_TIME_OFFSET_SECOND);
    }

    public int getDataConsistencyCheckTimeIntervalSecond() {
        return getIntProperty(DATA_CONSISTENCY_CHECK_TIME_INTERVAL_SECOND, DEFAULT_DATA_CONSISTENCY_CHECK_TIME_INTERVAL_SECOND);
    }

    public String getListenReplicatorSwitch() {
        return getProperty(LISTEN_REPLICATOR_SWITCH, SWITCH_STATUS_ON);
    }

    public String getListenReplicatorMonitorSwitch() {
        return getProperty(LISTEN_REPLICATOR_MONITOR_SWITCH, SWITCH_STATUS_ON);
    }

    public String getUnitVericationManagerSwitch() {
        return getProperty(UNIT_VERIFICATION_MANAGER_SWITCH, SWITCH_STATUS_OFF);
    }

    public String getUidMapSource() {
        return getProperty(UID_SOURCE, SOURCE_QCONFIG);
    }

    public String getUcsStrategyIdMapSource() {
        return getProperty(UCS_STRATEGY_SOURCE, SOURCE_QCONFIG);
    }

    public String getUnitResultHickwallAddress() {
        return getProperty(UNIT_RESULT_HICKWALL);
    }

    public String getCacheAllClusterNamesSwitch() {
        return getProperty(SWITCH_CACHE_ALL_CLUSTER_NAME, SWITCH_STATUS_OFF);
    }

    public String getInitDelayMonitorRecordSwitch() {
        return getProperty(INIT_DELAY_MONITOR_RECORD_SWITCH, SWITCH_STATUS_OFF);
    }

    public String getDeleteRedundantDelayMonitorRecordSwitch() {
        return getProperty(DELETE_REDUNDANT_DELAY_MONITOR_RECORD_SWITCH, SWITCH_STATUS_OFF);
    }

    public String getUpdateMonitorMetaInfoSwitch() {
        return getProperty(UPDATE_MONITOR_META_INFO_SWITCH, SWITCH_STATUS_ON);
    }

    public String getSlaveMachineOfflineSyncSwitch() {
        return getProperty(SWITCH_SALVE_MACHINE_OFFLINE_SYNC, DEFAULT_SWITCH_SALVE_MACHINE_OFFLINE_SYNC);
    }

    public int getDiffCountLimit() {
        return getIntProperty(DIFF_COUNT_LIMIT, DEFAULT_DIFF_COUNT_LIMIT);
    }

    public String getApplyModeMigrateSwitch() {
        return getProperty(APPLY_MODE_MIGRATE_SWITCH, SWITCH_STATUS_OFF);
    }

    public String getSyncDbInfoSwitch() {
        return getProperty(SYNC_DB_INFO_SWITCH, SWITCH_STATUS_OFF);
    }

    public String getUpdateDbInfoSwitch() {
        return getProperty(UPDATE_DB_INFO_SWITCH, SWITCH_STATUS_OFF);

    }

    public String getSendTrafficSwitch() {
        return getProperty(SEND_TRAFFIC_SWITCH, SWITCH_STATUS_OFF);

    }

    public Map<String, Set<String>> getRelationCostApps() {
        String relationCostInfo = getProperty(RELATION_COST_APPS, EMPTY_STRING);
        if (StringUtils.isEmpty(relationCostInfo)) {
            return Maps.newHashMap();
        } else {
            return JsonCodec.INSTANCE.decode(relationCostInfo, new GenericTypeReference<Map<String, Set<String>>>() {
            });
        }
    }
}
