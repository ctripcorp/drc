package com.ctrip.framework.drc.console.monitor.delay.task;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dto.v3.MhaDbReplicationDto;
import com.ctrip.framework.drc.console.enums.ReplicationTypeEnum;
import com.ctrip.framework.drc.console.monitor.DefaultCurrentMetaManager;
import com.ctrip.framework.drc.console.monitor.delay.config.DataCenterService;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.impl.execution.GeneralSingleExecution;
import com.ctrip.framework.drc.console.monitor.delay.impl.operator.WriteSqlOperatorWrapper;
import com.ctrip.framework.drc.console.pojo.MetaKey;
import com.ctrip.framework.drc.console.service.v2.ForwardService;
import com.ctrip.framework.drc.console.task.AbstractMasterMySQLEndpointObserver;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.monitor.column.DbDelayDto;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.core.server.observer.endpoint.MasterMySQLEndpointObserver;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.*;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2019-12-13
 * STEP 2
 */
@Order(1)
@DependsOn("metaProviderV2")
@Component("periodicalUpdateDbTaskV2")
public class PeriodicalUpdateDbTaskV2 extends AbstractMasterMySQLEndpointObserver implements MasterMySQLEndpointObserver {
    private final Logger logger = LoggerFactory.getLogger(CONSOLE_DB_DELAY_MONITOR_LOG);
    @Autowired
    private DataCenterService dataCenterService;
    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;
    @Autowired
    private DefaultCurrentMetaManager currentMetaManager;
    @Autowired
    private DefaultConsoleConfig consoleConfig;
    @Autowired
    private ForwardService forwardService;
    @Autowired
    private DefaultConsoleConfig defaultConsoleConfig;


    public static final int INITIAL_DELAY = 0;

    public static final int PERIOD = 1;

    public static final TimeUnit TIME_UNIT = TimeUnit.SECONDS;

    private static final int MAX_MAP_SIZE = 600;

    private final ExecutorService updateExecutor = ThreadUtils.newFixedThreadPool(20, "updateTaskV2");

    public static final String UPSERT_DB_SQL = "INSERT INTO `drcmonitordb`.`" + DRC_DB_DELAY_MONITOR_TABLE_NAME_PREFIX + "%s`(`id`, `delay_info`, `datachange_lasttime`) VALUES (%s, '%s', '%s') " +
            "ON DUPLICATE KEY UPDATE delay_info = '%s', datachange_lasttime = '%s';";

    // src mha name -> mha db replications
    private final Supplier<Map<String, List<MhaDbReplicationDto>>> mhaDbMapCache = Suppliers.memoizeWithExpiration(this::refreshAndGetMhaDbMap, 10, TimeUnit.SECONDS);
    private Map<String, List<MhaDbReplicationDto>> mhaDbMapBackUp = Maps.newHashMap();

    /**
     * value: the time when update sql commits
     */
    private final Map<DatachangeLastTime, Long> commitTimeMap = new LinkedHashMap<>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<DatachangeLastTime, Long> eldest) {
            return size() > MAX_MAP_SIZE;
        }
    };

    @Override
    public void initialize() {
        super.initialize();
        currentMetaManager.addObserver(this);
    }

    private Map<String, List<MhaDbReplicationDto>> refreshAndGetMhaDbMap() {
        try {
            if (consoleConfig.getLocalConfigCloudDc().contains(dataCenterService.getDc())) {
                logger.warn("[[task=updateDelayTable_v2]] skip update localDcName");
                return Maps.newHashMap();
            }
            Map<String, List<MhaDbReplicationDto>> mhaDbReplication = Maps.newHashMap();
            for (String dc : dcsInRegion) {
                mhaDbReplication.putAll(getMhaDbReplicationByDc(dc));
            }
            // update backup
            mhaDbMapBackUp = mhaDbReplication;
        } catch (Exception e) {
            logger.error("[[task=updateDelayTable_v2]] error in refreshMhaTblMap", e);
        }
        return Collections.unmodifiableMap(mhaDbMapBackUp);
    }

    private Map<String, List<MhaDbReplicationDto>> getMhaDbReplicationByDc(String dcName) throws Exception {
        List<MhaDbReplicationDto> mhaDbMappingTblV2s = forwardService.getMhaDbReplications(dcName);
        if (CollectionUtils.isEmpty(mhaDbMappingTblV2s)) {
            return Maps.newHashMap();
        }
        return mhaDbMappingTblV2s.stream()
                .filter(this::filterGrey)
                .collect(Collectors.groupingBy(e -> e.getSrc().getMhaName()));
    }

    @Override
    public void scheduledTask() {
        if (isRegionLeader) {
            DefaultTransactionMonitorHolder.getInstance().logTransactionSwallowException("DRC.console.delay.update", "task", this::updateDbDelay);
        } else {
            logger.info("[[monitor=delay_v2]] not leader do nothing");
        }
    }


    private void updateDbDelay() {
        if (!monitorTableSourceProvider.getDelayMonitorUpdateDbV2Switch()) {
            logger.info("[[monitor=delay_v2]] is leader but switch is off, do nothing");
            return;
        }

        logger.info("[[monitor=delay_v2]] start updateDbDelay");
        Set<Map.Entry<MetaKey, MySqlEndpoint>> entries = masterMySQLEndpointMap.entrySet();
        List<Future<?>> list = Lists.newArrayList();
        Map<String, List<MhaDbReplicationDto>> mhaDbReplicationMap = mhaDbMapCache.get();
        for (Map.Entry<MetaKey, MySqlEndpoint> endPointEntry : entries) {
            MetaKey metaKey = endPointEntry.getKey();
            Endpoint endpoint = endPointEntry.getValue();
            String registryKey = metaKey.getClusterId();
            String mhaName = metaKey.getMhaName();
            String dcName = metaKey.getDc();
            String region = consoleConfig.getRegionForDc(dcName);
            List<MhaDbReplicationDto> mhaDbReplicationDtos = mhaDbReplicationMap.getOrDefault(mhaName, Collections.emptyList());
            if (CollectionUtils.isEmpty(mhaDbReplicationDtos)) {
                logger.info("[[monitor=delay_v2]] get mhaDbInfo empty for mha:{}", mhaName);
                continue;
            }
            List<String> dbNames = mhaDbReplicationDtos.stream().map(e -> e.getSrc().getDbName()).collect(Collectors.toList());
            logger.info("[[monitor=delay_v2]] going to update for mha:{}, dbs:{}", mhaName, dbNames);
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.console.delay.update", mhaName);
            WriteSqlOperatorWrapper sqlOperatorWrapper = getSqlOperatorWrapper(endpoint);
            for (MhaDbReplicationDto e : mhaDbReplicationDtos) {
                list.add(updateExecutor.submit(() -> {
                    String dbName = e.getSrc().getDbName().toLowerCase();
                    Long mappingId = e.getId();
                    long timestampInMillis = System.currentTimeMillis();
                    Timestamp timestamp = new Timestamp(timestampInMillis);
                    String delayInfoJson = DbDelayDto.DelayInfo.from(dcName, region, mhaName, dbName).toJson();
                    String sql = String.format(UPSERT_DB_SQL, dbName, mappingId, delayInfoJson, timestamp, delayInfoJson, timestamp);
                    GeneralSingleExecution execution = new GeneralSingleExecution(sql);
                    try {
                        CONSOLE_DB_DELAY_MONITOR_LOGGER.info("[[monitor=delay_v2,endpoint={},dc={},cluster={},db={}]][Update DB] timestamp: {}", endpoint.getSocketAddress(), localDcName, registryKey, dbName, timestamp);
                        sqlOperatorWrapper.update(execution);
                        long commitTimeInMillis = System.currentTimeMillis();
                        boolean slowCommit = commitTimeInMillis - timestampInMillis > SLOW_COMMIT_THRESHOLD;
                        CONSOLE_DB_DELAY_MONITOR_LOGGER.info("[[monitor=delay_v2,endpoint={},dc={},cluster={},db={},slow={}]][Update DB] timestamp: {}, commit time: {}", endpoint.getSocketAddress(), localDcName, registryKey, dbName, slowCommit, timestamp, new Timestamp(commitTimeInMillis));
                        if (slowCommit) {
                            DatachangeLastTime datachangeLastTime = new DatachangeLastTime(registryKey, dbName, timestamp.toString());
                            commitTimeMap.put(datachangeLastTime, commitTimeInMillis);
                            CONSOLE_DB_DELAY_MONITOR_LOGGER.warn("[[monitor=delay_v2,endpoint={},dc={},cluster={},db={}]] Put commitTimeMap: {} -> {}", endpoint.getSocketAddress(), localDcName, registryKey, dbName, datachangeLastTime.toString(), commitTimeInMillis);
                        }
                    } catch (Throwable t) {
                        removeSqlOperator(endpoint);
                        DefaultEventMonitorHolder.getInstance().logEvent("DRC.console.delay.update.exception", mhaName + "." + dbName + ":" + endpoint.getHost() + ":" + endpoint.getPort());
                        CONSOLE_DB_DELAY_MONITOR_LOGGER.warn("[[monitor=delay_v2,endpoint={},dc={},cluster={},db={}]] fail update db, ", endpoint.getSocketAddress(), localDcName, registryKey, dbName, t);
                    }
                }));
            }
        }
        try {
            for (Future<?> future : list) {
                future.get(defaultConsoleConfig.getDelayExceptionTimeInMilliseconds(), TimeUnit.MILLISECONDS);
                DefaultEventMonitorHolder.getInstance().logAlertEvent("DRC.console.delay.update.timeout");
            }
        } catch (Exception e) {
            logger.error("[[monitor=delay_v2]] wait timeout for task", e);
        }
    }

    @Override
    public void switchToLeader() {
        // do nothing
    }

    @Override
    public void switchToSlave() throws Throwable {
        mhaDbMapBackUp.clear();
    }

    @Override
    public void setLocalDcName() {
        localDcName = dataCenterService.getDc();
    }

    @Override
    public void setLocalRegionInfo() {
        this.regionName = consoleConfig.getRegion();
        this.dcsInRegion = consoleConfig.getDcsInLocalRegion();
    }

    @Override
    public void setOnlyCarePart() {
        this.onlyCarePart = true;
    }

    @Override
    public boolean isCare(MetaKey metaKey) {
        return this.dcsInRegion.contains(metaKey.getDc());
    }

    @Override
    public void clearOldEndpointResource(Endpoint endpoint) {
        removeSqlOperator(endpoint);
    }

    public Long getAndDeleteCommitTime(DatachangeLastTime datachangeLastTime) {
        return commitTimeMap.remove(datachangeLastTime);
    }

    private boolean filterGrey(MhaDbReplicationDto e) {
        String mhaName;
        if (e.getReplicationType().equals(ReplicationTypeEnum.DB_TO_MQ.getType())) {
            mhaName = e.getSrc().getMhaName();
        } else {
            mhaName = e.getDst().getMhaName();
        }
        return consoleConfig.getDbApplierConfigureSwitch(mhaName);
    }

    public static final class DatachangeLastTime {

        private final String registryKey;
        private final String dbName;
        private final String timestamp;

        public DatachangeLastTime(String registryKey, String dbName, String timestamp) {
            this.registryKey = registryKey;
            this.dbName = dbName;
            this.timestamp = timestamp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof DatachangeLastTime)) return false;
            DatachangeLastTime that = (DatachangeLastTime) o;
            return Objects.equals(registryKey, that.registryKey) && Objects.equals(dbName, that.dbName) && Objects.equals(timestamp, that.timestamp);
        }

        @Override
        public int hashCode() {
            return Objects.hash(registryKey, dbName, timestamp);
        }

        @Override
        public String toString() {
            return String.format("%s-%s-%s", registryKey, dbName, timestamp);
        }
    }

    @Override
    public int getDefaultInitialDelay() {
        return INITIAL_DELAY;
    }

    @Override
    public int getDefaultPeriod() {
        return PERIOD;
    }

    @Override
    public TimeUnit getDefaultTimeUnit() {
        return TIME_UNIT;
    }

}
