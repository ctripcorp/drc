package com.ctrip.framework.drc.console.monitor.delay.task;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.config.MhaGrayConfig;
import com.ctrip.framework.drc.console.dao.entity.MhaTbl;
import com.ctrip.framework.drc.console.monitor.DefaultCurrentMetaManager;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.impl.execution.GeneralSingleExecution;
import com.ctrip.framework.drc.console.monitor.delay.impl.operator.WriteSqlOperatorWrapper;
import com.ctrip.framework.drc.console.pojo.MetaKey;
import com.ctrip.framework.drc.console.service.impl.MetaInfoServiceImpl;
import com.ctrip.framework.drc.console.task.AbstractMasterMySQLEndpointObserver;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.monitor.column.DelayInfo;
import com.ctrip.framework.drc.core.server.observer.endpoint.MasterMySQLEndpointObserver;
import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;


import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONSOLE_DELAY_MONITOR_LOGGER;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.SLOW_COMMIT_THRESHOLD;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2019-12-13
 * STEP 2
 */
@Order(1)
@DependsOn("dbClusterSourceProvider")
@Component("periodicalUpdateDbTask")
public class PeriodicalUpdateDbTask extends AbstractMasterMySQLEndpointObserver implements MasterMySQLEndpointObserver {

    @Autowired
    private DbClusterSourceProvider sourceProvider;

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Autowired
    private DefaultCurrentMetaManager currentMetaManager;
    
    @Autowired
    private DefaultConsoleConfig consoleConfig;
    
    @Autowired
    private MetaInfoServiceImpl metaInfoService;
    
    @Autowired
    private MhaGrayConfig mhaGrayConfig;

    public static final int INITIAL_DELAY = 0;

    public static final int PERIOD = 1;

    public static final TimeUnit TIME_UNIT = TimeUnit.SECONDS;

    private static final int MAX_MAP_SIZE = 60;

    /**
     * due to legacy, src_ip means dc, dest_ip means mhaName
     */
    public static final String UPSERT_SQL = "INSERT INTO `drcmonitordb`.`delaymonitor`(`id`, `src_ip`, `dest_ip`) VALUES(%s, '%s', '%s') ON DUPLICATE KEY UPDATE src_ip = '%s',dest_ip = '%s',datachange_lasttime = '%s';";
    private final Map<String ,Long> mhaName2IdMap = Maps.newHashMap();

    /**
     * value: the time when update sql commits
     */
    private Map<DatachangeLastTime, Long> commitTimeMap = new LinkedHashMap<>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<DatachangeLastTime, Long> eldest) {
            return size() > MAX_MAP_SIZE;
        }
    };

    public Long getAndDeleteCommitTime(DatachangeLastTime datachangeLastTime) {
        return commitTimeMap.remove(datachangeLastTime);
    }

    @Override
    public void initialize() {
        super.initialize();
        currentMetaManager.addObserver(this);
    }

    private void refreshMhaTblMap() {
        try {
            Set<String> localConfigCloudDc = consoleConfig.getLocalConfigCloudDc();
            String localDcName = sourceProvider.getLocalDcName();
            if (localConfigCloudDc.contains(localDcName)) {
                mhaName2IdMap.putAll(consoleConfig.getLocalConfigMhasMap());
            } else {
                for (String dc : dcsInRegion) {
                    refreshMhaTblByDc(dc);
                }
            }
        } catch (Exception e) {
            logger.error("[[task=updateDelayTable]] error in refreshMhaTblMap",e);
        }
    }
    
    private void refreshMhaTblByDc(String dcName) throws Exception {
        List<MhaTbl> mhasByDc = metaInfoService.getMhas(dcName);
        mhasByDc.forEach(
                mhaTbl -> mhaName2IdMap.put(mhaTbl.getMhaName(),mhaTbl.getId())
        );
    }
    

    @Override
    public void scheduledTask() {
        if(isRegionLeader) {
            String delayMonitorSwitch = monitorTableSourceProvider.getDelayMonitorUpdatedbSwitch();
            if ("on".equalsIgnoreCase(delayMonitorSwitch)) {
                logger.info("[[monitor=delay]] going to update monitor table");
                for (Map.Entry<MetaKey, MySqlEndpoint> entry : masterMySQLEndpointMap.entrySet()) {
                    MetaKey metaKey = entry.getKey();
                    Endpoint endpoint = entry.getValue();
                    String registryKey = metaKey.getClusterId();
                    String mhaName = metaKey.getMhaName();
                    String dcName = metaKey.getDc();
                    String region = consoleConfig.getRegionForDc(dcName);
                    DelayInfo mhaDelayInfo = new DelayInfo(dcName, region,mhaName);
                    WriteSqlOperatorWrapper sqlOperatorWrapper = getSqlOperatorWrapper(endpoint);
                    Long mhaId = mhaName2IdMap.get(mhaName);
                    if (mhaId == null) {
                        refreshMhaTblMap();
                        mhaId = mhaName2IdMap.get(mhaName);
                        if (mhaId == null) {
                            logger.error("[[monitor=delay]] can not get mhaInfo for mha:{}",mhaName);
                            continue;
                        }
                    }
                    long timestampInMillis = System.currentTimeMillis();
                    Timestamp timestamp = new Timestamp(timestampInMillis);
                    String sql;
                    if (mhaGrayConfig.gray(mhaName)) {
                        sql = String.format(UPSERT_SQL,mhaId, dcName,Codec.DEFAULT.encode(mhaDelayInfo),dcName,Codec.DEFAULT.encode(mhaDelayInfo),timestamp);
                    } else {
                        sql = String.format(UPSERT_SQL,mhaId, dcName,mhaName,dcName,mhaName,timestamp);
                    }
                    GeneralSingleExecution execution = new GeneralSingleExecution(sql);
                    try {
                        CONSOLE_DELAY_MONITOR_LOGGER.info("[[monitor=delay,endpoint={},dc={},cluster={}]][Update DB] timestamp: {}", endpoint.getSocketAddress(), localDcName, registryKey, timestamp);
                        sqlOperatorWrapper.update(execution);
                        long commitTimeInMillis = System.currentTimeMillis();
                        boolean slowCommit = commitTimeInMillis - timestampInMillis > SLOW_COMMIT_THRESHOLD;
                        CONSOLE_DELAY_MONITOR_LOGGER.info("[[monitor=delay,endpoint={},dc={},cluster={},slow={}]][Update DB] timestamp: {}, commit time: {}", endpoint.getSocketAddress(), localDcName, registryKey, slowCommit, timestamp, new Timestamp(commitTimeInMillis));
                        if(slowCommit) {
                            DatachangeLastTime datachangeLastTime = new DatachangeLastTime(registryKey, timestamp.toString());
                            commitTimeMap.put(datachangeLastTime, commitTimeInMillis);
                            CONSOLE_DELAY_MONITOR_LOGGER.warn("[[monitor=delay,endpoint={},dc={},cluster={}]] Put commitTimeMap: {} -> {}", endpoint.getSocketAddress(), localDcName, registryKey, datachangeLastTime.toString(), commitTimeInMillis);
                        }
                    } catch (Throwable t) {
                        removeSqlOperator(endpoint);
                        CONSOLE_DELAY_MONITOR_LOGGER.warn("[[monitor=delay,endpoint={},dc={},cluster={}]] fail update db, ", endpoint.getSocketAddress(), localDcName, registryKey, t);
                    }
                }
            } else {
                logger.warn("[[monitor=delay]] is leader but switch is off");
            }
        } else {
            logger.info("[[monitor=delay]] not leader do nothing");
        }
    }

    @Override
    public void switchToLeader() throws Throwable {
        // do nothing
        
    }

    @Override
    public void switchToSlave() throws Throwable {
        mhaName2IdMap.clear();
    }

    @Override
    public void setLocalDcName() {
        localDcName = sourceProvider.getLocalDcName();
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
    
    public static final class DatachangeLastTime {

        private String registryKey;

        private String timestamp;

        public DatachangeLastTime(String registryKey, String timestamp) {
            this.registryKey = registryKey;
            this.timestamp = timestamp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof DatachangeLastTime)) return false;
            DatachangeLastTime wrapper = (DatachangeLastTime) o;
            return Objects.equals(registryKey, wrapper.registryKey) &&
                    Objects.equals(timestamp, wrapper.timestamp);
        }

        @Override
        public int hashCode() {
            return Objects.hash(registryKey, timestamp);
        }

        @Override
        public String toString() {
            return String.format("%s-%s", registryKey, timestamp);
        }
    }

    @Override
    public int getDefaultInitialDelay() {
        return INITIAL_DELAY;
    }

    @Override
    public int getDefaultPeriod(){
        return PERIOD;
    }

    @Override
    public TimeUnit getDefaultTimeUnit() {
        return TIME_UNIT;
    }
    
}
