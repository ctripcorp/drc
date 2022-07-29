package com.ctrip.framework.drc.console.monitor.delay.task;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
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
import com.ctrip.framework.drc.core.server.observer.endpoint.MasterMySQLEndpointObserver;
import com.ctrip.xpipe.api.cluster.LeaderAware;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
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
@Order(2)
@DependsOn("dbClusterSourceProvider")
@Component("periodicalUpdateDbTask")
public class PeriodicalUpdateDbTask extends AbstractMasterMySQLEndpointObserver implements MasterMySQLEndpointObserver , LeaderAware {

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

    private volatile boolean isRegionLeader = false;

    public static final int INITIAL_DELAY = 0;

    public static final int PERIOD = 1;

    public static final TimeUnit TIME_UNIT = TimeUnit.SECONDS;

    private static final int MAX_MAP_SIZE = 60;

    /**
     * due to legacy, src_ip means dc, dest_ip means mhaName
     */
    public static final String UPSERT_SQL = "INSERT INTO `drcmonitordb`.`delaymonitor`(`id`, `src_ip`, `dest_ip`) VALUES(%s, '%s', '%s') ON DUPLICATE KEY UPDATE datachange_lasttime = '%s';";
    private final Map<String, MhaInfo> mhaTblMap = Maps.newHashMap();

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
        refreshMhaTblMap();
        currentMetaManager.addObserver(this);
    }

    private void refreshMhaTblMap() {
        try {
            Set<String> localConfigCloudDc = consoleConfig.getLocalConfigCloudDc();
            String localDcName = sourceProvider.getLocalDcName();
            if (localConfigCloudDc.contains(localDcName)) {
                consoleConfig.getLocalConfigMhasMap().forEach(
                        (k, v) -> mhaTblMap.put(k, new MhaInfo(v, k, localDcName))
                );
            } else {
                for (String dc : dcsInRegion) {
                    refreshMhaTblByDc(dc);
                }
            }
        } catch (SQLException e) {
            logger.error("[[task=updateDelayTable]] sql error in refreshMhaTblMap",e);
        }
    }
    
    private void refreshMhaTblByDc(String dcName) throws SQLException {
        List<MhaTbl> mhasByDc = metaInfoService.getMhas(dcName);
        mhasByDc.forEach(
                mhaTbl -> mhaTblMap.put(
                        mhaTbl.getMhaName(),
                        new MhaInfo(mhaTbl.getId(),mhaTbl.getMhaName(),dcName)
                )
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
                    WriteSqlOperatorWrapper sqlOperatorWrapper = getSqlOperatorWrapper(endpoint);
                    MhaInfo mhaInfo = mhaTblMap.get(mhaName);
                    if (mhaInfo == null) {
                        refreshMhaTblMap();
                        mhaInfo = mhaTblMap.get(mhaName);
                        if (mhaInfo == null) {
                            logger.warn("[[monitor=delay]] can not get mhaInfo for mha:{}",mhaName);
                            continue;
                        }
                    }
                    long timestampInMillis = System.currentTimeMillis();
                    Timestamp timestamp = new Timestamp(timestampInMillis);
                    String sql = String.format(UPSERT_SQL,mhaInfo.getId(),mhaInfo.getDc(),mhaInfo.getMha(),timestamp);
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

    @Override
    public void isleader() {
        isRegionLeader = true;
    }

    @Override
    public void notLeader() {
        isRegionLeader = false;
    }
    
    private static class MhaInfo {
        private Long id;
        private String mha;
        private String dc;

        public MhaInfo(Long id, String mha,String dc) {
            this.id = id;
            this.mha = mha;
            this.dc = dc;
        }
        
        @Override
        public String toString() {
            return "MhaInfo{" +
                    "mha='" + mha + '\'' +
                    ", id=" + id +
                    ", dc='" + dc + '\'' +
                    '}';
        }
        
        public String getMha() {
            return mha;
        }

        public void setMha(String mha) {
            this.mha = mha;
        }

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getDc() {
            return dc;
        }

        public void setDc(String dc) {
            this.dc = dc;
        }
    }
}
