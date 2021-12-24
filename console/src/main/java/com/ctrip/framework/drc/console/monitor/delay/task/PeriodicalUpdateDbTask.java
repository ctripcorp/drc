package com.ctrip.framework.drc.console.monitor.delay.task;

import com.ctrip.framework.drc.console.monitor.DefaultCurrentMetaManager;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.impl.execution.GeneralSingleExecution;
import com.ctrip.framework.drc.console.monitor.delay.impl.operator.WriteSqlOperatorWrapper;
import com.ctrip.framework.drc.console.pojo.MetaKey;
import com.ctrip.framework.drc.console.task.AbstractMasterMySQLEndpointObserver;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.server.observer.endpoint.MasterMySQLEndpointObserver;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.SWITCH_STATUS_ON;
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
@Component
public class PeriodicalUpdateDbTask extends AbstractMasterMySQLEndpointObserver implements MasterMySQLEndpointObserver {

    @Autowired
    private DbClusterSourceProvider sourceProvider;

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Autowired
    private DefaultCurrentMetaManager currentMetaManager;

    public static final int INITIAL_DELAY = 0;

    public static final int PERIOD = 1;

    public static final TimeUnit TIME_UNIT = TimeUnit.SECONDS;

    private static final int MAX_MAP_SIZE = 60;

    private static final String UPDATE_SQL = "UPDATE `drcmonitordb`.`delaymonitor` SET `datachange_lasttime`='%s' WHERE `dest_ip`='%s';";

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

    @Override
    public void scheduledTask() {
        String delayMonitorSwitch = monitorTableSourceProvider.getDelayMonitorUpdatedbSwitch();
        if(SWITCH_STATUS_ON.equalsIgnoreCase(delayMonitorSwitch)) {
            for (Map.Entry<MetaKey, MySqlEndpoint> entry : masterMySQLEndpointMap.entrySet()) {
                MetaKey metaKey = entry.getKey();
                Endpoint endpoint = entry.getValue();
                String registryKey = metaKey.getClusterId();
                WriteSqlOperatorWrapper sqlOperatorWrapper = getSqlOperatorWrapper(endpoint);
                long timestampInMillis = System.currentTimeMillis();
                Timestamp timestamp = new Timestamp(timestampInMillis);
                String sql = String.format(UPDATE_SQL, timestamp, metaKey.getMhaName());
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
        }
    }

    @Override
    public void setLocalDcName() {
        localDcName = sourceProvider.getLocalDcName();
    }

    @Override
    public void setOnlyCareLocal() {
        this.onlyCareLocal = true;
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
