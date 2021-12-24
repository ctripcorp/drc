package com.ctrip.framework.drc.console.monitor.cases.function;

import com.ctrip.framework.drc.console.enums.ActionEnum;
import com.ctrip.framework.drc.console.monitor.DefaultCurrentMetaManager;
import com.ctrip.framework.drc.console.monitor.delay.DelayMap;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.impl.execution.GeneralSingleExecution;
import com.ctrip.framework.drc.console.monitor.delay.impl.operator.WriteSqlOperatorWrapper;
import com.ctrip.framework.drc.console.pojo.MetaKey;
import com.ctrip.framework.drc.console.service.impl.HealthServiceImpl;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.monitor.entity.UnidirectionalEntity;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.core.monitor.reporter.Reporter;
import com.ctrip.framework.drc.core.server.observer.endpoint.MasterMySQLEndpointObservable;
import com.ctrip.framework.drc.core.server.observer.endpoint.MasterMySQLEndpointObserver;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.unidal.tuple.Triple;

import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.SWITCH_STATUS_ON;
import static com.ctrip.framework.drc.console.service.impl.HealthServiceImpl.NORMAL_DELAY_THRESHOLD;
import static com.ctrip.framework.drc.core.monitor.enums.MeasurementEnum.DELAY_V2_MEASUREMENT;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONSOLE_BEACON_HEALTH_LOGGER;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-09-07
 */
@Component
public class DatachangeLastTimeMonitorCase extends AbstractMonitorCase implements MonitorCase, MasterMySQLEndpointObserver {

    protected DelayMap delayMap = DelayMap.getInstance();

    private Reporter reporter = DefaultReporterHolder.getInstance();

    private static ThreadLocal<SimpleDateFormat> dateFormatThreadLocal = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"));

    private static final String SELECT_DATACHANGE_LASTTIME = "SELECT `datachange_lasttime` FROM `drcmonitordb`.`delaymonitor` WHERE `src_ip`='%s';";

    private static final int DATACHANGE_LASTTIME_INDEX = 1;

    @Autowired
    private DbClusterSourceProvider sourceProvider;

    @Autowired
    private HealthServiceImpl healthService;

    @Autowired
    private DefaultCurrentMetaManager currentMetaManager;

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;

    protected String localDcName;

    private Map<DelayMap.DrcDirection, UnidirectionalEntity> delayEntityMap = Maps.newConcurrentMap();

    protected Map<MetaKey, MySqlEndpoint> masterMySQLEndpointMap = Maps.newConcurrentMap();

    public Map<DelayMap.DrcDirection, Integer> getFailCount() {
        return Collections.unmodifiableMap(failCount);
    }

    private Map<DelayMap.DrcDirection, Integer> failCount = Maps.newConcurrentMap();

    public static final int FAIL_THRESHOLD = 5;

    public int getMonitorRoundCount() {
        return monitorRoundCount;
    }

    protected int monitorRoundCount = 0;

    public static final int MONITOR_ROUND_INTERVAL = 5;

    public static final int MONITOR_ROUND_RESET_POINT = 60;

    @Override
    public void update(Object args, Observable observable) {
        if (observable instanceof MasterMySQLEndpointObservable) {
            Triple<MetaKey, MySqlEndpoint, ActionEnum> message = (Triple<MetaKey, MySqlEndpoint, ActionEnum>) args;
            MetaKey metaKey = message.getFirst();
            MySqlEndpoint masterMySQLEndpoint = message.getMiddle();
            ActionEnum action = message.getLast();

            if(!metaKey.getDc().equalsIgnoreCase(localDcName)) {
                CONSOLE_BEACON_HEALTH_LOGGER.warn("[OBSERVE][{}] {} not interested in {}({})", getClass().getName(), localDcName, metaKey, masterMySQLEndpoint.getSocketAddress());
                return;
            }

            if(ActionEnum.ADD.equals(action) || ActionEnum.UPDATE.equals(action)) {
                CONSOLE_BEACON_HEALTH_LOGGER.info("[OBSERVE][{}] {} {}({})", getClass().getName(), action.name(), metaKey, masterMySQLEndpoint.getSocketAddress());
                MySqlEndpoint oldEndpoint = masterMySQLEndpointMap.get(metaKey);
                if (oldEndpoint != null) {
                    CONSOLE_BEACON_HEALTH_LOGGER.info("[OBSERVE][{}] {} clear old {}({})", getClass().getName(), action.name(), metaKey, oldEndpoint.getSocketAddress());
                    removeSqlOperator(oldEndpoint);
                }
                masterMySQLEndpointMap.put(metaKey, masterMySQLEndpoint);
            } else if (ActionEnum.DELETE.equals(action)) {
                CONSOLE_BEACON_HEALTH_LOGGER.info("[OBSERVE][{}] {} {}", getClass().getName(), action.name(), metaKey);
                MySqlEndpoint oldEndpoint = masterMySQLEndpointMap.remove(metaKey);
                if (oldEndpoint != null) {
                    CONSOLE_BEACON_HEALTH_LOGGER.info("[OBSERVE][{}] {} clear old {}({})", getClass().getName(), action.name(), metaKey, oldEndpoint.getSocketAddress());
                    removeSqlOperator(oldEndpoint);
                }
            }
        }
    }

    @Override
    public void doInitialize() {
        localDcName = sourceProvider.getLocalDcName();
        currentMetaManager.addObserver(this);
    }

    @Override
    public void doMonitor() {
        String datachangeLastTimeMonitorSwitch = monitorTableSourceProvider.getDatachangeLastTimeMonitorSwitch();
        if(!SWITCH_STATUS_ON.equalsIgnoreCase(datachangeLastTimeMonitorSwitch)) {
            return;
        }

        for (Map.Entry<MetaKey, MySqlEndpoint> entry : masterMySQLEndpointMap.entrySet()) {
            String mhaName = entry.getKey().getMhaName();
            MySqlEndpoint endpoint = entry.getValue();
            List<String> targetDcMha = sourceProvider.getTargetDcMha(mhaName);
            if (targetDcMha != null) {
                updateDrcDelay(mhaName, endpoint, targetDcMha.get(0), targetDcMha.get(1));
            }
        }
        handleMonitorRoundCount();
    }

    @VisibleForTesting
    protected void handleMonitorRoundCount() {
        if(monitorRoundCount >= MONITOR_ROUND_RESET_POINT) {
            monitorRoundCount = 0;
        } else {
            monitorRoundCount++;
        }
    }

    protected void updateDrcDelay(String mha, Endpoint masterDb, String targetDc, String targetMha) {
        DelayMap.DrcDirection drcDirection = new DelayMap.DrcDirection(targetMha, mha);

        if(allowUpdate(drcDirection)) {
            WriteSqlOperatorWrapper sqlOperatorWrapper = getSqlOperatorWrapper(masterDb);
            try {
                CONSOLE_BEACON_HEALTH_LOGGER.debug("[[monitor=datachange,srcMha={},destMha={},endpoint={}]] check delay", drcDirection.getSrcMha(), drcDirection.getDestMha(), masterDb.getSocketAddress());
                long delayInMillis = getDelayInMillis(sqlOperatorWrapper, targetDc);
                reporter.reportDelay(getDelayEntity(drcDirection), delayInMillis, DELAY_V2_MEASUREMENT.getMeasurement());
                if(delayMap.size(drcDirection) <= 0 && delayInMillis > Double.valueOf(NORMAL_DELAY_THRESHOLD).longValue()) {
                    CONSOLE_BEACON_HEALTH_LOGGER.info("[[monitor=datachange,srcMha={},destMha={}]] size: {}, first delay must be smaller than {}ms, actual delay: {}ms", drcDirection.getSrcMha(), drcDirection.getDestMha(), delayMap.size(drcDirection), NORMAL_DELAY_THRESHOLD, delayInMillis);
                    return;
                }

                delayMap.put(drcDirection, delayInMillis);
            } catch (Exception e) {
                CONSOLE_BEACON_HEALTH_LOGGER.error("Fail to get delay for: {}->{}", targetMha, mha, e);
                removeSqlOperator(masterDb);
            }
            if(monitorRoundCount % MONITOR_ROUND_INTERVAL == 0) {
                handleConsoleUpdating(drcDirection);
            }
        }
    }

    /**
     * allow update condition:
     * 1. delayMap.size(drcDirection) > 0: which means it is being updated, 0 means it has been cleared for some reason like not all console updating local db
     * 2. isAllConsoleUpdatingDb(mha, targetMha): if has been cleared, all console updating db would be the basic condition to allow update
     * 3. monitorRoundCount >= MONITOR_ROUND_RESET_POINT: it stops the condition from checking for every round to decrease Http request to remote console
     */
    private boolean allowUpdate(DelayMap.DrcDirection drcDirection) {
        if(delayMap.size(drcDirection) > 0) {
            CONSOLE_BEACON_HEALTH_LOGGER.debug("{} updating", drcDirection.toString());
            return true;
        }
        if(monitorRoundCount < MONITOR_ROUND_RESET_POINT) {
            CONSOLE_BEACON_HEALTH_LOGGER.info("[[monitor=datachange,srcMha={},destMha={}]] wait for monitorCount({}) reach {} to check allowable to update", drcDirection.getSrcMha(), drcDirection.getDestMha(), monitorRoundCount, MONITOR_ROUND_RESET_POINT);
            return false;
        }
        return isAllConsoleUpdatingDb(drcDirection.getDestMha(), drcDirection.getSrcMha());
    }

    private UnidirectionalEntity getDelayEntity(DelayMap.DrcDirection drcDirection) {
        UnidirectionalEntity unidirectionalEntity = delayEntityMap.get(drcDirection);
        if(null == unidirectionalEntity) {
            unidirectionalEntity = new UnidirectionalEntity.Builder()
                    .srcMhaName(drcDirection.getSrcMha())
                    .destMhaName(drcDirection.getDestMha())
                    .build();
            delayEntityMap.put(drcDirection, unidirectionalEntity);
        }
        return unidirectionalEntity;
    }

    @VisibleForTesting
    protected void handleConsoleUpdating(DelayMap.DrcDirection drcDirection) {
        String localMha = drcDirection.getDestMha();
        String targetMha = drcDirection.getSrcMha();
        if(!isAllConsoleUpdatingDb(localMha, targetMha)) {
            CONSOLE_BEACON_HEALTH_LOGGER.info("[[monitor=datachange,srcMha={},destMha={}]] fail count plus 1", localMha, targetMha);
            failCount.merge(drcDirection, 1, Integer::sum);
        } else {
            CONSOLE_BEACON_HEALTH_LOGGER.info("[[monitor=datachange,srcMha={},destMha={}]] fail count reset 0", localMha, targetMha);
            failCount.put(drcDirection, 0);
        }

        if(failCount.get(drcDirection) >= FAIL_THRESHOLD) {
            CONSOLE_BEACON_HEALTH_LOGGER.info("[[monitor=datachange,srcMha={},destMha={}]] fail count({}) >= threshold({}), do clear", localMha, targetMha, failCount.get(drcDirection), FAIL_THRESHOLD);
            delayMap.clear(drcDirection);
            failCount.put(drcDirection, 0);
        }
    }

    @VisibleForTesting
    protected boolean isAllConsoleUpdatingDb(String localMha, String targetMha) {
        Boolean localDcUpdating = healthService.isLocalDcUpdating(localMha);
        Boolean targetDcUpdating = healthService.isTargetDcUpdating(localMha);
        CONSOLE_BEACON_HEALTH_LOGGER.info("[check update]local({}): {}, target({}): {}", localMha, localDcUpdating, targetMha, targetDcUpdating);
        return localDcUpdating && targetDcUpdating;
    }

    public static long getDelayInMillis(WriteSqlOperatorWrapper sqlOperatorWrapper, String dc) throws Exception {
        SimpleDateFormat formatter = dateFormatThreadLocal.get();
        ReadResource readResource = null;
        try {
            String sql = String.format(SELECT_DATACHANGE_LASTTIME, dc);
            GeneralSingleExecution execution = new GeneralSingleExecution(sql);
            readResource = sqlOperatorWrapper.select(execution);
            ResultSet rs = readResource.getResultSet();
            rs.next();
            String datachangeLasttimeStr = rs.getString(DATACHANGE_LASTTIME_INDEX);
            CONSOLE_BEACON_HEALTH_LOGGER.info("[datachange] dc: {}, timestamp: {}", dc, datachangeLasttimeStr);
            long roughDelay = System.currentTimeMillis() - formatter.parse(datachangeLasttimeStr).getTime();
            return roughDelay > 0 ? roughDelay : 0;
        } finally {
            if(readResource != null) {
                readResource.close();
            }
        }
    }
}
