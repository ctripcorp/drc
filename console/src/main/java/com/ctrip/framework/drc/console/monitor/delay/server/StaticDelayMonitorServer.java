package com.ctrip.framework.drc.console.monitor.delay.server;

import com.ctrip.framework.drc.console.monitor.delay.config.DelayMonitorSlaveConfig;
import com.ctrip.framework.drc.console.monitor.delay.impl.driver.DelayMonitorConnection;
import com.ctrip.framework.drc.console.monitor.delay.task.PeriodicalUpdateDbTask;
import com.ctrip.framework.drc.console.monitor.delay.task.PeriodicalUpdateDbTaskV2;
import com.ctrip.framework.drc.console.param.mysql.DdlHistoryEntity;
import com.ctrip.framework.drc.console.service.v2.CentralService;
import com.ctrip.framework.drc.core.config.DynamicConfig;
import com.ctrip.framework.drc.core.driver.AbstractMySQLSlave;
import com.ctrip.framework.drc.core.driver.MySQLConnection;
import com.ctrip.framework.drc.core.driver.MySQLConnector;
import com.ctrip.framework.drc.core.driver.MySQLSlave;
import com.ctrip.framework.drc.core.driver.binlog.LogEventHandler;
import com.ctrip.framework.drc.core.driver.binlog.constant.QueryType;
import com.ctrip.framework.drc.core.driver.binlog.impl.DelayMonitorLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.DrcHeartbeatLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.ParsedDdlLogEvent;
import com.ctrip.framework.drc.core.driver.config.MySQLSlaveConfig;
import com.ctrip.framework.drc.core.exception.dump.NetworkException;
import com.ctrip.framework.drc.core.monitor.column.DbDelayDto;
import com.ctrip.framework.drc.core.monitor.column.DbDelayMonitorColumn;
import com.ctrip.framework.drc.core.monitor.column.DelayInfo;
import com.ctrip.framework.drc.core.monitor.column.DelayMonitorColumn;
import com.ctrip.framework.drc.core.monitor.entity.UnidirectionalEntity;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.core.server.config.RegistryKey;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.xpipe.redis.ProxyRegistry;
import com.ctrip.xpipe.api.codec.Codec;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.core.driver.config.GlobalConfig.BU;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.SLOW_COMMIT_THRESHOLD;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2019-11-27
 * Given Replicator and drcDbEndpoint,
 * Console will act as a netty client to keep receiving the timestamp sent by the Replicator,
 * then calculate the delay, report to hickwall
 */
public class StaticDelayMonitorServer extends AbstractMySQLSlave implements MySQLSlave {

    private final Logger logger = LoggerFactory.getLogger("delayMonitorLogger");

    private final Logger dbLogger = LoggerFactory.getLogger("dbDelayMonitorLogger");

    private ScheduledExecutorService checkScheduledExecutorService = ThreadUtils.newFixedThreadScheduledPool(2, "DelayMonitor");

    private CentralService centralService;

    private MySQLConnector mySQLConnector;

    private PeriodicalUpdateDbTask periodicalUpdateDbTask;
    private PeriodicalUpdateDbTaskV2 periodicalUpdateDbTaskV2;

    private static ThreadLocal<SimpleDateFormat> dateFormatThreadLocal = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"));

    private static final String DRC_DELAY_EXCEPTION_MESUREMENT = "fx.drc.delay.exception";
    public static final String FX_DRC_DB_DELAY_MEASUREMENT = "fx.drc.db.delay";
    private static final String FX_DRC_DB_DELAY_EXCEPTION_MEASUREMENT = "fx.drc.db.delay.exception";

    private Map<String, UnidirectionalEntity> entityMap = Maps.newConcurrentMap();
    private Map<String, Map<String, UnidirectionalEntity>> entityV2Map = Maps.newConcurrentMap();

    /**
     * key : srcMha, value: the time when Console receives the timestamp(receivedTime) sent by Replicator
     */
    private Map<String, Long> receiveTimeMap = Maps.newConcurrentMap();
    private Map<String, Map<String, Long>> receiveTimeMapV2 = Maps.newConcurrentMap();

    private static final long TOLERANCE_TIME = 5 * 60000L;

    private static final long SLOW_THRESHOLD = 100L;

    public static final long DELTA = 50L;

    private static final long SLOW_COMMIT_THRESHOLD_WITH_DELTA = SLOW_COMMIT_THRESHOLD - DELTA;

    private long toleranceTime;

    private static final long HUGE_VAL = 60 * 60000L;

    private static final int INITIAL_DELAY = 1;

    private static final int PERIOD = 1;

    private DelayMonitorSlaveConfig config;

    private boolean isReplicatorMaster = true;

    // the time which was updated by local Console and flew thru local MySQL-local Rep-dest Applier-dest MySQL, and finally sent by dest Rep
    long datachangeLastime = 0;

    // default 1 day
    private long delayExceptionTime = 864500000L;

    private static final int PENDING_TIME = 2000;

    private static final String WARN = "warn";

    private static final String ERROR = "error";

    private static final String DEBUG = "debug";

    private static final String INFO = "info";

    private static final String CLOG_TAGS = "[[monitor=delay,direction={}({}):{}({}),cluster={},replicator={}:{},measurement={},role={}]]";
    private static final String CLOG_TAGS_V2 = "[[monitor=delay_v2,db={},direction={}({}):{}({}),cluster={},replicator={}:{},measurement={},role={}]]";

    public void setConfig(DelayMonitorSlaveConfig config) {
        this.config = config;
    }

    /**
     * Lambda expression function: LogEventHandler for StaticDelayMonitor
     * it will be called when failure of MySQLConnection.DumpCallBack or passed some logEvent from Replicator
     */
    private LogEventHandler eventHandler = (logEvent, logEventCallBack, exception) -> {
        SimpleDateFormat formatter = dateFormatThreadLocal.get();
        if (null != exception) {
            if (exception instanceof NetworkException) {
                // reconnect to the replicator
                log("network error when dumping event, going to restart DelayMonitorServer", ERROR, exception);
                try {
                    log("pending to reconnect, for 2 seconds", DEBUG, null);
                    Thread.sleep(PENDING_TIME);
                } catch (InterruptedException e) {
                    log("UNLIKELY - interrupted when pending to reconnect replicator", ERROR, e);
                    return;
                }
                try {
                    log("reconnecting...", DEBUG, null);
                    if (!getLifecycleState().isStopped()) {
                        this.stop();
                    }
                    this.start();
                    return;
                } catch (Exception e) {
                    log("DelayMonitorServer fails to reconnect to replicator, will do agaGin", ERROR, e);
                    if (getLifecycleState().isDisposed()) {
                        log("StaticDelayMonitorServer has been disposed.", WARN, null);
                        return;
                    }
                }
            } else {
                log("UNLIKELY - unknown exception", ERROR, exception);
                return;
            }
        }
        /**
         * generate the data[cluster_name, srcDcName, destDcName, delay]
         * 1. report to hickwall
         * 2. store in drc table
         */
        if (logEvent instanceof DelayMonitorLogEvent) {
            try {
                DelayMonitorLogEvent delayMonitorLogEvent = (DelayMonitorLogEvent) logEvent;
                if (DynamicConfig.getInstance().getOldDelayEventProcessSwitch() && DelayMonitorColumn.match(delayMonitorLogEvent)) {
                    processMhaDelay(formatter, delayMonitorLogEvent);
                } else if (DbDelayMonitorColumn.match(delayMonitorLogEvent)) {
                    processDbDelay(delayMonitorLogEvent);
                } else {
                    dbLogger.warn("parse fail, logEvent: {}", delayMonitorLogEvent);
                    DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.delay.parse.fail", "unknown");
                }
            } catch (Exception e) {
                log("[parse] DelayMonitorLogEvent: ", ERROR, e);
            } finally {
                try {
                    logEvent.release();
                } catch (Exception e) {
                    log("[release] DelayMonitorLogEvent: ", ERROR, e);
                }
            }
        } else if (logEvent instanceof ParsedDdlLogEvent) {
            try {
                if (isReplicatorMaster) {
                    return;
                }
                // slave replicator monitor connect by local console, suitable for double-truncate-check monitor;
                ParsedDdlLogEvent parsedDdlLogEvent = (ParsedDdlLogEvent) logEvent;
                QueryType queryType = parsedDdlLogEvent.getQueryType();
                String ddl = parsedDdlLogEvent.getDdl();
                String schema = parsedDdlLogEvent.getSchema();
                String table = parsedDdlLogEvent.getTable();
                if (QueryType.TRUNCATE.equals(queryType)) {
                    try {
                        String data = String.format("[TRUNCATE] %s.%s in %s.%s(%s)", schema, table, config.getCluster(), config.getDestMha(), config.getDestDc());
                        log(data, INFO, null);
                        String catName = String.format("%s.%s.%s.%s", config.getCluster(), config.getDestMha(), schema, table);
                        DefaultEventMonitorHolder.getInstance().logEvent("DRC.console.truncate", catName);
                        DdlHistoryEntity entity = new DdlHistoryEntity(config.getDestMha(), ddl, queryType.ordinal(), schema, table);
                        centralService.insertDdlHistory(entity);
                    } catch (Throwable t) {
                        String data = String.format("Fail insert truncate history for %s.%s", schema, table);
                        log(data, ERROR, t);
                    }
                }
            } catch (Exception e) {
                log("[parse] ParsedDdlLogEvent: ", ERROR, e);
            } finally {
                try {
                    logEvent.release();
                } catch (Exception e) {
                    log("[release] ParsedDdlLogEvent: ", ERROR, e);
                }
            }
        } else if (logEvent instanceof DrcHeartbeatLogEvent) {
            try {
                logEventCallBack.onHeartHeat();
            } finally {
                try {
                    logEvent.release();
                } catch (Exception e) {
                    log("[release] DrcHeartbeatLogEvent: ", ERROR, e);
                }
            }
        }
    };

    private void processMhaDelay(SimpleDateFormat formatter, DelayMonitorLogEvent delayMonitorLogEvent) {
        List<List<Object>> rowValues = DelayMonitorColumn.getAfterPresentRowsValues(delayMonitorLogEvent);
        String gtid = delayMonitorLogEvent.getGtid();
        if (rowValues != null && !rowValues.isEmpty()) {
            reportHickwall(gtid, rowValues, formatter);
        }
    }

    private void processDbDelay(DelayMonitorLogEvent delayMonitorLogEvent) throws ParseException {
        DbDelayDto delayDto = DbDelayMonitorColumn.parseEvent(delayMonitorLogEvent);
        String gtid = delayMonitorLogEvent.getGtid();
        String mhaString = delayDto.getMha();
        String dbName = delayDto.getDbName();
        if (!isReplicatorMaster && !mhaString.equalsIgnoreCase(config.getDestMha())) {
            // filter same region drc other mha delayInfo
            return;
        }

        Long datachangeLasttime = delayDto.getDatachangeLasttime();
        SimpleDateFormat formatter = dateFormatThreadLocal.get();
        String delayString = formatter.format(datachangeLasttime);

        Long rTime = System.currentTimeMillis();
        receiveTimeMapV2.computeIfAbsent(mhaString, k -> new HashMap<>()).put(dbName, rTime);

        logForDb(delayDto.getDbName(), "receiveTime: " + rTime + '(' + datachangeLasttime + ')', INFO, null);
        logForDb(delayDto.getDbName(), "Calc: " + rTime + '-' + datachangeLasttime, INFO, null);
        long delay = rTime - datachangeLasttime;
        if (delay > SLOW_COMMIT_THRESHOLD_WITH_DELTA) {
            String registryKey = RegistryKey.from(config.getCluster(), mhaString);
            PeriodicalUpdateDbTaskV2.DatachangeLastTime datachangeLastTime = new PeriodicalUpdateDbTaskV2.DatachangeLastTime(registryKey, dbName, delayString);
            Long commitTime = periodicalUpdateDbTaskV2.getAndDeleteCommitTime(datachangeLastTime);
            if (null != commitTime) {
                logForDb(delayDto.getDbName(), String.format("Calc C: %d-%d(%s)", rTime, commitTime, delayString), WARN, null);
                delay = Math.max(rTime - commitTime, 0);
                dbLogger.warn("[[monitor=delay,direction={}({}):{}({}),cluster={},replicator={}:{},measurement={},slow={}]]\nslow commit\nGTID: {}\nrealDelay({}ms) = currentTime({}) - commitTime({})", mhaString, config.getDc(), config.getDestMha(), config.getDestDc(), config.getCluster(), config.getEndpoint().getHost(), config.getEndpoint().getPort(), FX_DRC_DB_DELAY_MEASUREMENT, delay > SLOW_THRESHOLD, null == gtid ? "null" : gtid, delay, formatter.format(rTime), formatter.format(commitTime));
            }
        }

        UnidirectionalEntity unidirectionalEntity = getUnidirectionalEntity(mhaString, dbName);
        if (delay < delayExceptionTime) {
            DefaultReporterHolder.getInstance().reportDelay(unidirectionalEntity, delay, FX_DRC_DB_DELAY_MEASUREMENT);
            DefaultReporterHolder.getInstance().reportDelay(unidirectionalEntity, 0L, FX_DRC_DB_DELAY_EXCEPTION_MEASUREMENT);
            if (null != gtid) {
                dbLogger.info("[[" +
                                "monitor=delay_v2,db={},direction={}({}):{}({})," +
                                "cluster={}," +
                                "replicator={}:{}," +
                                "measurement={},slow={},"
                                + "role={}]]" +
                                "\n[Report Delay] {}ms" +
                                "\nGTID: {}" +
                                "\ndelay = currentTime({}) - datachange_lasttime({})" +
                                "[or commitTime]",
                        dbName, mhaString, config.getDc(), config.getDestMha(), config.getDestDc(),
                        config.getCluster(),
                        config.getEndpoint().getHost(), config.getEndpoint().getPort(),
                        FX_DRC_DB_DELAY_MEASUREMENT, delay > SLOW_THRESHOLD,
                        isReplicatorMaster,
                        delay, gtid,
                        formatter.format(rTime), delayString);
            } else {
                dbLogger.info("[[monitor=delay_v2,db={},direction={}({}):{}({}),cluster={},replicator={}:{},measurement={},slow={}]]\n[Report Delay] {}ms\ndelay = currentTime({}) - datachange_lasttime({})[or commitTime]", dbName, mhaString, config.getDc(), config.getDestMha(), config.getDestDc(), config.getCluster(), config.getEndpoint().getHost(), config.getEndpoint().getPort(), config.getMeasurement(), delay > SLOW_THRESHOLD, delay, formatter.format(rTime), delayString);
            }
        } else {
            DefaultReporterHolder.getInstance().reportDelay(unidirectionalEntity, delay, FX_DRC_DB_DELAY_EXCEPTION_MEASUREMENT);
            if (null != gtid) {
                dbLogger.info("[[monitor=delay_v2,db={},direction={}({}):{}({}),cluster={},replicator={}:{},measurement={},slow={}]]\n[Report Delay Exception] {}ms\nGTID: {}\ndelay = currentTime({}) - datachange_lasttime({})[or commitTime]", dbName, mhaString, config.getDc(), config.getDestMha(), config.getDestDc(), config.getCluster(), config.getEndpoint().getHost(), config.getEndpoint().getPort(), config.getMeasurement(), delay > SLOW_THRESHOLD, delay, gtid, formatter.format(rTime), delayString);
            } else {
                dbLogger.info("[[monitor=delay_v2,db={},direction={}({}):{}({}),cluster={},replicator={}:{},measurement={},slow={}]]\n[Report Delay Exception] {}ms\ndelay = currentTime({}) - datachange_lasttime({})[or commitTime]", dbName, mhaString, config.getDc(), config.getDestMha(), config.getDestDc(), config.getCluster(), config.getEndpoint().getHost(), config.getEndpoint().getPort(), config.getMeasurement(), delay > SLOW_THRESHOLD, delay, formatter.format(rTime), delayString);
            }
        }
    }


    public StaticDelayMonitorServer(MySQLSlaveConfig mySQLSlaveConfig,
                                    MySQLConnector mySQLConnector,
                                    PeriodicalUpdateDbTask periodicalUpdateDbTask,
                                    PeriodicalUpdateDbTaskV2 periodicalUpdateDbTaskV2,
                                    CentralService centralService,
                                    long delayExceptionTime) {
        super(mySQLSlaveConfig);
        this.config = (DelayMonitorSlaveConfig) mySQLSlaveConfig;
        this.setLogEventHandler(eventHandler);
        this.mySQLConnector = mySQLConnector;
        this.periodicalUpdateDbTask = periodicalUpdateDbTask;
        this.periodicalUpdateDbTaskV2 = periodicalUpdateDbTaskV2;
        this.delayExceptionTime = delayExceptionTime;
        this.centralService = centralService;
        toleranceTime = TOLERANCE_TIME;

        if (config.getMha() != null && config.getMha().equals(config.getDestMha())) {
            this.isReplicatorMaster = false;
        }
    }

    @Override
    protected MySQLConnection newConnection(MySQLSlaveConfig mySQLSlaveConfig, LogEventHandler eventHandler) {
        return new DelayMonitorConnection(mySQLSlaveConfig, eventHandler, mySQLConnector);
    }

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        String routeInfo = config.getRouteInfo();
        if (StringUtils.isNotBlank(routeInfo)) {
            ProxyRegistry.registerProxy(config.getRegistryKey(), config.getIp(), config.getPort(), routeInfo);
        }
        log("initialized server success", INFO, null);
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        Long rTime = System.currentTimeMillis();
        if (isReplicatorMaster) {
            Set<String> mhasShouldMonitor = periodicalUpdateDbTask.getSrcMhasShouldMonitor(config.getDestMha());
            logger.info("dstClusterId:{},srcMhasShouldMonitor:{}", config.getCluster() + "." + config.getDestMha(), mhasShouldMonitor);
            mhasShouldMonitor.forEach(mha -> {
                receiveTimeMap.put(mha, rTime);
            });
        } else {
            receiveTimeMap.put(config.getMha(), rTime);
        }
        log("init receiveTime: " + rTime + '(' + dateFormatThreadLocal.get().format(rTime) + ')', INFO, null);

        checkScheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    SimpleDateFormat formatter = dateFormatThreadLocal.get();
                    log(" CLOSE DEBUG, version" + '(' + formatter.format(System.currentTimeMillis()) + ')', DEBUG, null);
                    long curTime = System.currentTimeMillis();

                    Set<String> mhasShouldMonitor;
                    if (isReplicatorMaster) {
                        mhasShouldMonitor = periodicalUpdateDbTask.getSrcMhasShouldMonitor(config.getDestMha());
                    } else {
                        boolean mhaMonitorEnabled = periodicalUpdateDbTask.isMhaMonitorEnabled(config.getMha());
                        mhasShouldMonitor = mhaMonitorEnabled? Sets.newHashSet(config.getMha()) : Sets.newHashSet();
                    }
                    
                    Iterator<Entry<String, Long>> iterator = receiveTimeMap.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Entry<String, Long> entry = iterator.next();
                        String srcMha = entry.getKey();
                        if (!mhasShouldMonitor.contains(srcMha)) {
                            LinkedHashMap<String, String> tags = Maps.newLinkedHashMap();
                            tags.put("destMha", config.getDestMha());
                            tags.put("srcMha", srcMha);
                            iterator.remove();
                            DefaultReporterHolder.getInstance().removeRegister(config.getMeasurement(),tags);
                            continue;
                        }
                        Long receiveTime = entry.getValue();
                        long timeDiff = curTime - receiveTime;
                        if (timeDiff > toleranceTime) {
                            UnidirectionalEntity unidirectionalEntity = getUnidirectionalEntity(srcMha);
                            DefaultReporterHolder.getInstance()
                                    .reportDelay(unidirectionalEntity, HUGE_VAL, config.getMeasurement());
                            DefaultEventMonitorHolder.getInstance().logEvent(
                                    "DRC." + config.getMeasurement(),
                                    unidirectionalEntity.getMhaName() + '.' + unidirectionalEntity.getDestMhaName());
                            log("[Report huge] Console not receive timestamp for " + timeDiff + "ms, "
                                    + "Last receive time : " + formatter.format(receiveTime) +
                                    " and current time: " + formatter.format(curTime) + ","
                                    + srcMha + "->" + config.getDestMha()
                                    + " report a huge number to trigger the alert.", INFO, null);
                        }
                    }
                } catch (Throwable t) {
                    logger.error("static delay monitor server schedule error", t);
                }
            }
        }, INITIAL_DELAY, PERIOD, TimeUnit.SECONDS);

        Map<String, Map<String, Long>> initMap = periodicalUpdateDbTaskV2.getMhaDbRelatedByDestMha(config.getDestMha())
                .entrySet().stream().collect(Collectors.toMap(
                        Entry::getKey,
                        e -> e.getValue().stream().distinct().collect(Collectors.toMap(t -> t, t -> rTime))
                ));
        receiveTimeMapV2.putAll(initMap);
        log("init receiveTime: " + rTime + '(' + dateFormatThreadLocal.get().format(rTime) + ')', INFO, null);
        checkScheduledExecutorService.scheduleWithFixedDelay(() -> {
            try {
                SimpleDateFormat formatter = dateFormatThreadLocal.get();
                long curTime = System.currentTimeMillis();
                Map<String, List<String>> relatedMhaDb = periodicalUpdateDbTaskV2.getMhaDbRelatedByDestMha(config.getDestMha());
                for (Iterator<Entry<String, Map<String, Long>>> mhaIterator = receiveTimeMapV2.entrySet().iterator(); mhaIterator.hasNext(); ) {
                    Entry<String, Map<String, Long>> entry = mhaIterator.next();
                    String mhaName = entry.getKey();
                    List<String> relatedDbs = relatedMhaDb.get(mhaName);

                    if (CollectionUtils.isEmpty(relatedDbs)) {
                        if (isReplicatorMaster) {
                            mhaIterator.remove();
                        }
                        continue;
                    }
                    for (Iterator<Entry<String, Long>> dbIterator = entry.getValue().entrySet().iterator(); dbIterator.hasNext(); ) {
                        Entry<String, Long> dbEntry = dbIterator.next();
                        String dbName = dbEntry.getKey();
                        if (!relatedDbs.contains(dbName)) {
                            if (isReplicatorMaster) {
                                dbIterator.remove();
                            }
                            continue;
                        }
                        Long receiveTime = dbEntry.getValue();
                        long timeDiff = curTime - receiveTime;
                        if (timeDiff > toleranceTime) {
                            UnidirectionalEntity unidirectionalEntity = getUnidirectionalEntity(mhaName, dbName);
                            DefaultReporterHolder.getInstance().reportDelay(unidirectionalEntity, HUGE_VAL, FX_DRC_DB_DELAY_MEASUREMENT);
                            String identifier = String.join(".", dbName, unidirectionalEntity.getMhaName(), unidirectionalEntity.getDestMhaName());
                            DefaultEventMonitorHolder.getInstance().logEvent(
                                    "DRC." + FX_DRC_DB_DELAY_MEASUREMENT,
                                    identifier);
                            logForDb(dbName, "[Report huge] Console not receive timestamp for " + timeDiff + "ms for " + identifier
                                    + "Last receive time : " + formatter.format(receiveTime) +
                                    " and current time: " + formatter.format(curTime) + ","
                                    + " report a huge number to trigger the alert.", INFO, null);
                        }
                    }
                }
            } catch (Throwable t) {
                logger.error("static delay monitor server schedule error v2", t);
            }
        }, INITIAL_DELAY, PERIOD, TimeUnit.SECONDS);

        log("started server success", INFO, null);
    }

    @Override
    protected void doStop() throws Exception {
        super.doStop();
        if (!parseExecutorService.awaitTermination(1, TimeUnit.SECONDS)) {
            parseExecutorService.shutdownNow();
        }
        if (!checkScheduledExecutorService.awaitTermination(1, TimeUnit.SECONDS)) {
            checkScheduledExecutorService.shutdownNow();
        }
        UnidirectionalEntity unidirectionalEntity = entityMap.remove(config.getMha());
        if (unidirectionalEntity != null) {
            DefaultReporterHolder.getInstance().removeRegister(config.getMeasurement(), "destMha", config.getDestMha());
        }

        Map<String, UnidirectionalEntity> remove = entityV2Map.remove(config.getMha());
        if (remove != null && remove.size() > 0) {
            DefaultReporterHolder.getInstance().removeRegister(FX_DRC_DB_DELAY_MEASUREMENT, "destMha", config.getDestMha());
        }

        log("stopped server success", INFO, null);
    }

    @Override
    protected void doDispose() throws Exception {
        super.doDispose();
        if (StringUtils.isNotBlank(config.getRouteInfo())) {
            ProxyRegistry.unregisterProxy(config.getRegistryKey(), config.getIp(), config.getPort());
        }
        log("disposed server success", INFO, null);
    }

    private UnidirectionalEntity getUnidirectionalEntity(String mhaString) {
        UnidirectionalEntity unidirectionalEntity = entityMap.get(mhaString);
        if (null == unidirectionalEntity) {
            unidirectionalEntity = new UnidirectionalEntity.Builder()
                    .clusterAppId(null)
                    .buName(BU)
                    .srcDcName(config.getDc())
                    .destDcName(config.getDestDc())
                    .clusterName(config.getCluster())
                    .srcMhaName(mhaString)
                    .destMhaName(config.getDestMha())
                    .isReplicatorMaster(isReplicatorMaster)
                    .replicatorAddress(config.getEndpoint().getSocketAddress().toString())
                    .build();
            entityMap.put(mhaString, unidirectionalEntity);
        }
        return unidirectionalEntity;
    }


    private UnidirectionalEntity getUnidirectionalEntity(String mhaString, String dbName) {
        Map<String, UnidirectionalEntity> dbMap = entityV2Map.get(mhaString);
        if (dbMap == null) {
            dbMap = Maps.newConcurrentMap();
            entityV2Map.put(mhaString, dbMap);
        }
        UnidirectionalEntity unidirectionalEntity = dbMap.get(dbName);
        if (null == unidirectionalEntity) {
            unidirectionalEntity = new UnidirectionalEntity.Builder()
                    .clusterAppId(null)
                    .buName(BU)
                    .srcDcName(config.getDc())
                    .destDcName(config.getDestDc())
                    .clusterName(config.getCluster())
                    .srcMhaName(mhaString)
                    .destMhaName(config.getDestMha())
                    .isReplicatorMaster(isReplicatorMaster)
                    .replicatorAddress(config.getEndpoint().getSocketAddress().toString())
                    .dbName(dbName)
                    .build();
            dbMap.put(mhaString, unidirectionalEntity);
        }
        return unidirectionalEntity;
    }

    protected void reportHickwall(String gtid, List<List<Object>> rowValues, SimpleDateFormat formatter) {
        List<Object> values = rowValues.get(0);
        String mhaDelayInfoJson = (String) values.get(2);
        String mhaString;
        try {
            DelayInfo mhaDelayInfo = Codec.DEFAULT.decode(mhaDelayInfoJson, DelayInfo.class);
            mhaString = mhaDelayInfo.getM();
        } catch (Exception e) {
            mhaString = mhaDelayInfoJson;
        }

        if (!isReplicatorMaster && !mhaString.equalsIgnoreCase(config.getDestMha())) {
            // filter same region drc other mha delayInfo 
            return;
        }

        String delayString = (String) values.get(3);
        log("mha: " + mhaString + ", delayString: " + delayString + ", GTID: " + gtid, INFO, null);

        Long rTime = System.currentTimeMillis();
        receiveTimeMap.put(mhaString, rTime);
        log("receiveTime: " + rTime + '(' + formatter.format(rTime) + ')', INFO, null);
        try {
            datachangeLastime = formatter.parse(delayString).getTime();
            log("Calc: " + rTime + '-' + datachangeLastime, INFO, null);
            long delay = rTime - datachangeLastime;
            if (delay > SLOW_COMMIT_THRESHOLD_WITH_DELTA) {
                String registyKey = RegistryKey.from(config.getCluster(), mhaString);
                PeriodicalUpdateDbTask.DatachangeLastTime datachangeLastTime = new PeriodicalUpdateDbTask.DatachangeLastTime(registyKey, delayString);
                Long commitTime = periodicalUpdateDbTask.getAndDeleteCommitTime(datachangeLastTime);
                if (null != commitTime) {
                    log(String.format("Calc C: %d-%d(%s)", rTime, commitTime, delayString), WARN, null);
                    delay = Math.max(rTime - commitTime, 0);
                    logger.warn("[[monitor=delay,direction={}({}):{}({}),cluster={},replicator={}:{},measurement={},slow={}]]\nslow commit\nGTID: {}\nrealDelay({}ms) = currentTime({}) - commitTime({})", mhaString, config.getDc(), config.getDestMha(), config.getDestDc(), config.getCluster(), config.getEndpoint().getHost(), config.getEndpoint().getPort(), config.getMeasurement(), delay > SLOW_THRESHOLD, null == gtid ? "null" : gtid, delay, formatter.format(rTime), formatter.format(commitTime));
                }
            }

            UnidirectionalEntity unidirectionalEntity = getUnidirectionalEntity(mhaString);
            if (delay < delayExceptionTime) {
                DefaultReporterHolder.getInstance().reportDelay(unidirectionalEntity, delay, config.getMeasurement());
                DefaultReporterHolder.getInstance().reportDelay(unidirectionalEntity, 0L, DRC_DELAY_EXCEPTION_MESUREMENT);
                if (null != gtid) {
                    logger.info("[[" +
                                    "monitor=delay,direction={}({}):{}({})," +
                                    "cluster={}," +
                                    "replicator={}:{}," +
                                    "measurement={},slow={},"
                                    + "role={}]]" +
                                    "\n[Report Delay] {}ms" +
                                    "\nGTID: {}" +
                                    "\ndelay = currentTime({}) - datachange_lasttime({})" +
                                    "[or commitTime]",
                            mhaString, config.getDc(), config.getDestMha(), config.getDestDc(),
                            config.getCluster(),
                            config.getEndpoint().getHost(), config.getEndpoint().getPort(),
                            config.getMeasurement(), delay > SLOW_THRESHOLD,
                            isReplicatorMaster,
                            delay, gtid,
                            formatter.format(rTime), delayString);
                } else {
                    logger.info("[[monitor=delay,direction={}({}):{}({}),cluster={},replicator={}:{},measurement={},slow={}]]\n[Report Delay] {}ms\ndelay = currentTime({}) - datachange_lasttime({})[or commitTime]", mhaString, config.getDc(), config.getDestMha(), config.getDestDc(), config.getCluster(), config.getEndpoint().getHost(), config.getEndpoint().getPort(), config.getMeasurement(), delay > SLOW_THRESHOLD, delay, formatter.format(rTime), delayString);
                }
            } else {
                DefaultReporterHolder.getInstance().reportDelay(unidirectionalEntity, delay, DRC_DELAY_EXCEPTION_MESUREMENT);
                if (null != gtid) {
                    logger.info("[[monitor=delay,direction={}({}):{}({}),cluster={},replicator={}:{},measurement={},slow={}]]\n[Report Delay Exception] {}ms\nGTID: {}\ndelay = currentTime({}) - datachange_lasttime({})[or commitTime]", mhaString, config.getDc(), config.getDestMha(), config.getDestDc(), config.getCluster(), config.getEndpoint().getHost(), config.getEndpoint().getPort(), config.getMeasurement(), delay > SLOW_THRESHOLD, delay, gtid, formatter.format(rTime), delayString);
                } else {
                    logger.info("[[monitor=delay,direction={}({}):{}({}),cluster={},replicator={}:{},measurement={},slow={}]]\n[Report Delay Exception] {}ms\ndelay = currentTime({}) - datachange_lasttime({})[or commitTime]", mhaString, config.getDc(), config.getDestMha(), config.getDestDc(), config.getCluster(), config.getEndpoint().getHost(), config.getEndpoint().getPort(), config.getMeasurement(), delay > SLOW_THRESHOLD, delay, formatter.format(rTime), delayString);
                }
            }
        } catch (ParseException e) {
            log("Parse " + delayString + " exception, ", ERROR, e);
        }
    }


    private void log(String msg, String types, Throwable t) {
        String prefix = new StringBuilder().append(CLOG_TAGS).append(msg).toString();
        switch (types) {
            case WARN:
                logger.warn(prefix, config.getMha(), config.getDc(), config.getDestMha(), config.getDestDc(),
                        config.getCluster(), config.getEndpoint().getHost(), config.getEndpoint().getPort(),
                        config.getMeasurement(), isReplicatorMaster);
                break;
            case ERROR:
                logger.error(prefix, config.getMha(), config.getDc(), config.getDestMha(), config.getDestDc(),
                        config.getCluster(), config.getEndpoint().getHost(), config.getEndpoint().getPort(),
                        config.getMeasurement(), isReplicatorMaster, t);
                break;
            case DEBUG:
                logger.debug(prefix, config.getMha(), config.getDc(), config.getDestMha(), config.getDestDc(),
                        config.getCluster(), config.getEndpoint().getHost(), config.getEndpoint().getPort(),
                        config.getMeasurement(), isReplicatorMaster);
                break;
            case INFO:
                logger.info(prefix, config.getMha(), config.getDc(), config.getDestMha(), config.getDestDc(),
                        config.getCluster(), config.getEndpoint().getHost(), config.getEndpoint().getPort(),
                        config.getMeasurement(), isReplicatorMaster);
                break;
        }
    }

    private void logForDb(String dbName, String msg, String types, Throwable t) {
        String prefix = CLOG_TAGS_V2 + msg;

        switch (types) {
            case WARN:
                dbLogger.warn(prefix, dbName, config.getMha(), config.getDc(), config.getDestMha(), config.getDestDc(),
                        config.getCluster(), config.getEndpoint().getHost(), config.getEndpoint().getPort(),
                        FX_DRC_DB_DELAY_MEASUREMENT, isReplicatorMaster);
                break;
            case ERROR:
                dbLogger.error(prefix, dbName, config.getMha(), config.getDc(), config.getDestMha(), config.getDestDc(),
                        config.getCluster(), config.getEndpoint().getHost(), config.getEndpoint().getPort(),
                        FX_DRC_DB_DELAY_MEASUREMENT, isReplicatorMaster, t);
                break;
            case DEBUG:
                dbLogger.debug(prefix, dbName, config.getMha(), config.getDc(), config.getDestMha(), config.getDestDc(),
                        config.getCluster(), config.getEndpoint().getHost(), config.getEndpoint().getPort(),
                        FX_DRC_DB_DELAY_MEASUREMENT, isReplicatorMaster);
                break;
            case INFO:
                dbLogger.info(prefix, dbName, config.getMha(), config.getDc(), config.getDestMha(), config.getDestDc(),
                        config.getCluster(), config.getEndpoint().getHost(), config.getEndpoint().getPort(),
                        FX_DRC_DB_DELAY_MEASUREMENT, isReplicatorMaster);
                break;
        }
    }

    public DelayMonitorSlaveConfig getConfig() {
        return config;
    }
}
