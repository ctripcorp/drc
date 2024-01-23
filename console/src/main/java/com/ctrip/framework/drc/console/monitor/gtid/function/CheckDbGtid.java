package com.ctrip.framework.drc.console.monitor.gtid.function;

import com.ctrip.framework.drc.console.monitor.delay.config.DataCenterService;
import com.ctrip.framework.drc.console.pojo.MetaKey;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.binlog.gtid.db.DbTransactionTableGtidReader;
import com.ctrip.framework.drc.core.driver.binlog.gtid.db.GtidReader;
import com.ctrip.framework.drc.core.driver.binlog.gtid.db.ShowMasterGtidReader;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.driver.healthcheck.task.ExecutedGtidQueryTask;
import com.ctrip.framework.drc.core.monitor.entity.GtidGapEntity;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.core.monitor.reporter.Reporter;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.xpipe.api.config.Config;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.config.AbstractConfigBean;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.*;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.core.driver.config.GlobalConfig.BU;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONSOLE_GTID_LOGGER;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.PROCESSORS_SIZE;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2019-12-25
 * Check all the MySQL master's GTID and report to hickwall
 */
@Component
public class CheckDbGtid extends AbstractConfigBean {

    private final Reporter reporter = DefaultReporterHolder.getInstance();

    private final ListeningExecutorService gtidCheckExecutorService = MoreExecutors.listeningDecorator(ThreadUtils.newFixedThreadPool(PROCESSORS_SIZE, "PeriodicalCheckDbGtid"));

    private static final long NON_REPEAT_COUNT = 0L;

    public CheckDbGtid() {
    }

    public CheckDbGtid(Config config) {
        super(config);
    }

    /**
     * key: mha, value: corresponding GtidSet from last check
     * for checking repeat gap
     */
    private final Map<String, Map<String, GtidSet>> mhaDbGtidSet = new HashMap<>();

    /**
     * key: uuid, value: a list of last GapInterval
     * for log use
     */
    private final Map<String, Map<String, List<GapInterval>>> dbUuidGapIntervalsMapper = new ConcurrentHashMap<>();

    @Autowired
    private DataCenterService dataCenterService;

    public void checkDbGtidGap(Map<String, Map<String, Set<String>>> uuidMapper, Map<MetaKey, MySqlEndpoint> masterMySQLEndpointMap) {
        String localDcName = dataCenterService.getDc();
        // traverse the local master db to check GTID gaps
        for (Map.Entry<MetaKey, MySqlEndpoint> entry : masterMySQLEndpointMap.entrySet()) {
            String mhaName = entry.getKey().getMhaName();
            Map<String, Set<String>> dbUuidMap = uuidMapper.get(mhaName);
            if (dbUuidMap == null) {
                continue;
            }
            Endpoint mySqlMasterEndpoint = entry.getValue();
            for (Map.Entry<String, Set<String>> dbUuidEntry : dbUuidMap.entrySet()) {
                String dbName = dbUuidEntry.getKey();
                Set<String> uuidSet = dbUuidEntry.getValue();
                ArrayList<GtidReader> gtidReaderList = Lists.newArrayList(new ShowMasterGtidReader(), new DbTransactionTableGtidReader(mySqlMasterEndpoint, dbName));
                ListenableFuture<String> listenableFuture = gtidCheckExecutorService.submit(new ExecutedGtidQueryTask(mySqlMasterEndpoint, gtidReaderList));
                Futures.addCallback(listenableFuture, new FutureCallback<>() {
                    @Override
                    public void onSuccess(String gtidSetStr) {
                        GtidSet curGtidSet = new GtidSet(gtidSetStr);
                        // for each mha, check all the uuid which is not from the local db
                        for (String uuid : curGtidSet.getUUIDs()) {
                            if (uuidSet.contains(uuid)) {
                                // if the uuid in local master db's Executed_Gtid_Set is in the uuidMapper, it must come from other idc
                                // report gap count
                                MDC.put("mhaName", mhaName);
                                final List<GtidSet.Interval> intervals = curGtidSet.getUUIDSet(uuid).getIntervals();
                                long gapCount = getGapCount(intervals);
                                GtidGapEntity gtidGapEntity = new GtidGapEntity.Builder()
                                        .clusterAppId(null)
                                        .buName(BU)
                                        .dcName(localDcName)
                                        .mha(mhaName)
                                        .db(dbName)
                                        .mysqlIp(mySqlMasterEndpoint.getHost())
                                        .mysqlPort(mySqlMasterEndpoint.getPort())
                                        .uuid(uuid)
                                        .build();
                                reporter.reportDbGtidGapCount(gtidGapEntity, gapCount);
                                CONSOLE_GTID_LOGGER.info("[[monitor=dbGtid,dc={},mha={},db={},dbEndpoint={}:{}]]\n [GTID] {}\n [Report GTID Gap Count] mysql {}:{} uuid : {} of gapCount: {}",
                                        localDcName, mhaName, dbName, mySqlMasterEndpoint.getHost(), mySqlMasterEndpoint.getPort(), getGtid(uuid, intervals), mySqlMasterEndpoint.getHost(), mySqlMasterEndpoint.getPort(), uuid, gapCount);

                                GtidSet lastGtidSet = Optional.ofNullable(mhaDbGtidSet.get(mhaName)).map(e -> e.get(dbName)).orElse(null);
                                reportRepeatedGtidGap(curGtidSet, uuid, gtidGapEntity, lastGtidSet);
                                MDC.remove("mhaName");
                            }
                            mhaDbGtidSet.computeIfAbsent(mhaName, k -> Maps.newHashMap()).put(dbName, curGtidSet);
                        }
                        CONSOLE_GTID_LOGGER.info("put current gtidset for {}, with: {}", mhaName, curGtidSet);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        String msg = String.format("[[monitor=dbGtid,dc=%s,mha=%s,db=%s,dbEndpoint=%s:%d]][Query] new master executed gtid error: ", localDcName, mhaName, dbName, mySqlMasterEndpoint.getHost(), mySqlMasterEndpoint.getPort());
                        CONSOLE_GTID_LOGGER.error(msg, t);
                    }
                });
            }
        }
    }

    private void reportRepeatedGtidGap(GtidSet curGtidSet, String uuid, GtidGapEntity gtidGapEntity, GtidSet lastGtidSet) {
        long count = getRepeatedGtidGap(gtidGapEntity, uuid, curGtidSet, lastGtidSet);
        reporter.reportDbGtidGapRepeat(gtidGapEntity, count);
    }

    protected GtidSet getCurGtidSetAfterFilter(String gtidSetStr, String gtidFilterSetStr) {
        GtidSet gtidSet = new GtidSet(gtidSetStr);
        GtidSet filterGtidSet = new GtidSet(gtidFilterSetStr);
        Set<String> filterUuiDs = filterGtidSet.getUUIDs();

        Map<String, GtidSet.UUIDSet> realFilterUuidSets = new ConcurrentHashMap<>();
        for (String filterUuid : filterUuiDs) {

            if (gtidSet.getUUIDs().contains(filterUuid)) {
                GtidSet.UUIDSet realFilterUuidSet = getRealFilterUuidSet(gtidSet, filterGtidSet, filterUuid);
                if (null != realFilterUuidSet) {
                    realFilterUuidSets.put(filterUuid, realFilterUuidSet);
                }
            }
        }
        GtidSet realFilterGtidSet = new GtidSet(realFilterUuidSets);
        return gtidSet.subtract(realFilterGtidSet);
    }

    protected GtidSet.UUIDSet getRealFilterUuidSet(GtidSet gtidSetSet, GtidSet filterGtidSet, String filterUuid) {
        Long maxTxIdAfterSubtraction = getMaxTransactionId(gtidSetSet.subtract(filterGtidSet), filterUuid);
        List<GtidSet.Interval> realFilterIntervals = Lists.newArrayList();
        for (GtidSet.Interval filterInterval : filterGtidSet.getUUIDSet(filterUuid).getIntervals()) {
            long start = filterInterval.getStart();
            long end = filterInterval.getEnd();
            if (start > maxTxIdAfterSubtraction + 1) {
                realFilterIntervals.add(new GtidSet.Interval(start, end));
            }
        }
        if (realFilterIntervals.size() != 0) {
            return new GtidSet.UUIDSet(filterUuid, realFilterIntervals);
        }
        return null;
    }

    protected String getGtid(String uuid, List<GtidSet.Interval> intervals) {
        StringBuilder sb = new StringBuilder();
        sb.append(uuid);
        for (GtidSet.Interval interval : intervals) {
            sb.append(":" + interval.getStart() + "-" + interval.getEnd());
        }
        return sb.toString();
    }

    protected long getGapCount(List<GtidSet.Interval> intervals) {
        int size = intervals.size();
        if (size < 2) {
            return 0;
        }
        long gapCount = 0;
        for (int i = 0; i < size - 1; ++i) {
            gapCount += (intervals.get(i + 1).getStart() - intervals.get(i).getEnd() - 1);
        }
        return gapCount;
    }

    /**
     * report repeated gap for a certain uuid in other db for current mha
     * log the gap intervals if there is gap
     * and also log the last gap intervals and repeated gap intervals if there is repeated transactions
     */
    protected long getRepeatedGtidGap(GtidGapEntity gtidGapEntity, String uuid, GtidSet curGtidSet, GtidSet lastGtidSet) {
        List<GtidSet.Interval> intervals = curGtidSet.getUUIDSet(uuid).getIntervals();
        String dcName = gtidGapEntity.getDcName();
        String mha = gtidGapEntity.getMhaName();
        String db = gtidGapEntity.getDbName();
        // last uuid and Gap mapper
        List<GapInterval> curGapIntervals = getGapIntervals(intervals);
        List<GapInterval> gapIntervals = Optional.ofNullable(dbUuidGapIntervalsMapper.get(db)).map(e -> e.get(uuid)).orElse(null);
        dbUuidGapIntervalsMapper.computeIfAbsent(uuid, k -> Maps.newConcurrentMap()).put(uuid, curGapIntervals);

        if (CollectionUtils.isEmpty(curGapIntervals)) {
            // since there is no gap in current Gtid, no need to check the repeat gap
            CONSOLE_GTID_LOGGER.info("[[monitor=dbGtid,dc={},mha={},db={},dbEndpoint={}:{}]]\n [Report RepeatGap] uuid : {} has no repeated gap", dcName, mha, db, gtidGapEntity.getIp(), gtidGapEntity.getPort(), uuid);
            return NON_REPEAT_COUNT;
        }
        if (lastGtidSet == null) {
            // if this is the first time check the gap, there is no repeat gap
            CONSOLE_GTID_LOGGER.info("[[monitor=dbGtid,dc={},mha={},db={},dbEndpoint={}:{}]]\n [Report RepeatGap] uuid : {} has no repeated gap", dcName, mha, db, gtidGapEntity.getIp(), gtidGapEntity.getPort(), uuid);
            return NON_REPEAT_COUNT;
        }

        // check if current gap has existed since last task
        CONSOLE_GTID_LOGGER.info("[[uuid={}]]lastGtidSet : {}, curGapIntervals: {}", uuid, lastGtidSet, curGapIntervals);
        List<GapInterval> repeatedGapIntervals = getRepeatedGapInterval(uuid, curGapIntervals, lastGtidSet);
        if (CollectionUtils.isEmpty(repeatedGapIntervals)) {
            CONSOLE_GTID_LOGGER.info("[[monitor=db_tid,dc={},mha={},db={},dbEndpoint={}:{}]]\n" +
                    " [Report current GTID Gap] mysql {}:{} uuid : {} of gap: {}\n" +
                    " [Report Repeat Gap] uuid : {} has no repeated gap", dcName, mha, db, gtidGapEntity.getIp(), gtidGapEntity.getPort(), gtidGapEntity.getIp(), gtidGapEntity.getPort(), uuid, curGapIntervals, uuid);
            return NON_REPEAT_COUNT;
        }

        CONSOLE_GTID_LOGGER.info("[[monitor=dbGtid,dc={},mha={},db={},dbEndpoint={}:{}]]\n" +
                        " [Report last GTID Gap] mysql {}:{} uuid : {} of gap: {}\n" +
                        " [Report current GTID Gap] mysql {}:{} uuid : {} of gap: {}\n" +
                        " [Report RepeatGap] uuid : {} has repeated transactions: {}",
                dcName, mha, db, gtidGapEntity.getIp(), gtidGapEntity.getPort(),
                gtidGapEntity.getIp(), gtidGapEntity.getPort(), uuid, gapIntervals,
                gtidGapEntity.getIp(), gtidGapEntity.getPort(), uuid, curGapIntervals,
                uuid, repeatedGapIntervals);
        return getRepeatedGapCount(repeatedGapIntervals);
    }

    protected List<GapInterval> getGapIntervals(List<GtidSet.Interval> intervals) {
        List<GapInterval> gapIntervals = new LinkedList<>();
        if (intervals.size() > 1) {
            for (int i = 0; i < intervals.size() - 1; ++i) {
                long before = intervals.get(i).getEnd();
                long after = intervals.get(i + 1).getStart();
                gapIntervals.add(new GapInterval(before + 1, after - 1));
            }
        }
        return gapIntervals;
    }

    /**
     * @return gtid in curGapIntervals, and not in lastGtidSet (limited by last max tx id)
     */
    protected List<GapInterval> getRepeatedGapInterval(String uuid, List<GapInterval> curGapIntervals, GtidSet lastGtidSet) {
        long maxTransactionId = getMaxTransactionId(lastGtidSet, uuid);
        GtidSet curGapGtid = convertToGtid(uuid, curGapIntervals);
        GtidSet subtract = curGapGtid.subtract(lastGtidSet);
        return subtract.getUUIDSet(uuid).getIntervals()
                .stream().filter(e -> e.getStart() < maxTransactionId)
                .map(e -> new GapInterval(e.getStart(), Math.min(e.getEnd(), maxTransactionId)))
                .collect(Collectors.toList());
    }


    private GtidSet convertToGtid(String uuid, List<GapInterval> curGapIntervals) {
        List<GtidSet.Interval> list = curGapIntervals.stream().map(e -> new GtidSet.Interval(e.start, e.end)).collect(Collectors.toList());
        Map<String, GtidSet.UUIDSet> uuidSets = new HashMap<>();
        uuidSets.put(uuid, new GtidSet.UUIDSet(uuid, list));
        return new GtidSet(uuidSets);
    }

    protected long getRepeatedGapCount(List<GapInterval> gapIntervals) {
        long sum = 0L;
        for (GapInterval gapInterval : gapIntervals) {
            sum += (gapInterval.end - gapInterval.start + 1);
        }
        return sum;
    }

    protected Long getMaxTransactionId(GtidSet gtidSet, String uuid) {
        CONSOLE_GTID_LOGGER.info("[[uuid={}]]gtid: {}", uuid, gtidSet);
        Set<String> uuids = gtidSet.getUUIDs();
        CONSOLE_GTID_LOGGER.info("[[uuid={}]]gtidSet uuids: {}", uuid, uuids);
        if (!uuids.contains(uuid)) {
            return null;
        }
        GtidSet.UUIDSet uuidSet = gtidSet.getUUIDSet(uuid);
        CONSOLE_GTID_LOGGER.info("[[uuid={}]]gtidSet uuidSet: {}", uuid, uuidSet);
        List<GtidSet.Interval> intervals = uuidSet.getIntervals();
        CONSOLE_GTID_LOGGER.info("[[uuid={}]]gtidSet intervals: {}", uuid, intervals);
        return intervals.get(intervals.size() - 1).getEnd();
    }

    public void resourcesRelease() {
        mhaDbGtidSet.clear();
        dbUuidGapIntervalsMapper.clear();
        DefaultReporterHolder.getInstance().removeRegister("fx.drc.db.gtid.gap.count");
        DefaultReporterHolder.getInstance().removeRegister("fx.drc.db.gtid.gap.repeat");
    }
}
