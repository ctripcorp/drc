package com.ctrip.framework.drc.console.monitor.gtid.function;

import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.pojo.MetaKey;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.monitor.entity.GtidGapEntity;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.core.monitor.reporter.Reporter;
import com.ctrip.framework.drc.core.driver.healthcheck.task.ExecutedGtidQueryTask;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.xpipe.api.config.Config;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.config.AbstractConfigBean;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.*;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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
public class CheckGtid extends AbstractConfigBean {

    private Reporter reporter = DefaultReporterHolder.getInstance();

    private ListeningExecutorService gtidCheckExecutorService = MoreExecutors.listeningDecorator(ThreadUtils.newFixedThreadPool(PROCESSORS_SIZE, "PeriodicalCheckGtid"));

    private static final String MHA_FILTER_GTID = "filter.gtid.%s";

    private static final long NON_REPEAT_COUNT = 0L;

    public CheckGtid() {}

    public CheckGtid(Config config) {
        super(config);
    }

    /**
     * key: mha, value: corresponding GtidSet from last check
     * for checking repeat gap
     */
    private Map<String, GtidSet> mhaGtidSet = new HashMap<>();

    /**
     * key: uuid, value: a list of last GapInterval
     * for log use
     */
    private Map<String, List<GapInterval>> uuidGapIntervalsMapper = new ConcurrentHashMap<>();

    @Autowired
    private DbClusterSourceProvider dbClusterSourceProvider;

    /**
     * check GtidSet from mha's master db in local dc,
     * then check and report the GTID Gap for which the UUID is from master db in other dc
     * @param uuidMapper
     */
    public void checkGtidGap(Map<String, Set<String>> uuidMapper, Map<MetaKey, MySqlEndpoint> masterMySQLEndpointMap) {

        String localDcName = dbClusterSourceProvider.getLocalDcName();
        // traverse the local master db to check GTID gaps
        for (Map.Entry<MetaKey, MySqlEndpoint> entry : masterMySQLEndpointMap.entrySet()) {
            String mhaName = entry.getKey().getMhaName();
            Endpoint mySqlMasterEndpoint = entry.getValue();
            String gtidFilterSetStr = getProperty(String.format(MHA_FILTER_GTID, mhaName), "");
            ListenableFuture<String> listenableFuture = gtidCheckExecutorService.submit(new ExecutedGtidQueryTask(mySqlMasterEndpoint));
            Futures.addCallback(listenableFuture, new FutureCallback<>() {
                @Override
                public void onSuccess(String gtidSetStr) {

                    GtidSet curGtidSet = getCurGtidSetAfterFilter(gtidSetStr, gtidFilterSetStr);
                    /**
                     * for each mha, check all the uuid which is not from the local db
                     */
                    for(String uuid : curGtidSet.getUUIDs()) {

                        Set<String> uuidSet;
                        if((uuidSet = uuidMapper.get(mhaName)) != null && uuidSet.contains(uuid)) {
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
                                    .mysqlIp(mySqlMasterEndpoint.getHost())
                                    .mysqlPort(mySqlMasterEndpoint.getPort())
                                    .uuid(uuid)
                                    .build();
                            reporter.reportGtidGapCount(gtidGapEntity, gapCount);
                            CONSOLE_GTID_LOGGER.info("[[monitor=gtid,dc={},mha={},db={}:{}]]\n [GTID] {}\n [Report GTID Gap Count] mysql {}:{} uuid : {} of gapCount: {}", 
                                    localDcName, mhaName, mySqlMasterEndpoint.getHost(), mySqlMasterEndpoint.getPort(), getGtid(uuid, intervals), mySqlMasterEndpoint.getHost(), mySqlMasterEndpoint.getPort(), uuid, gapCount);

                            reportRepeatedGtidGap(gtidGapEntity, uuid, intervals);
                            mhaGtidSet.put(mhaName, curGtidSet);
                            MDC.remove("mhaName");
                        }
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    CONSOLE_GTID_LOGGER.error("[[monitor=gtid,dc={},mha={},db={}:{}]][Query] new master executed gtid error: ", localDcName, mhaName, mySqlMasterEndpoint.getHost(), mySqlMasterEndpoint.getPort(), t);
                }
            });
        }
    }

    protected GtidSet getCurGtidSetAfterFilter(String gtidSetStr, String gtidFilterSetStr) {
        GtidSet gtidSet = new GtidSet(gtidSetStr);
        GtidSet filterGtidSet = new GtidSet(gtidFilterSetStr);
        Set<String> filterUuiDs = filterGtidSet.getUUIDs();

        Map<String, GtidSet.UUIDSet> realFilterUuidSets = new ConcurrentHashMap<>();
        for(String filterUuid : filterUuiDs) {

            if(gtidSet.getUUIDs().contains(filterUuid)) {
                GtidSet.UUIDSet realFilterUuidSet = getRealFilterUuidSet(gtidSet, filterGtidSet, filterUuid);
                if(null != realFilterUuidSet) {
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
        for(GtidSet.Interval filterInterval : filterGtidSet.getUUIDSet(filterUuid).getIntervals()) {
            long start = filterInterval.getStart();
            long end = filterInterval.getEnd();
            if(start > maxTxIdAfterSubtraction + 1) {
                realFilterIntervals.add(new GtidSet.Interval(start, end));
            }
        }
        if(realFilterIntervals.size() != 0) {
            return new GtidSet.UUIDSet(filterUuid, realFilterIntervals);
        }
        return null;
    }

    protected String getGtid(String uuid, List<GtidSet.Interval> intervals) {
        StringBuilder sb = new StringBuilder();
        sb.append(uuid);
        for(GtidSet.Interval interval : intervals) {
            sb.append(":"+interval.getStart()+"-"+interval.getEnd());
        }
        return sb.toString();
    }

    protected long getGapCount(List<GtidSet.Interval> intervals) {
        int size = intervals.size();
        if(size < 2) {
            return 0;
        }
        long gapCount = 0;
        for(int i = 0; i < size - 1; ++i) {
            gapCount += (intervals.get(i+1).getStart() - intervals.get(i).getEnd() - 1);
        }
        return gapCount;
    }

    /**
     * @param gtidGapEntity
     * @param uuid
     * @param intervals the uuid's intervals in current GtidSet
     * report repeated gap for a certain uuid in other db for current mha
     * log the gap intervals if there is gap
     * and also log the last gap intervals and repeated gap intervals if there is repeated transactions
     */
    protected void reportRepeatedGtidGap(GtidGapEntity gtidGapEntity, String uuid, List<GtidSet.Interval> intervals) {
        String dcName = gtidGapEntity.getDcName();
        String mha = gtidGapEntity.getMhaName();
        // last gtidSet
        GtidSet lastGtidSet = mhaGtidSet.get(mha);
        // last uuid and Gap mapper
        List<GapInterval> gapIntervals = uuidGapIntervalsMapper.get(uuid);
        List<GapInterval> curGapIntervals = getGapIntervals(intervals);
        if (curGapIntervals.size() == 0) {
            // since there is no gap in current Gtid, no need to check the repeat gap
            reporter.reportGtidGapRepeat(gtidGapEntity, NON_REPEAT_COUNT);
            CONSOLE_GTID_LOGGER.info("[[monitor=gtid,dc={},mha={},db={}:{}]]\n [Report RepeatGap] uuid : {} has no repeated gap", dcName, mha, gtidGapEntity.getIp(), gtidGapEntity.getPort(), uuid);
        } else {
            // there is gap in current Gtid
            if(null == lastGtidSet) {
                // if this is the first time check the gap, there is no repeat gap
                reporter.reportGtidGapRepeat(gtidGapEntity, NON_REPEAT_COUNT);
                CONSOLE_GTID_LOGGER.info("[[monitor=gtid,dc={},mha={},db={}:{}]]\n [Report RepeatGap] uuid : {} has no repeated gap", dcName, mha, gtidGapEntity.getIp(), gtidGapEntity.getPort(), uuid);
            } else {
                // if this is not the first time check the gap
                // check repeat gap logic:
                //      add the current Gtid Gap's transaction id into previous gtidSet
                //      if succeed, we can prove that there is gap repeated between the previous and current GtidSet
                CONSOLE_GTID_LOGGER.info("[[uuid={}]]lastGtidSet : {}, curGapIntervals: {}", uuid, lastGtidSet, curGapIntervals);
                List<GapInterval> repeatedGapIntervals = getRepeatedGapInterval(uuid, curGapIntervals, lastGtidSet);
                Long repeatedGapCount = getRepeatedGapCount(repeatedGapIntervals);
                if(repeatedGapIntervals.size() == 0) {
                    reporter.reportGtidGapRepeat(gtidGapEntity, NON_REPEAT_COUNT);
                    CONSOLE_GTID_LOGGER.info("[[monitor=gtid,dc={},mha={},db={}:{}]]\n [Report current GTID Gap] mysql {}:{} uuid : {} of gap: {}\n [Report Repeat Gap] uuid : {} has no repeated gap", dcName, mha, gtidGapEntity.getIp(), gtidGapEntity.getPort(), gtidGapEntity.getIp(), gtidGapEntity.getPort(), uuid, curGapIntervals, uuid);
                } else {
                    CONSOLE_GTID_LOGGER.info("[[monitor=gtid,dc={},mha={},db={}:{}]]\n [Report last GTID Gap] mysql {}:{} uuid : {} of gap: {}\n [Report current GTID Gap] mysql {}:{} uuid : {} of gap: {}\n [Report RepeatGap] uuid : {} has repeated transactions: {}", dcName, mha, gtidGapEntity.getIp(), gtidGapEntity.getPort(), gtidGapEntity.getIp(), gtidGapEntity.getPort(), uuid, gapIntervals, gtidGapEntity.getIp(), gtidGapEntity.getPort(), uuid, curGapIntervals, uuid, repeatedGapIntervals);
                    reporter.reportGtidGapRepeat(gtidGapEntity, repeatedGapCount);
                }
            }
        }
        uuidGapIntervalsMapper.put(uuid, curGapIntervals);
    }

    protected List<GapInterval> getGapIntervals(List<GtidSet.Interval> intervals) {
        List<GapInterval> gapIntervals = new LinkedList<>();
        if(intervals.size() > 1) {
            for(int i = 0; i < intervals.size() - 1; ++i) {
                long before = intervals.get(i).getEnd();
                long after = intervals.get(i+1).getStart();
                gapIntervals.add(new GapInterval(before+1, after-1));
            }
        }
        return gapIntervals;
    }

    /**
     * @param uuid
     * @param curGapIntervals
     * @param lastGtidSet
     * @return
     */
    protected List<GapInterval> getRepeatedGapInterval(String uuid, List<GapInterval> curGapIntervals, GtidSet lastGtidSet) {

        List<GapInterval> repeatedGapIntervals = Lists.newArrayList();
        long maxTransactionId = getMaxTransactionId(lastGtidSet, uuid);
        Long repeatedGapStart = null, repeatedGapEnd = null;

        for(GapInterval gapInterval : curGapIntervals) {
            long start = gapInterval.getStart();
            long end = gapInterval.getEnd();
            for(long transactionId = start; transactionId <= end; ++transactionId) {
                if(transactionId >= maxTransactionId) {
                    if(null != repeatedGapEnd) {
                        repeatedGapIntervals.add(new GapInterval(repeatedGapStart, repeatedGapEnd));
                    }
                    return repeatedGapIntervals;
                }
                String gtid = uuid+":"+transactionId;
                if(lastGtidSet.add(gtid)) {
                    if(null == repeatedGapStart) {
                        // first time find the repeat tx id
                        repeatedGapStart = transactionId;
                        repeatedGapEnd = transactionId;
                    } else {
                        if(transactionId - repeatedGapEnd > 1) {
                            // if repeat tx id jump over at least one
                            // 1. make the GapInterval since
                            // 2. reset the repeatedGapStart
                            repeatedGapIntervals.add(new GapInterval(repeatedGapStart, repeatedGapEnd));
                            repeatedGapStart = transactionId;
                        }
                        repeatedGapEnd = transactionId;
                    }
                }
            }
        }
        if(null != repeatedGapStart) {
            // add the last one
            repeatedGapIntervals.add(new GapInterval(repeatedGapStart, repeatedGapEnd));
        }
        return repeatedGapIntervals;
    }

    protected long getRepeatedGapCount(List<GapInterval> gapIntervals) {
        long sum = 0L;
        for(GapInterval gapInterval : gapIntervals) {
            sum += (gapInterval.end-gapInterval.start+1);
        }
        return sum;
    }

    protected Long getMaxTransactionId(GtidSet gtidSet, String uuid) {
        CONSOLE_GTID_LOGGER.info("[[uuid={}]]gtid: {}", uuid, gtidSet);
        Set<String> uuids = gtidSet.getUUIDs();
        CONSOLE_GTID_LOGGER.info("[[uuid={}]]gtidSet uuids: {}", uuid, uuids);
        if(!uuids.contains(uuid)) {
            return null;
        }
        GtidSet.UUIDSet uuidSet = gtidSet.getUUIDSet(uuid);
        CONSOLE_GTID_LOGGER.info("[[uuid={}]]gtidSet uuidSet: {}", uuid, uuidSet);
        List<GtidSet.Interval> intervals = uuidSet.getIntervals();
        CONSOLE_GTID_LOGGER.info("[[uuid={}]]gtidSet intervals: {}", uuid, intervals);
        return intervals.get(intervals.size() - 1).getEnd();
    }
    
    public void resourcesRelease() {
        mhaGtidSet.clear();
        uuidGapIntervalsMapper.clear();
        DefaultReporterHolder.getInstance().removeRegister("fx.drc.gtid.gap.count");
        DefaultReporterHolder.getInstance().removeRegister("fx.drc.gtid.gap.repeat");
    }

    /**
     * An interval of contiguous gap transaction identifiers.
     */
    public static final class GapInterval {

        private long start;
        private long end;

        public GapInterval(long start, long end) {
            this.start = start;
            this.end = end;
        }

        /**
         * Get the starting transaction number in this interval.
         * @return this interval's first transaction number
         */
        public long getStart() {
            return start;
        }

        /**
         * Get the ending transaction number in this interval.
         * @return this interval's last transaction number
         */
        public long getEnd() {
            return end;
        }

        @Override
        public String toString() {
            return start + "-" + end;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof GapInterval)) return false;
            GapInterval that = (GapInterval) o;
            return getStart() == that.getStart() &&
                    getEnd() == that.getEnd();
        }

        @Override
        public int hashCode() {
            return Objects.hash(getStart(), getEnd());
        }
    }
}
