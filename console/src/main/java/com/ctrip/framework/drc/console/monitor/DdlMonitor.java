package com.ctrip.framework.drc.console.monitor;

import com.ctrip.framework.drc.console.dao.entity.DdlHistoryTbl;
import com.ctrip.framework.drc.console.dao.entity.MhaTbl;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.service.impl.MetaGenerator;
import com.ctrip.framework.drc.console.service.impl.MetaInfoServiceImpl;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.core.driver.binlog.constant.QueryType;
import com.ctrip.framework.drc.core.monitor.entity.UnidirectionalEntity;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.core.monitor.reporter.Reporter;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.StatementParameters;
import com.ctrip.platform.dal.dao.sqlbuilder.FreeUpdateSqlBuilder;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.SWITCH_STATUS_ON;
import static com.ctrip.framework.drc.core.monitor.enums.MeasurementEnum.TRUNCATE_CONSISTENCY_MEASUREMENT;

@Component
public class DdlMonitor extends AbstractMonitor implements Monitor {

    @Autowired
    private MetaInfoServiceImpl metaInfoService;

    @Autowired
    private MetaGenerator metaGenerator;

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;

    private DalUtils dalUtils = DalUtils.getInstance();

    private Reporter reporter = DefaultReporterHolder.getInstance();

    private ExecutorService alertExecutorService = ThreadUtils.newSingleThreadExecutor(getClass().getSimpleName() + "-truncateInconsistencyAlert");

    protected Map<DdlMonitorItem, AtomicInteger> truncateInconsistencyCounter = Maps.newConcurrentMap();

    public static final int DDL_MONITOR_INITIAL_DELAY = 60;// wait for MetaServiceImpl to run first scheduleTask

    public static final int DDL_MONITOR_PERIOD = 20;

    public static final int ALERT_TIME_THRESHOLD = 60;

    public static final int ALERT_ROUND_THRESHOLD = ALERT_TIME_THRESHOLD/DDL_MONITOR_PERIOD;

    public static final int CONSISTENT_TRUNCATE_SET_SIZE = 1;

    public static final String CLEAN_SQL = "delete from ddl_history_tbl where datachange_lasttime < date_sub(CURRENT_TIMESTAMP(3), interval %s hour);";

    @Override
    public void initialize() {
        setInitialDelay(DDL_MONITOR_INITIAL_DELAY);
        setPeriod(DDL_MONITOR_PERIOD);
        setTimeUnit(TimeUnit.SECONDS);
    }

    @Override
    public void scheduledTask() {
        if(SWITCH_STATUS_ON.equalsIgnoreCase(monitorTableSourceProvider.getTruncateConsistentMonitorSwitch())) {
            try {
                Map<DdlMonitorItem, Map<String, AtomicInteger>> allItemTruncateHistory = buildTruncateHistoryMap();
                Set<DdlMonitorItem> inconsistentItem = checkTruncateInconsistency(allItemTruncateHistory);
                compareAndAlertInconsistenctTruncate(inconsistentItem);
            } catch (Exception e) {
                logger.error("Fail to monitor truncate history, will try next round.", e);
            }
            try {
                int cleanRows = cleanTask();
                logger.info("[clean] ddl_history_tbl data 1 day before now, affected rows: {}", cleanRows);
            } catch (SQLException e) {
                logger.error("Fail to clean ddl_history_tbl data 1 day before now.", e);
            }
        }
    }

    @VisibleForTesting
    protected Map<DdlMonitorItem, Map<String, AtomicInteger>> buildTruncateHistoryMap() throws SQLException {
        // inner map: key(mhaName), value(count of truncate in this mha)
        Map<DdlMonitorItem, Map<String, AtomicInteger>> allItemTruncateHistory = Maps.newHashMap();
        List<DdlHistoryTbl> ddlHistoryTbls = metaGenerator.getDdlHistoryTbls();
        List<MhaTbl> mhaTbls = metaGenerator.getMhaTbls();
        for(DdlHistoryTbl ddlHistoryTbl : ddlHistoryTbls) {
            if(QueryType.TRUNCATE.ordinal() == ddlHistoryTbl.getQueryType()) {
                Long mhaId = ddlHistoryTbl.getMhaId();
                String mhaName = mhaTbls.stream().filter(p -> mhaId.equals(p.getId())).map(MhaTbl::getMhaName).findFirst().orElse(null);
                String cluster = metaInfoService.getCluster(mhaName);
                Long mhaGroupId = metaInfoService.getMhaGroupId(mhaId);
                List<MhaTbl> mhaTblsInGroup = metaInfoService.getMhaTbls(mhaGroupId);
                List<String> mhaNames = mhaTblsInGroup.stream().map(MhaTbl::getMhaName).collect(Collectors.toList());
                DdlMonitorItem ddlMonitorItem = new DdlMonitorItem(mhaGroupId, ddlHistoryTbl.getSchemaName(), ddlHistoryTbl.getTableName(), cluster, mhaNames, ddlHistoryTbl.getQueryType());
                allItemTruncateHistory.putIfAbsent(ddlMonitorItem, Maps.newHashMap());
                allItemTruncateHistory.get(ddlMonitorItem).putIfAbsent(mhaName, new AtomicInteger(0));
                allItemTruncateHistory.get(ddlMonitorItem).get(mhaName).incrementAndGet();
            }
        }
        return allItemTruncateHistory;
    }

    protected Set<DdlMonitorItem> checkTruncateInconsistency(Map<DdlMonitorItem, Map<String, AtomicInteger>> allItemTruncateHistory) {
        Set<DdlMonitorItem> inconsistentItem = Sets.newHashSet();
        for(Map.Entry<DdlMonitorItem, Map<String, AtomicInteger>> truncateHistory : allItemTruncateHistory.entrySet()) {
            DdlMonitorItem ddlMonitorItem = truncateHistory.getKey();
            Map<String, AtomicInteger> truncateCountForAllMhas = truncateHistory.getValue();
            String truncateInfo = ddlMonitorItem.toString() + truncateCountForAllMhas.toString();
            if(ddlMonitorItem.getMhaNames().size() != truncateCountForAllMhas.size()) {
                logger.info(truncateInfo + "[INCONSISTENT TRUNCATE]");
                inconsistentItem.add(ddlMonitorItem);
            } else {
                Set<Integer> truncateCountSet = Sets.newHashSet();
                for(Map.Entry<String, AtomicInteger> truncateCountInMha : truncateCountForAllMhas.entrySet()) {
                    AtomicInteger truncateCount = truncateCountInMha.getValue();
                    truncateCountSet.add(truncateCount.get());
                }
                if(CONSISTENT_TRUNCATE_SET_SIZE != truncateCountSet.size()) {
                    logger.info(truncateInfo + "[INCONSISTENT TRUNCATE]");
                    inconsistentItem.add(ddlMonitorItem);
                } else {
                    logger.info(truncateInfo + "[CONSISTENT TRUNCATE]");
                }
            }
        }
        return inconsistentItem;
    }

    // return: count of inconsistency alert triggerred
    protected int compareAndAlertInconsistenctTruncate(Set<DdlMonitorItem> inconsistentItem) {
        // clear the consistent ones
        int inconsistentAlertCount = 0;
        truncateInconsistencyCounter.entrySet().removeIf(entry -> !inconsistentItem.contains(entry.getKey()));
        for(DdlMonitorItem ddlMonitorItem : inconsistentItem) {
            truncateInconsistencyCounter.putIfAbsent(ddlMonitorItem, new AtomicInteger(0));
            int count = truncateInconsistencyCounter.get(ddlMonitorItem).incrementAndGet();
            if(count >= ALERT_ROUND_THRESHOLD) {
                doAlert(ddlMonitorItem);
                inconsistentAlertCount++;
            }
        }
        return inconsistentAlertCount;
    }

    private void doAlert(DdlMonitorItem ddlMonitorItem) {
        logger.info(ddlMonitorItem.toString() + "alert truncate inconsistency");
        alertExecutorService.submit(new Runnable() {
            @Override
            public void run() {
                DefaultEventMonitorHolder.getInstance().logEvent("DRC.console.truncate.inconsistency", StringUtils.join(ddlMonitorItem.getMhaNames(), '.') + "_" + ddlMonitorItem.getSchema() + '.' + ddlMonitorItem.getTable());
                UnidirectionalEntity entity = new UnidirectionalEntity.Builder()
                        .clusterName(ddlMonitorItem.getCluster())
                        .srcMhaName(ddlMonitorItem.getMhaNames().get(0))
                        .destMhaName(ddlMonitorItem.getMhaNames().get(1))
                        .build();
                reporter.reportResetCounter(entity.getTags(), 1L, TRUNCATE_CONSISTENCY_MEASUREMENT.getMeasurement());
            }
        });
    }

    private int cleanTask() throws SQLException {
        FreeUpdateSqlBuilder builder = new FreeUpdateSqlBuilder();
        String sql = String.format(CLEAN_SQL, monitorTableSourceProvider.getCleanTableDeviation());
        builder.setTemplate(sql);
        StatementParameters parameters = new StatementParameters();
        return dalUtils.getDalQueryDao().update(builder, parameters, new DalHints());
    }

    public static final class DdlMonitorItem {
        private Long mhaGroupId;
        private String schema;
        private String table;
        private String cluster;
        private List<String> mhaNames;
        private int queryType;

        public DdlMonitorItem(Long mhaGroupId, String schema, String table, String cluster, List<String> mhaNames, int queryType) {
            this.mhaGroupId = mhaGroupId;
            this.schema = schema;
            this.table = table;
            this.cluster = cluster;
            this.mhaNames = mhaNames;
            this.queryType = queryType;
        }

        public String getCluster() {
            return cluster;
        }

        public Long getMhaGroupId() {
            return mhaGroupId;
        }

        public String getSchema() {
            return schema;
        }

        public String getTable() {
            return table;
        }

        public List<String> getMhaNames() {
            return mhaNames;
        }

        public int getQueryType() {
            return queryType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DdlMonitorItem that = (DdlMonitorItem) o;
            return queryType == that.queryType && Objects.equals(mhaGroupId, that.mhaGroupId) && Objects.equals(schema, that.schema) && Objects.equals(table, that.table) && Objects.equals(cluster, that.cluster) && Objects.equals(mhaNames, that.mhaNames);
        }

        @Override
        public int hashCode() {
            return Objects.hash(mhaGroupId, schema, table, cluster, mhaNames, queryType);
        }

        @Override
        public String toString() {
            return "[[monitor=truncate, schema=" + schema + ",table=" +  table + ",cluster=" + cluster + ",mhaCount=" + mhaNames.size() + ",queryType=" + queryType + "]]";
        }
    }
}
