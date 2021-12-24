package com.ctrip.framework.drc.console.monitor.consistency.instance;

import com.ctrip.framework.drc.console.dao.entity.DataInconsistencyHistoryTbl;
import com.ctrip.framework.drc.console.dao.entity.MhaTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.SourceTypeEnum;
import com.ctrip.framework.drc.console.monitor.consistency.sql.operator.StreamSqlOperatorWrapper;
import com.ctrip.framework.drc.console.monitor.consistency.table.DefaultTableNameDescriptor;
import com.ctrip.framework.drc.console.monitor.consistency.table.TableNameDescriptor;
import com.ctrip.framework.drc.console.monitor.consistency.task.KeyedQueryTask;
import com.ctrip.framework.drc.console.monitor.consistency.task.RangeQueryTask;
import com.ctrip.framework.drc.console.monitor.delay.config.DelayMonitorConfig;
import com.ctrip.framework.drc.core.service.utils.Constants;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.core.monitor.entity.ConsistencyEntity;
import com.ctrip.framework.drc.core.monitor.enums.ConsistencyEnum;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.core.monitor.reporter.Reporter;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.*;

/**
 * Created by mingdongli
 * 2019/11/15 下午5:03.
 */
public class DefaultConsistencyCheck extends AbstractLifecycle implements ConsistencyCheck {


    protected Logger logger = LoggerFactory.getLogger("consistencyMonitorLogger");

    private TableNameDescriptor tableDescriptor;

    private StreamSqlOperatorWrapper src;

    private StreamSqlOperatorWrapper dst;

    private RangeQueryTask rangeQueryTask;

    private KeyedQueryTask keyedQueryTask;

    private BlockingQueue<Checkable> checkableRound2 = new DelayQueue<Checkable>();

    private ExecutorService checkableRound2ExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("checkableRound2");

    private BlockingQueue<Checkable> checkableRound3 = new DelayQueue<Checkable>();

    private ExecutorService checkableRound3ExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("checkableRound3");

    private Reporter reporter = DefaultReporterHolder.getInstance();

    private DalUtils dalUtils = DalUtils.getInstance();

    private String cluster;

    public ConsistencyEntity getConsistencyEntity() {
        return consistencyEntity;
    }

    private ConsistencyEntity consistencyEntity;

    private String registerKey;

    public DefaultConsistencyCheck(InstanceConfig instanceConfig) {
        this.cluster = instanceConfig.getCluster();
        this.consistencyEntity = instanceConfig.getConsistencyEntity();
        DelayMonitorConfig delayMonitorConfig = instanceConfig.getDelayMonitorConfig();

        this.registerKey = instanceConfig.getConsistencyEntity().getRegistryKey();

        tableDescriptor = new DefaultTableNameDescriptor(delayMonitorConfig.getTableSchema(), delayMonitorConfig.getKey(), delayMonitorConfig.getOnUpdate());

        rangeQueryTask = new RangeQueryTask(tableDescriptor, consistencyEntity);

        src = new StreamSqlOperatorWrapper(instanceConfig.getSrcEndpoint());
        dst = new StreamSqlOperatorWrapper(instanceConfig.getDstEndpoint());
    }

    @Override
    protected void doInitialize() throws Exception {
        src.initialize();
        dst.initialize();
    }

    @Override
    protected void doStart() {

        checkableRound2ExecutorService.submit(new Runnable() {
            @Override
            public void run() {
                while (getLifecycleState().isInitialized()) {
                    try {
                        Checkable checkable = checkableRound2.take();
                        Set<String> previousKeys = checkable.getKeys();
                        keyedQueryTask = new KeyedQueryTask(tableDescriptor, previousKeys);
                        Set<String> currentKeys = keyedQueryTask.calculate(src, dst).keySet();
                        if (!CollectionUtils.isEmpty(currentKeys)) {
                            checkableRound3.offer(new Checkable(currentKeys, 60000));
                            logger.warn("[Check] error for cluster: {} for table: {} in checkableRound2, error in {}", cluster, registerKey, org.apache.commons.lang3.StringUtils.join(currentKeys, ","));
                            DefaultEventMonitorHolder.getInstance().logEvent("DRC.console.consistency.wrong.round2", cluster);
                            continue;
                        }
                        DefaultEventMonitorHolder.getInstance().logEvent("DRC.console.consistency.right.round2", cluster);
                        reporter.reportConsistency(consistencyEntity, ConsistencyEnum.CONSISTENT);
                        logger.info("checkableRound2 successfully for cluster: {} for table: {}, [keys] {}", cluster, registerKey, previousKeys);
                    } catch (Throwable t) {
                        logger.error("checkableRound2 take error", t);
                    }
                }
            }
        });

        checkableRound3ExecutorService.submit(new Runnable() {
            @Override
            public void run() {
                while (getLifecycleState().isInitialized()) {
                    try {
                        Checkable checkable = checkableRound3.take();
                        Set<String> previousKeys = checkable.getKeys();
                        keyedQueryTask = new KeyedQueryTask(tableDescriptor, previousKeys);
                        Set<String> currentKeys = keyedQueryTask.calculate(src, dst).keySet();
                        if (!CollectionUtils.isEmpty(currentKeys)) {
                            DefaultEventMonitorHolder.getInstance().logEvent("DRC.console.consistency.wrong.round3", cluster);
                            reporter.reportConsistency(consistencyEntity, ConsistencyEnum.NON_CONSISTENT);
                            logger.error("[Check] error for cluster: {} for table: {}, error in {}", cluster, registerKey, org.apache.commons.lang3.StringUtils.join(currentKeys, ","));
                            recordInconsistency(currentKeys, SourceTypeEnum.INCREMENTAL);
                            continue;
                        }
                        DefaultEventMonitorHolder.getInstance().logEvent("DRC.console.consistency.right.round3", cluster);
                        reporter.reportConsistency(consistencyEntity, ConsistencyEnum.CONSISTENT);
                        logger.info("checkableRound3 successfully for cluster: {} for table: {}, [keys] {}", cluster, registerKey, previousKeys);
                    } catch (Throwable t) {
                        logger.error("checkableRound3 take error", t);
                    }
                }
            }
        });
    }

    public void recordInconsistency(Set<String> currentKeys, SourceTypeEnum sourceTypeEnum) {
        List<DataInconsistencyHistoryTbl> dataInconsistencyHistoryTblList = Lists.newArrayList();
        try {
            MhaTbl mhaTbl = dalUtils.getMhaTblDao()
                    .queryAll()
                    .stream()
                    .filter(p -> p.getDeleted().equals(BooleanEnum.FALSE.getCode()) && consistencyEntity.getMhaName().equalsIgnoreCase(p.getMhaName())).findFirst().orElse(null);
            if (null != mhaTbl) {
                Long mhaGroupId = mhaTbl.getMhaGroupId();
//                Set<String> keyValsInDb = dalUtils.getDataInconsistencyHistoryTblDao().queryAll()
//                        .stream()
//                        .filter(p -> mhaGroupId.equals(p.getMhaGroupId()) && tableDescriptor.getSchema().equalsIgnoreCase(p.getMonitorSchemaName()) && tableDescriptor.getTable().equalsIgnoreCase(p.getMonitorTableName()))
//                        .map(DataInconsistencyHistoryTbl::getMonitorTableKeyValue)
//                        .collect(Collectors.toSet());
//                for(String keyVal : currentKeys) {
//                    if(!keyValsInDb.contains(keyVal)) {
//                        dataInconsistencyHistoryTblList.add(dalUtils.createDataInconsistencyHistoryPojo(tableDescriptor.getSchema(), tableDescriptor.getTable(), tableDescriptor.getKey(), keyVal, mhaGroupId, sourceTypeEnum));
//                    }
//                }
                String[] tableSchema = tableDescriptor.getTable().split("\\.");
                for (String keyVal : currentKeys) {
                    dataInconsistencyHistoryTblList.add(dalUtils.createDataInconsistencyHistoryPojo(tableSchema[0], tableSchema[1], tableDescriptor.getKey(), keyVal, mhaGroupId, sourceTypeEnum));
                }
                dalUtils.getDataInconsistencyHistoryTblDao().batchInsert(dataInconsistencyHistoryTblList);
            }
        } catch (SQLException e) {
            logger.warn("[[monitor=dataConsistency,table={}]]Fail record data inconsistency for {}-{}", tableDescriptor.getTable(), consistencyEntity.getMhaName(), consistencyEntity.getDcName());
        }
    }

    @Override
    protected void doStop() {
        checkableRound2.clear();
        checkableRound3.clear();
        checkableRound2ExecutorService.shutdown();
        checkableRound3ExecutorService.shutdown();
    }

    @Override
    public boolean check() {
        reporter.reportResetCounter(consistencyEntity.getTags(), 0L, "fx.drc.dataCheck.compareNum");
        Set<String> keys = rangeQueryTask.calculate(src, dst).keySet();
        if (!CollectionUtils.isEmpty(keys)) {
            checkableRound2.offer(new Checkable(keys, 30000));
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.console.consistency.wrong.round1", cluster);
            if (keys.size() <= Constants.hundred) {
                logger.warn("[Check] error for cluster: {} for table: {} in checkableRound1, error in {}", cluster, registerKey, org.apache.commons.lang3.StringUtils.join(keys, ","));
            } else {
                logger.warn("[Check] error for cluster: {} for table: {} in checkableRound1, errorSize is {}", cluster, registerKey, keys.size());
            }
            return false;
        }
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.console.consistency.right.round1", cluster);
        reporter.reportConsistency(consistencyEntity, ConsistencyEnum.CONSISTENT);
        logger.info("checkableRound1 successfully for cluster: {} for table: {}", cluster, registerKey);
        return true;
    }

    @Override
    public Set<String> getDiff() {
        return null;
    }

    private static class Checkable implements Delayed {

        private Set<String> keys;

        private long startTime;

        public Checkable(Set<String> keys, long delay) {
            this.keys = keys;
            this.startTime = TimeUnit.NANOSECONDS.convert(delay, TimeUnit.MILLISECONDS) + System.nanoTime();
        }

        public Set<String> getKeys() {
            return keys;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            long diff = startTime - System.nanoTime();
            return unit.convert(diff, TimeUnit.NANOSECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            if (this.startTime < ((Checkable) o).startTime) {
                return -1;
            }
            if (this.startTime > ((Checkable) o).startTime) {
                return 1;
            }
            return 0;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Checkable checkable = (Checkable) o;
            return startTime == checkable.startTime &&
                    Objects.equals(keys, checkable.keys);
        }

        @Override
        public int hashCode() {

            return Objects.hash(keys, startTime);
        }
    }
}
