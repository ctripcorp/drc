package com.ctrip.framework.drc.console.monitor.task;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.DcTblDao;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.entity.DcTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaDbMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.DbReplicationTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaDbMappingTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.core.meta.ReplicationTypeEnum;
import com.ctrip.framework.drc.console.monitor.AbstractLeaderAwareMonitor;
import com.ctrip.framework.drc.console.utils.DalclusterUtils;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.mq.MqType;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@Component
@Order(2)
public class ShardedDbReplicationConsistencyCheckTask extends AbstractLeaderAwareMonitor {

    public static final String SHARDBASEDB = "shardbasedb";
    @Autowired
    private DefaultConsoleConfig consoleConfig;
    @Autowired
    private MhaTblV2Dao mhaTblV2Dao;
    @Autowired
    private DbReplicationTblDao dbReplicationTblDao;
    @Autowired
    private MhaDbMappingTblDao mhaDbMappingTblDao;
    @Autowired
    private DbTblDao dbTblDao;
    @Autowired
    private DcTblDao dcTblDao;
    private static final String DRC_SHARDED_CONFIG_CONSISTENCY_FAIL = "drc.sharded.config.consistency.fail";

    @Override
    public void initialize() {
        setInitialDelay(1);
        setPeriod(3);
        setTimeUnit(TimeUnit.MINUTES);
        super.initialize();
    }

    @Override
    public void scheduledTask() {
        if (!isRegionLeader || !consoleConfig.isCenterRegion()) {
            return;
        }
        if (!consoleConfig.getDbReplicationConsistencyCheckSwitch()) {
            return;
        }
        logger.info("[[monitor=ShardedDbReplicationConsistencyCheckTask]] is leader, going to check");
        try {
            this.checkDbReplicationConsistency();
        } catch (Exception e) {
            logger.error("[[monitor=ShardedDbReplicationConsistencyCheckTask]] fail", e);
        }
    }

    protected void checkDbReplicationConsistency() throws SQLException {
        List<DbReplicationTbl> dbReplicationTbls = dbReplicationTblDao.queryAllExist();
        Set<Long> mappingIds = dbReplicationTbls.stream().flatMap(e -> Stream.of(e.getSrcMhaDbMappingId(), e.getDstMhaDbMappingId())).collect(Collectors.toSet());
        List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryAllExist().stream().filter(e -> mappingIds.contains(e.getId())).collect(Collectors.toList());
        List<MhaTblV2> mhaTblV2List = mhaTblV2Dao.queryAllExist();
        List<DcTbl> dcTbls = dcTblDao.queryAllExist();
        Map<Long, String> regionNameMap = dcTbls.stream().collect(Collectors.toMap(DcTbl::getId, DcTbl::getRegionName));
        List<DbTbl> dbTbls = dbTblDao.queryAllExist();
        Map<Long, String> dbIdToNameMap = dbTbls.stream().collect(Collectors.toMap(DbTbl::getId, DbTbl::getDbName));
        Map<Long, String> mhaIdToRegionName = mhaTblV2List.stream().collect(Collectors.toMap(MhaTblV2::getId, e -> regionNameMap.get(e.getDcId())));
        Map<Long, String> mappingIdToRegionNameMap = mhaDbMappingTbls.stream().collect(Collectors.toMap(MhaDbMappingTbl::getId, e -> mhaIdToRegionName.get(e.getMhaId())));
        Map<Long, String> mappingIdToDbNameMap = mhaDbMappingTbls.stream().collect(Collectors.toMap(MhaDbMappingTbl::getId, e -> dbIdToNameMap.get(e.getDbId())));


        Map<String, Long> dalClusterToDbCount = dbTbls.stream().filter(e -> !e.getDbName().toLowerCase().contains(SHARDBASEDB)).collect(Collectors.groupingBy(e -> DalclusterUtils.getDalClusterName(e.getDbName()), Collectors.counting()));
        // db to db
        Map<RouteDo, List<DbReplicationTbl>> dbToDbReplications = dbReplicationTbls.stream().filter(e -> ReplicationTypeEnum.DB_TO_DB.getType().equals(e.getReplicationType())).collect(
                Collectors.groupingBy(e -> new RouteDo(mappingIdToRegionNameMap.get(e.getSrcMhaDbMappingId()), mappingIdToRegionNameMap.get(e.getDstMhaDbMappingId())))
        );
        checkDbReplicationConsistency(this.convertToReplicationDoMap(mappingIdToDbNameMap, dbToDbReplications), dalClusterToDbCount);

        // db to qmq/kafka
        for (MqType mqType : MqType.values()) {
            ReplicationTypeEnum replicationType = mqType.getReplicationType();
            Map<RouteDo, List<DbReplicationTbl>> dbToMqReplications = dbReplicationTbls.stream().filter(e -> replicationType.getType().equals(e.getReplicationType())).collect(
                    Collectors.groupingBy(e -> new RouteDo(mappingIdToRegionNameMap.get(e.getSrcMhaDbMappingId()), mappingIdToRegionNameMap.get(e.getDstMhaDbMappingId())))
            );
            checkDbReplicationConsistency(this.convertToReplicationDoMap(mappingIdToDbNameMap, dbToMqReplications), dalClusterToDbCount);
        }
    }

    private Map<RouteDo, Map<String, List<DbReplicationDo>>> convertToReplicationDoMap(Map<Long, String> mappingIdToDbNameMap, Map<RouteDo, List<DbReplicationTbl>> replicationsGroupByRoute) {
        return replicationsGroupByRoute.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, routeDoListEntry -> {
            // group by dal cluster name
            return routeDoListEntry.getValue().stream()
                    .filter(e -> mappingIdToDbNameMap.containsKey(e.getSrcMhaDbMappingId())) // filter illegal data
                    .collect(Collectors.groupingBy(e -> mappingIdToDbNameMap.get(e.getSrcMhaDbMappingId())))
                    .entrySet().stream().map(e -> {
                        String dbName = e.getKey();
                        Set<LogicTable> logicTables = e.getValue().stream().map(t -> new LogicTable(t.getSrcLogicTableName(), t.getDstLogicTableName())).collect(Collectors.toSet());
                        return new DbReplicationDo(dbName, logicTables);
                    }).collect(Collectors.groupingBy(DbReplicationDo::getDalClusterName));
        }));
    }

    protected boolean checkDbReplicationConsistency(Map<RouteDo, Map<String, List<DbReplicationDo>>> replicationsByRoute, Map<String, Long> dalClusterToDbCount) {
        boolean legal = true;
        for (Map.Entry<RouteDo, Map<String, List<DbReplicationDo>>> routeDoListEntry : replicationsByRoute.entrySet()) {
            RouteDo routeDo = routeDoListEntry.getKey();
            Map<String, List<DbReplicationDo>> replicationsByDalCluster = routeDoListEntry.getValue();
            for (Map.Entry<String, List<DbReplicationDo>> stringListEntry : replicationsByDalCluster.entrySet()) {
                String dalClusterName = stringListEntry.getKey();
                List<DbReplicationDo> dbReplicationDos = stringListEntry.getValue();
                Long expectDbNum = dalClusterToDbCount.getOrDefault(dalClusterName, 0L);
                // check all logic table same
                boolean consistent = isConsistent(dbReplicationDos);
                // check all db num right
                boolean numRight = expectDbNum.equals((long) dbReplicationDos.size());
                if (!(consistent && numRight)) {
                    legal = false;
                    logger.info("[db consistency check fail]: {} ({}): {}", dalClusterName, routeDo, dbReplicationDos);
                    DefaultEventMonitorHolder.getInstance().logEvent(DRC_SHARDED_CONFIG_CONSISTENCY_FAIL, dalClusterName + "(" + routeDo + ")");
                }
            }
        }
        return legal;
    }



    public static boolean isConsistent(List<DbReplicationDo> dbReplicationDos) {
        long count = dbReplicationDos.stream().map(DbReplicationDo::getLogicTables).distinct().count();
        return count <= 1;
    }

    static class DbReplicationDo {

        private final String dbName;
        private final Set<LogicTable> logicTables;

        public DbReplicationDo(String dbName, Set<LogicTable> logicTables) {
            this.dbName = dbName;
            this.logicTables = logicTables;
        }

        public String getDalClusterName() {
            return DalclusterUtils.getDalClusterName(dbName);
        }

        public String getDbName() {
            return dbName;
        }

        public Set<LogicTable> getLogicTables() {
            return logicTables;
        }

        @Override
        public String toString() {
            return dbName + ":" + logicTables;
        }
    }

    static class RouteDo {
        String srcRegionName;
        String dstRegionName;

        public RouteDo(String srcRegionName, String dstRegionName) {
            this.srcRegionName = srcRegionName;
            this.dstRegionName = dstRegionName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof RouteDo)) return false;
            RouteDo routeDo = (RouteDo) o;
            return Objects.equals(srcRegionName, routeDo.srcRegionName) && Objects.equals(dstRegionName, routeDo.dstRegionName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(srcRegionName, dstRegionName);
        }

        @Override
        public String toString() {
            if (StringUtils.isEmpty(dstRegionName)) {
                return srcRegionName;
            } else {
                return srcRegionName + "->" + dstRegionName;
            }
        }
    }

    static class LogicTable {
        private String src;
        private String dst;

        public LogicTable(String src, String dst) {
            this.src = src;
            this.dst = dst;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof LogicTable)) return false;
            LogicTable that = (LogicTable) o;
            return Objects.equals(src, that.src) && Objects.equals(dst, that.dst);
        }

        @Override
        public int hashCode() {
            return Objects.hash(src, dst);
        }

        public String getSrc() {
            return src;
        }

        public String getDst() {
            return dst;
        }

        @Override
        public String toString() {
            if (StringUtils.isEmpty(dst)) {
                return src;
            } else {
                return src + "->" + dst;
            }
        }
    }


}
