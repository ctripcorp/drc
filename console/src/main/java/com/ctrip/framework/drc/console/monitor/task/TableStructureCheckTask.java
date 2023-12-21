package com.ctrip.framework.drc.console.monitor.task;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaDbMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.DbReplicationTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaDbMappingTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.enums.ReplicationTypeEnum;
import com.ctrip.framework.drc.console.monitor.AbstractLeaderAwareMonitor;
import com.ctrip.framework.drc.console.param.mysql.DbFilterReq;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.utils.MultiKey;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONSOLE_MONITOR_LOGGER;

/**
 * Created by dengquanliang
 * 2023/12/20 15:06
 */
@Component
@Order(2)
public class TableStructureCheckTask extends AbstractLeaderAwareMonitor {

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
    private MysqlServiceV2 mysqlServiceV2;

    private static final String TABLE_STRUCTURE_MEASUREMENT = "drc.table.structure";
    private final ListeningExecutorService executorService = MoreExecutors.listeningDecorator(ThreadUtils.newFixedThreadPool(10, "tableStructureCheck"));

    @Override
    //ql_deng TODO 2023/12/21:
    public void initialize() {
        setInitialDelay(1);
        setPeriod(5);
        setTimeUnit(TimeUnit.MINUTES);
        super.initialize();
    }

    @Override
    public void scheduledTask() {
        if (!isRegionLeader || !consoleConfig.isCenterRegion()) {
            return;
        }
        CONSOLE_MONITOR_LOGGER.info("[[monitor=tableStructureCheck]] is leader, going on to check");

        try {
            List<MhaTblV2> mhaTblV2s = mhaTblV2Dao.queryAllExist();
            List<DbReplicationTbl> dbReplicationTbls = dbReplicationTblDao.queryAllExist().stream()
                    .filter(e -> e.getReplicationType().equals(ReplicationTypeEnum.DB_TO_DB.getType())).collect(Collectors.toList());
            List<DbTbl> dbTbls = dbTblDao.queryAllExist();
            List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryAllExist();

            Map<Long, MhaDbMappingTbl> mhaDbMappingTblMap = mhaDbMappingTbls.stream().collect(Collectors.toMap(MhaDbMappingTbl::getId, Function.identity()));
            Map<Long, String> dbTblMap = dbTbls.stream().collect(Collectors.toMap(DbTbl::getId, DbTbl::getDbName));
            Map<Long, String> mhaMap = mhaTblV2s.stream().collect(Collectors.toMap(MhaTblV2::getId, MhaTblV2::getMhaName));

            Map<MultiKey, List<String>> dbReplicationMap = new HashMap<>();
            for (DbReplicationTbl dbReplicationTbl : dbReplicationTbls) {
                long srcMhaMappingId = dbReplicationTbl.getSrcMhaDbMappingId();
                long dstMhaMappingId = dbReplicationTbl.getDstMhaDbMappingId();
                String logicTableName = dbReplicationTbl.getSrcLogicTableName();
                if (dbReplicationMap.containsKey(new MultiKey(srcMhaMappingId, dstMhaMappingId))) {
                    dbReplicationMap.get(new MultiKey(srcMhaMappingId, dstMhaMappingId)).add(logicTableName);
                } else if (dbReplicationMap.containsKey(new MultiKey(dstMhaMappingId, srcMhaMappingId))) {
                    dbReplicationMap.get(new MultiKey(dstMhaMappingId, srcMhaMappingId)).add(logicTableName);
                } else {
                    dbReplicationMap.put(new MultiKey(srcMhaMappingId, dstMhaMappingId), Lists.newArrayList(logicTableName));
                }
            }

            for (Map.Entry<MultiKey, List<String>> entry : dbReplicationMap.entrySet()) {
                MultiKey multiKey = entry.getKey();
                List<String> logicTables = entry.getValue();
                long srcMhaMappingId = (long) multiKey.getKey(0);
                long dstMhaMappingId = (long) multiKey.getKey(1);

                MhaDbMappingTbl srcMhaDbMappingTbl = mhaDbMappingTblMap.get(srcMhaMappingId);
                MhaDbMappingTbl dstMhaDbMappingTbl = mhaDbMappingTblMap.get(dstMhaMappingId);
                String srcMhaName = mhaMap.get(srcMhaDbMappingTbl.getMhaId());
                String dstMhaName = mhaMap.get(dstMhaDbMappingTbl.getMhaId());
                String dbName = dbTblMap.get(srcMhaDbMappingTbl.getDbId());
                List<String> dbTables = logicTables.stream().map(table -> dbName + "\\." + table).collect(Collectors.toList());
                String dbFilter = Joiner.on(",").join(dbTables);

                executorService.submit(() -> {
                    Map<String, Set<String>> srcTableColumns = mysqlServiceV2.getTableColumns(new DbFilterReq(srcMhaName, dbFilter));
                    Map<String, Set<String>> dstTableColumns = mysqlServiceV2.getTableColumns(new DbFilterReq(dstMhaName, dbFilter));
                    compareTableColumns(srcMhaName, dstMhaName, srcTableColumns, dstTableColumns);
                });
            }
        } catch (Exception e) {
            CONSOLE_MONITOR_LOGGER.error("[[monitor=tableStructureCheck]] fail, {}", e);
        }
    }

    private void compareTableColumns(String srcMhaName, String dstMhaName, Map<String, Set<String>> srcTableColumns, Map<String, Set<String>> dstTableColumns) {
        for (Map.Entry<String, Set<String>> entry : srcTableColumns.entrySet()) {
            String tableName = entry.getKey();
            Set<String> srcColumns = entry.getValue();
            Set<String> dstColumns = dstTableColumns.get(tableName);
            List<String> diffColumns = getDiff(srcColumns, dstColumns);
            if (!CollectionUtils.isEmpty(diffColumns)) {
                logger.info("report diff columns between mha: {} and {}, tableName: {}, diffColumns: {}", srcMhaName, dstMhaName, tableName, diffColumns);
                DefaultReporterHolder.getInstance().resetReportCounter(getTags(srcMhaName, dstMhaName, tableName, diffColumns), 1L, TABLE_STRUCTURE_MEASUREMENT);
            }
        }
    }

    private Map<String, String> getTags(String srcMhaName, String dstMhaName, String tableName, List<String> diffColumns) {
        Map<String, String> tags = new HashMap<>();
        String[] dbTable = tableName.split(".");
        tags.put("srcMha", srcMhaName);
        tags.put("dstMha", dstMhaName);
        tags.put("db", dbTable[0]);
        tags.put("table", dbTable[1]);
        tags.put("diffColumn", Joiner.on(",").join(diffColumns));
        return tags;
    }

    private List<String> getDiff(Set<String> set0, Set<String> set1) {
        List<String> diffs = new ArrayList<>();
        for (String s : set0) {
            if (!set1.contains(s)) {
                diffs.add(s);
            }
        }

        for (String s : set1) {
            if (!set0.contains(s)) {
                diffs.add(s);
            }
        }
        return diffs;
    }
}
