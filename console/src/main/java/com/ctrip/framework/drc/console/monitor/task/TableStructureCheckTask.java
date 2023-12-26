package com.ctrip.framework.drc.console.monitor.task;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaDbMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaReplicationTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.DbReplicationTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaDbMappingTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaReplicationTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.ReplicationTypeEnum;
import com.ctrip.framework.drc.console.monitor.AbstractLeaderAwareMonitor;
import com.ctrip.framework.drc.console.param.mysql.DbFilterReq;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.utils.MultiKey;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.core.monitor.reporter.Reporter;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
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
    private MhaReplicationTblDao mhaReplicationTblDao;
    @Autowired
    private MysqlServiceV2 mysqlServiceV2;

    private Reporter reporter = DefaultReporterHolder.getInstance();
    private static final String TABLE_STRUCTURE_MEASUREMENT = "drc.table.structure";
    private static final String TABLE_COLUMN_STRUCTURE_MEASUREMENT = "drc.table.column.structure";
    private final ListeningExecutorService executorService = MoreExecutors.listeningDecorator(ThreadUtils.newFixedThreadPool(10, "tableStructureCheck"));

    @Override
    public void initialize() {
        setInitialDelay(1);
        setPeriod(60);
        setTimeUnit(TimeUnit.MINUTES);
        super.initialize();
    }

    @Override
    public void scheduledTask() {
        if (!isRegionLeader || !consoleConfig.isCenterRegion()) {
            return;
        }
        CONSOLE_MONITOR_LOGGER.info("[[monitor=TableStructureCheckTask]] is leader, going to check");
        try {
            checkTableStructure();
        } catch (Exception e) {
            CONSOLE_MONITOR_LOGGER.error("[[monitor=TableStructureCheckTask]] fail, {}", e);
        }
    }

    protected void checkTableStructure() throws SQLException {
        List<MhaReplicationTbl> mhaReplicationTbls = mhaReplicationTblDao.queryAllExist().stream()
                .filter(e -> e.getDrcStatus().equals(BooleanEnum.TRUE.getCode())).collect(Collectors.toList());

        List<MhaTblV2> mhaTblV2s = mhaTblV2Dao.queryAllExist();
        List<DbReplicationTbl> dbReplicationTbls = dbReplicationTblDao.queryAllExist().stream()
                .filter(e -> e.getReplicationType().equals(ReplicationTypeEnum.DB_TO_DB.getType())).collect(Collectors.toList());
        List<DbTbl> dbTbls = dbTblDao.queryAllExist();
        List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryAllExist();

        Map<Long, MhaDbMappingTbl> mhaDbMappingTblMap = mhaDbMappingTbls.stream().collect(Collectors.toMap(MhaDbMappingTbl::getId, Function.identity()));
        Map<Long, String> dbTblMap = dbTbls.stream().collect(Collectors.toMap(DbTbl::getId, DbTbl::getDbName));
        Map<Long, String> mhaMap = mhaTblV2s.stream().collect(Collectors.toMap(MhaTblV2::getId, MhaTblV2::getMhaName));

        Map<MultiKey, Set<String>> dbReplicationMap = new HashMap<>();
        for (DbReplicationTbl dbReplicationTbl : dbReplicationTbls) {
            long srcMhaMappingId = dbReplicationTbl.getSrcMhaDbMappingId();
            long dstMhaMappingId = dbReplicationTbl.getDstMhaDbMappingId();
            String logicTableName = dbReplicationTbl.getSrcLogicTableName();
            if (dbReplicationMap.containsKey(new MultiKey(srcMhaMappingId, dstMhaMappingId))) {
                dbReplicationMap.get(new MultiKey(srcMhaMappingId, dstMhaMappingId)).add(logicTableName);
            } else if (dbReplicationMap.containsKey(new MultiKey(dstMhaMappingId, srcMhaMappingId))) {
                dbReplicationMap.get(new MultiKey(dstMhaMappingId, srcMhaMappingId)).add(logicTableName);
            } else {
                dbReplicationMap.put(new MultiKey(srcMhaMappingId, dstMhaMappingId), Sets.newHashSet(logicTableName));
            }
        }

        List<MultiKey> mhaReplicationMultiKeys = getMhaReplicationMultiKeys(mhaReplicationTbls);
        Map<MultiKey, List<String>> mhaReplicationMap = new HashMap<>();
        mhaReplicationMultiKeys.forEach(multiKey -> {
            mhaReplicationMap.put(multiKey, new ArrayList<>());
        });
        for (Map.Entry<MultiKey, Set<String>> entry : dbReplicationMap.entrySet()) {
            MultiKey multiKey = entry.getKey();
            Set<String> logicTables = entry.getValue();
            long srcMhaMappingId = (long) multiKey.getKey(0);
            long dstMhaMappingId = (long) multiKey.getKey(1);

            MhaDbMappingTbl srcMhaDbMappingTbl = mhaDbMappingTblMap.get(srcMhaMappingId);
            MhaDbMappingTbl dstMhaDbMappingTbl = mhaDbMappingTblMap.get(dstMhaMappingId);
            String dbName = dbTblMap.get(srcMhaDbMappingTbl.getDbId());
            List<String> dbTables = logicTables.stream().map(table -> dbName + "\\." + table).collect(Collectors.toList());
            String dbFilter = Joiner.on(",").join(dbTables);

            long srcMhaId = srcMhaDbMappingTbl.getMhaId();
            long dstMhaId = dstMhaDbMappingTbl.getMhaId();
            if (mhaReplicationMultiKeys.contains(new MultiKey(srcMhaId, dstMhaId))) {
                mhaReplicationMap.get(new MultiKey(srcMhaId, dstMhaId)).add(dbFilter);
            } else if (mhaReplicationMultiKeys.contains(new MultiKey(dstMhaId, srcMhaId))) {
                mhaReplicationMap.get(new MultiKey(dstMhaId, srcMhaId)).add(dbFilter);
            }
        }

        for (Map.Entry<MultiKey, List<String>> entry : mhaReplicationMap.entrySet()) {
            MultiKey multiKey = entry.getKey();
            String dbFilter = Joiner.on(",").join(entry.getValue());
            long srcMhaId = (long) multiKey.getKey(0);
            long dstMhaId = (long) multiKey.getKey(1);
            String srcMhaName = mhaMap.get(srcMhaId);
            String dstMhaName = mhaMap.get(dstMhaId);

            executorService.submit(() -> {
                Map<String, Set<String>> srcTableColumns = mysqlServiceV2.getTableColumns(new DbFilterReq(srcMhaName, dbFilter));
                Map<String, Set<String>> dstTableColumns = mysqlServiceV2.getTableColumns(new DbFilterReq(dstMhaName, dbFilter));
                compareTableColumns(srcMhaName, dstMhaName, srcTableColumns, dstTableColumns);
            });
        }
    }

    private List<MultiKey> getMhaReplicationMultiKeys(List<MhaReplicationTbl> mhaReplicationTbls) {
        List<MultiKey> mhaReplicationMultiKeys = new ArrayList<>();
        for (MhaReplicationTbl mhaReplicationTbl : mhaReplicationTbls) {
            long srcMhaId = mhaReplicationTbl.getSrcMhaId();
            long dstMhaId = mhaReplicationTbl.getDstMhaId();
            if (mhaReplicationMultiKeys.contains(new MultiKey(dstMhaId, srcMhaId))) {
                continue;
            }
            mhaReplicationMultiKeys.add(new MultiKey(srcMhaId, dstMhaId));
        }
        return mhaReplicationMultiKeys;
    }

    private void compareTableColumns(String srcMhaName, String dstMhaName, Map<String, Set<String>> srcTableColumns, Map<String, Set<String>> dstTableColumns) {
        if (CollectionUtils.isEmpty(srcTableColumns)) {
            srcTableColumns = new HashMap<>();
        }
        if (CollectionUtils.isEmpty(dstTableColumns)) {
            dstTableColumns = new HashMap<>();
        }
        Set<String> srcTables = Sets.newHashSet(srcTableColumns.keySet());
        Set<String> dstTables = Sets.newHashSet(dstTableColumns.keySet());
        List<String> diffTables = getDiff(srcTables, dstTables);
        if (!CollectionUtils.isEmpty(diffTables)) {
            CONSOLE_MONITOR_LOGGER.info("report diff tables between mha: {} -> {}, diffTables: {}", srcMhaName, dstMhaName, diffTables);
            reporter.reportResetCounter(getTableTags(srcMhaName, dstMhaName, diffTables), 1L, TABLE_STRUCTURE_MEASUREMENT);
        }

        for (Map.Entry<String, Set<String>> entry : srcTableColumns.entrySet()) {
            String tableName = entry.getKey();
            if (diffTables.contains(tableName)) {
                continue;
            }
            Set<String> srcColumns = entry.getValue();
            Set<String> dstColumns = dstTableColumns.get(tableName);
            List<String> diffColumns = getDiff(srcColumns, dstColumns);
            if (!CollectionUtils.isEmpty(diffColumns)) {
                CONSOLE_MONITOR_LOGGER.info("report diff columns between mha: {} -> {}, tableName: {}, diffColumns: {}", srcMhaName, dstMhaName, tableName, diffColumns);
                reporter.reportResetCounter(getColumnTags(srcMhaName, dstMhaName, tableName, diffColumns), 1L, TABLE_COLUMN_STRUCTURE_MEASUREMENT);
            }
        }
    }

    private Map<String, String> getTableTags(String srcMhaName, String dstMhaName, List<String> diffTables) {
        Map<String, String> tags = new HashMap<>();
        tags.put("srcMha", srcMhaName);
        tags.put("dstMha", dstMhaName);
        tags.put("diffTable", Joiner.on(",").join(diffTables));
        return tags;
    }

    private Map<String, String> getColumnTags(String srcMhaName, String dstMhaName, String tableName, List<String> diffColumns) {
        Map<String, String> tags = new HashMap<>();
        String[] dbTable = tableName.split("\\.");
        tags.put("srcMha", srcMhaName);
        tags.put("dstMha", dstMhaName);
        tags.put("db", dbTable[0]);
        tags.put("table", dbTable[1]);
        tags.put("diffColumn", Joiner.on(",").join(diffColumns));
        return tags;
    }

    private List<String> getDiff(Set<String> set0, Set<String> set1) {
        List<String> diffs = new ArrayList<>();
        if (CollectionUtils.isEmpty(set0) && CollectionUtils.isEmpty(set1)) {
            return diffs;
        }
        if (CollectionUtils.isEmpty(set0)) {
            return Lists.newArrayList(set1);
        }
        if (CollectionUtils.isEmpty(set1)) {
            return Lists.newArrayList(set0);
        }

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
