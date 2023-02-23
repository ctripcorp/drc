package com.ctrip.framework.drc.console.service.monitor.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dto.CurrentRecordDto;
import com.ctrip.framework.drc.console.dto.CurrentRecordPairDto;
import com.ctrip.framework.drc.console.dto.CurrentResultSetDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.monitor.ConsistentMonitorContainer;
import com.ctrip.framework.drc.console.monitor.delay.config.*;
import com.ctrip.framework.drc.console.monitor.delay.impl.execution.GeneralSingleExecution;
import com.ctrip.framework.drc.console.monitor.delay.impl.operator.WriteSqlOperatorWrapper;
import com.ctrip.framework.drc.console.pojo.MhaGroupSqlOperator;
import com.ctrip.framework.drc.console.service.impl.DrcBuildServiceImpl;
import com.ctrip.framework.drc.console.service.impl.MetaGenerator;
import com.ctrip.framework.drc.console.service.impl.MetaInfoServiceImpl;
import com.ctrip.framework.drc.console.service.monitor.ConsistencyMonitorService;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.console.vo.display.MhaGroupPair;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.meta.DBInfo;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.server.config.RegistryKey;
import com.ctrip.framework.drc.core.server.config.validation.dto.ValidationConfigDto;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.core.service.utils.Constants;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.DalQueryDao;
import com.ctrip.platform.dal.dao.DalRowMapper;
import com.ctrip.platform.dal.dao.StatementParameters;
import com.ctrip.platform.dal.dao.sqlbuilder.FreeSelectSqlBuilder;
import com.ctrip.platform.dal.dao.sqlbuilder.FreeUpdateSqlBuilder;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.SWITCH_STATUS_OFF;

/**
 * Created by jixinwang on 2020/12/29
 */
@Service
public class ConsistencyConsistencyMonitorServiceImpl implements ConsistencyMonitorService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String SELECT_CURRENT_INCONSISTENCY_RECORD_SQL = "SELECT * FROM %s.%s WHERE %s in (%s);";

    @Autowired
    private MetaInfoServiceImpl metaInfoService;

    @Autowired
    private MetaGenerator metaService;

    @Autowired
    private DrcBuildServiceImpl drcBuildService;

    @Autowired
    private ConsistentMonitorContainer consistentMonitorContainer;

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Autowired
    private DefaultConsoleConfig defaultConsoleConfig;

    private ExecutorService fullDataConsistencyService = ThreadUtils.newFixedThreadPool(5, "fullDataConsistencyService");

    private DalUtils dalUtils = DalUtils.getInstance();

    private DataConsistencyMonitorTblDao dataConsistencyMonitorTblDao;

    private MhaTblDao mhaTblDao;

    private MhaGroupTblDao mhaGroupTblDao;

    private MachineTblDao machineTblDao;

    private DcTblDao dcTblDao;

    private DalQueryDao queryDao;

    private DalRowMapper<DataInconsistencyHistoryTbl> dataInconsistencyHistoryTblMapper;

    public ConsistencyConsistencyMonitorServiceImpl(DataConsistencyMonitorTblDao dataConsistencyMonitorTblDao, MhaTblDao mhaTblDao,
                                                    MhaGroupTblDao mhaGroupTblDao, MachineTblDao machineTblDao, DcTblDao dcTblDao,
                                                    DalQueryDao queryDao,
                                                    DalRowMapper<DataInconsistencyHistoryTbl> dataInconsistencyHistoryTblMapper) {
        this.dataConsistencyMonitorTblDao = dataConsistencyMonitorTblDao;
        this.mhaTblDao = mhaTblDao;
        this.queryDao = queryDao;
        this.machineTblDao = machineTblDao;
        this.dcTblDao = dcTblDao;
        this.mhaGroupTblDao = mhaGroupTblDao;
        this.dataInconsistencyHistoryTblMapper = dataInconsistencyHistoryTblMapper;
    }

    @Override
    public void addDataConsistencyMonitor(String mhaA, String mhaB, DelayMonitorConfig delayMonitorConfig) throws SQLException {
        List<DataConsistencyMonitorTbl> dataConsistencyMonitorTblList = dataConsistencyMonitorTblDao.queryAll();
        List<String> configuredTableList = new LinkedList<>();
        List<MhaTbl> mhaTblList = mhaTblDao.queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        Map<Long, MhaTbl> idAndMhaMap = mhaTblList.stream().collect(Collectors.toMap(MhaTbl::getId, MhaTbl -> MhaTbl));
        Map<String, MhaTbl> nameAndMhaMap = mhaTblList.stream().collect(Collectors.toMap(MhaTbl::getMhaName, MhaTbl -> MhaTbl));

        for (DataConsistencyMonitorTbl dataConsistencyMonitorTbl : dataConsistencyMonitorTblList) {
            String schema = dataConsistencyMonitorTbl.getMonitorSchemaName();
            String table = dataConsistencyMonitorTbl.getMonitorTableName();
            configuredTableList.add(schema + "." + table);
        }

        //choose dc
        long mhaADcId = nameAndMhaMap.get(mhaA).getDcId();
        long mhaBDcId = nameAndMhaMap.get(mhaB).getDcId();
        int mhaADcCount = 0;
        int mhaBDcCount = 0;
        long chooseMhaId;
        for (DataConsistencyMonitorTbl dataConsistencyMonitorTbl : dataConsistencyMonitorTblList) {
            long mhaId = dataConsistencyMonitorTbl.getMhaId();
            long dcId = idAndMhaMap.get(mhaId).getDcId();
            if (dcId == mhaADcId) {
                mhaADcCount++;
            } else if (dcId == mhaBDcId) {
                mhaBDcCount++;
            }
        }
        if (mhaADcCount > mhaBDcCount) {
            chooseMhaId = nameAndMhaMap.get(mhaB).getId();
        } else {
            chooseMhaId = nameAndMhaMap.get(mhaA).getId();
        }

        DataConsistencyMonitorTbl chooseEntity = new DataConsistencyMonitorTbl();
        chooseEntity.setMhaId((int) chooseMhaId);
        String schema = delayMonitorConfig.getSchema();
        String table = delayMonitorConfig.getTable();
        String schemaDotTable = schema + "." + table;
        if ((configuredTableList.contains(schemaDotTable))) {
//            logger.warn("this tableSchema({}) is already configured", schemaDotTable);
            return;
        }
        chooseEntity.setMonitorSchemaName(schema);
        chooseEntity.setMonitorTableName(table);
        chooseEntity.setMonitorTableKey(delayMonitorConfig.getKey());
        chooseEntity.setMonitorTableOnUpdate(delayMonitorConfig.getOnUpdate());
        dataConsistencyMonitorTblDao.insert(chooseEntity);
    }

    @Override
    public List<DataConsistencyMonitorTbl> getDataConsistencyMonitor(String mhaA, String mhaB) throws SQLException {
        List<DataConsistencyMonitorTbl> dataConsistencyMonitorTblList = dataConsistencyMonitorTblDao.queryAll();
        List<MhaTbl> mhaTblList = mhaTblDao.queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        Map<String, MhaTbl> nameAndMhaMap = mhaTblList.stream().collect(Collectors.toMap(MhaTbl::getMhaName, MhaTbl -> MhaTbl));
        long mhaAId = nameAndMhaMap.get(mhaA).getId();
        long mhaBId = nameAndMhaMap.get(mhaB).getId();
        return dataConsistencyMonitorTblList.stream().filter(monitor -> monitor.getMhaId() == mhaAId || monitor.getMhaId() == mhaBId).collect(Collectors.toList());
    }

    @Override
    public void deleteDataConsistencyMonitor(int id) throws SQLException {
        DataConsistencyMonitorTbl deleteMonitor = new DataConsistencyMonitorTbl();
        deleteMonitor.setId(id);
        logger.info("[Monitor] delete data consistency, monitor_table_name is {}", deleteMonitor.getMonitorTableName());
        dataConsistencyMonitorTblDao.delete(deleteMonitor);
    }

    @Override
    public int[] deleteDataInconsistency(Set<Long> idSet) {
        try {
            List<DataInconsistencyHistoryTbl> dataInconsistencyHistoryTbls = dalUtils.getDataInconsistencyHistoryTblDao().queryAll().stream().filter(p -> idSet.contains(p.getId())).collect(Collectors.toList());
            return dalUtils.getDataInconsistencyHistoryTblDao().batchDelete(dataInconsistencyHistoryTbls);
        } catch (SQLException e) {
            logger.error("Fail delete data inconsistency tuples {}, ", idSet, e);
        }
        return new int[idSet.size()];
    }

    @Override
    public boolean addUnitVerification(Long mhaGroupId) {
        logger.info("add unit route verification for group: {}", mhaGroupId);
        try {
            List<DBInfo> machines = metaInfoService.getMachines(mhaGroupId);
            Map<String, MhaTbl> mhaTblMap = metaInfoService.getMhaTblMap(mhaGroupId);
            for (Map.Entry<String, MhaTbl> entry : mhaTblMap.entrySet()) {
                ValidationConfigDto dto = new ValidationConfigDto();
                String dc = entry.getKey();
                MhaTbl mhaTbl = entry.getValue();
                String cluster = metaInfoService.getCluster(mhaTbl.getMhaName());
                dto.setIdc(dc);
                dto.setCluster(cluster);
                dto.setMhaName(mhaTbl.getMhaName());
                dto.setGtidExecuted(drcBuildService.getGtidInit(mhaTbl));
                dto.setMachines(machines);
                dto.setReplicator(metaInfoService.getReplicator(mhaTbl));
                dto.setUidMap(metaInfoService.getUidMap(cluster, mhaTbl.getMhaName()));
                dto.setUcsStrategyIdMap(metaInfoService.getUcsStrategyIdMap(cluster, mhaTbl.getMhaName()));
                String domain = defaultConsoleConfig.getValidationDomain(dc);
                logger.info("dto: {}", dto.toString());
                ApiResult result = HttpUtils.put(domain + "/validations", dto, ApiResult.class);
                if (result.getStatus() != ResultCode.HANDLE_SUCCESS.getCode()) {
                    return false;
                }
            }
            return true;
        } catch (SQLException e) {
            logger.error("[Unit] Fail add verification for mhaGroup({})", mhaGroupId, e);
            return false;
        }
    }

    @Override
    public boolean deleteUnitVerification(Long mhaGroupId) {
        try {
            // get all mhas for the mha group and notify corresponding validation
            Map<String, MhaTbl> mhaTblMap = metaInfoService.getMhaTblMap(mhaGroupId);
            for (Map.Entry<String, MhaTbl> entry : mhaTblMap.entrySet()) {
                String dc = entry.getKey();
                MhaTbl mhaTbl = entry.getValue();
                String cluster = metaInfoService.getCluster(mhaTbl.getMhaName());
                logger.info("[Unit] delete {}.{}", cluster, mhaTbl.getMhaName());
                String registryKey = RegistryKey.from(cluster, mhaTbl.getMhaName());
                String domain = defaultConsoleConfig.getValidationDomain(dc);
                ApiResult result = HttpUtils.delete(domain + "/validations/registryKeys/" + registryKey, ApiResult.class);
                if (result.getStatus() != ResultCode.HANDLE_SUCCESS.getCode()) {
                    return false;
                }
            }
            return true;
        } catch (SQLException e) {
            logger.error("[Unit] Fail delete verification for mhaGroup({})", mhaGroupId, e);
            return false;
        }
    }


    @Override
    public Map<String, Boolean> switchDataConsistencyMonitor(List<ConsistencyMonitorConfig> consistencyMonitorConfigs, String status) {
        Map<String, Boolean> switchResults = Maps.newHashMap();
        try {
            List<DataConsistencyMonitorTbl> dataConsistencyMonitorTbls = metaService.getDataConsistencyMonitorTbls();
            for (ConsistencyMonitorConfig consistencyMonitorConfig : consistencyMonitorConfigs) {
                switchResults.put(consistencyMonitorConfig.uniqKey(), switchDataConsistencyMonitor(consistencyMonitorConfig, status, dataConsistencyMonitorTbls));
            }
        } catch (SQLException e) {
            logger.error("Fail to switch data consistency monitor, ", e);
        }
        return switchResults;
    }

    @Override
    public boolean switchUnitVerification(MhaGroupPair mhaGroupPair, String status) {
        logger.info("[[monitor=unitVerification]]switch {} monitor for group of {}-{}", status, mhaGroupPair.getSrcMha(), mhaGroupPair.getDestMha());
        try {
            MhaGroupTbl mhaGroupTbl = metaInfoService.getMhaGroup(mhaGroupPair.getSrcMha(), mhaGroupPair.getDestMha());
            int unitVerificationSwitch = status.equalsIgnoreCase(SWITCH_STATUS_OFF) ? BooleanEnum.FALSE.getCode() : BooleanEnum.TRUE.getCode();
            if (unitVerificationSwitch == mhaGroupTbl.getUnitVerificationSwitch()) {
                logger.info("unit verification status for {}-{}({}) is already {}({})", mhaGroupPair.getSrcMha(), mhaGroupPair.getDestMha(), mhaGroupTbl.getId(), status, unitVerificationSwitch);
                return true;
            } else {
                logger.info("change verification status for {}-{}({}) to {}({})", mhaGroupPair.getSrcMha(), mhaGroupPair.getDestMha(), mhaGroupTbl.getId(), status, unitVerificationSwitch);
                mhaGroupTbl.setUnitVerificationSwitch(unitVerificationSwitch);
                try {
                    int update = dalUtils.updateMhaGroup(mhaGroupTbl);
                    return update != 0;
                } catch (SQLException e) {
                    logger.error("Fail change verification status for {}-{}({}) to {}({})", mhaGroupPair.getSrcMha(), mhaGroupPair.getDestMha(), mhaGroupTbl.getId(), status, unitVerificationSwitch, e);
                    return false;
                }
            }
        } catch (Exception e) {
            logger.error("Fail change verification status for {}-{} to {}", mhaGroupPair.getSrcMha(), mhaGroupPair.getDestMha(), status, e);
            return false;
        }
    }

    @Override
    public void addFullDataConsistencyMonitor(FullDataConsistencyMonitorConfig fullDataConsistencyMonitorConfig) {
//    ExecutorService fullDataConsistencyService = ThreadUtils.newSingleThreadScheduledExecutor("fullDataConsistencyService");
        fullDataConsistencyService.submit(new Callable() {
            @Override
            public Integer call() throws Exception {
                consistentMonitorContainer.addFullDataConsistencyCheck(fullDataConsistencyMonitorConfig);
                return null;
            }
        });
    }

    // just For internalTest
    @Override
    public void addFullDataConsistencyCheck(FullDataConsistencyCheckTestConfig fullDataConsistencyCheckTestConfig) {
//        ExecutorService fullDataConsistencyService = ThreadUtils.newSingleThreadScheduledExecutor("fullDataConsistencyForTestService");
        fullDataConsistencyService.submit(new Callable() {
            @Override
            public Integer call() {
                logger.info("[internalTest] addFullDataConsistencyCheck submit callable");
                try {
                    consistentMonitorContainer.addFullDataConsistencyCheckForTest(fullDataConsistencyCheckTestConfig);
                } catch (Exception e) {
                    logger.error("[internalTest] exception happen in call,reason is: {}", e);
                }
                return null;
            }
        });
    }

    @VisibleForTesting
    //just for test
    public void executeCustomSql(String sql) throws SQLException {
        DalHints hints = new DalHints();
        FreeUpdateSqlBuilder builder = new FreeUpdateSqlBuilder();
        builder.setTemplate(sql);
        StatementParameters parameters = new StatementParameters();
        queryDao.update(builder, parameters, hints);
    }

    @Override
    public Map<String, Object> getCurrentFullInconsistencyRecord(String mhaA, String mhaB, String dbName, String tableName, String key, String checkTime) throws SQLException {
        List<String> keyValueList = selectInconsistencyKeys(dbName, tableName, checkTime);
        return selectCurrentInconsistencyRecord(getMhaGroupIdByMhaName(mhaA), dbName, tableName, key, keyValueList);
    }

    @Override
    public Map<String, Object> getCurrentFullInconsistencyRecordForTest(FullDataConsistencyCheckTestConfig testConfig, String checkTime) throws SQLException {
        String schema = testConfig.getSchema();
        String table = testConfig.getTable();
        List<String> keyValueList = selectInconsistencyKeys(schema, table, checkTime);
        return selectCurrentInconsistencyRecordForTest(testConfig, schema, table, testConfig.getKey(), keyValueList);
    }

    @Override
    public Map<String, Object> getCurrentIncrementInconsistencyRecord(long mhaGroupId, String dbName, String tableName, String key, String keyValue) throws SQLException {
        List<String> keyValueList = Arrays.asList(keyValue);
        return selectCurrentInconsistencyRecord(mhaGroupId, dbName, tableName, key, keyValueList);
    }

    @Override
    public List<DataInconsistencyHistoryTbl> getIncrementInconsistencyHistory(int pageNo, int pageSize, String dbName, String tableName, String startTime, String endTime) throws SQLException {
        return selectInconsistencyHistory(pageNo,pageSize,dbName,tableName,startTime, endTime);
    }

    @Override
    public long getIncrementInconsistencyHistoryCount(String dbName, String tableName, String startTime, String endTime) throws SQLException {
        return selectInconsistencyHistoryCount(dbName,tableName, startTime,endTime);
    }

    @Override
    public void handleInconsistency(Map<String, String> updateInfo) throws SQLException {
        String mhaName = updateInfo.get("mhaName");
        long mhaGroupId = getMhaGroupIdByMhaName(mhaName);
        MhaGroupSqlOperator mhaGroupSqlOperator = getMhaGroupSqlOperator(mhaGroupId, BooleanEnum.TRUE);
        String mhaAName = mhaGroupSqlOperator.getMhaAName();
        String mhaBName = mhaGroupSqlOperator.getMhaBName();
        WriteSqlOperatorWrapper writeSqlOperatorWrapper = null;
        if (mhaName.equalsIgnoreCase(mhaAName)) {
            writeSqlOperatorWrapper = mhaGroupSqlOperator.getMhaASqlOperatorWrapper();
        }
        if (mhaName.equalsIgnoreCase(mhaBName)) {
            writeSqlOperatorWrapper = mhaGroupSqlOperator.getMhaBSqlOperatorWrapper();
        }
        String sql = updateInfo.get("sql");
        GeneralSingleExecution execution = new GeneralSingleExecution(sql);
        if (writeSqlOperatorWrapper != null) {
            writeSqlOperatorWrapper.write(execution);
        }
    }

    private boolean switchDataConsistencyMonitor(ConsistencyMonitorConfig consistencyMonitorConfig, String status, List<DataConsistencyMonitorTbl> dataConsistencyMonitorTbls) {
        String srcMha = consistencyMonitorConfig.getSrcMha();
        String dstMha = consistencyMonitorConfig.getDstMha();
        String schema = consistencyMonitorConfig.getSchema();
        String table = consistencyMonitorConfig.getTable();
        String uniqKey = consistencyMonitorConfig.uniqKey();
        logger.info("[[monitor=dataConsistency]]switch {} monitor for {}", status, uniqKey);
        List<Long> mhaIds = metaInfoService.getMhaIds(Arrays.asList(srcMha, dstMha));
        DataConsistencyMonitorTbl dataConsistencyMonitorTbl = dataConsistencyMonitorTbls.stream().filter(p -> mhaIds.contains((long) p.getMhaId()) && schema.equalsIgnoreCase(p.getMonitorSchemaName()) && table.equalsIgnoreCase(p.getMonitorTableName())).findFirst().orElse(null);
        if (null == dataConsistencyMonitorTbl) {
            logger.info("no such table({}) available", uniqKey);
            return false;
        } else {
            int monitorSwitch = status.equalsIgnoreCase(SWITCH_STATUS_OFF) ? BooleanEnum.FALSE.getCode() : BooleanEnum.TRUE.getCode();
            if (monitorSwitch == dataConsistencyMonitorTbl.getMonitorSwitch()) {
                logger.info("monitor status for {} is already {}({})", uniqKey, status, monitorSwitch);
                return true;
            } else {
                logger.info("change monitor status for {} to {}({})", uniqKey, status, monitorSwitch);
                dataConsistencyMonitorTbl.setMonitorSwitch(monitorSwitch);
                try {
                    int update = dalUtils.updateDataConsistencyMonitor(dataConsistencyMonitorTbl);
                    return update != 0;
                } catch (SQLException e) {
                    logger.error("Fail change monitor status for {} to {}({})", uniqKey, status, monitorSwitch, e);
                    return false;
                }
            }
        }
    }

    public List<DataConsistencyMonitorTbl> getAllDataConsistencyMonitor() throws SQLException {
        return dataConsistencyMonitorTblDao.queryAll();
    }

    private List<String> selectInconsistencyKeys(String dbName, String tableName, String checkTime) throws SQLException {
        DalHints hints = new DalHints();
        FreeSelectSqlBuilder<List<String>> builder = new FreeSelectSqlBuilder<>();
        builder.setTemplate("SELECT distinct monitor_table_key_value FROM data_inconsistency_history_tbl WHERE monitor_schema_name = ? AND monitor_table_name = ? AND create_time = ? AND source_type = 2");
        StatementParameters parameters = new StatementParameters();
        int i = 1;
        parameters.set(i++, "monitor_schema_name", Types.VARCHAR, dbName);
        parameters.set(i++, "monitor_table_name", Types.VARCHAR, tableName);
        parameters.set(i, "create_time", Types.VARCHAR, checkTime);
        builder.simpleType();

        return queryDao.query(builder, parameters, hints);
    }

    public long getMhaGroupIdByMhaName(String mhaName) throws SQLException {
        MhaTbl mhaSample = new MhaTbl();
        mhaSample.setMhaName(mhaName);
        List<MhaTbl> mhaList = mhaTblDao.queryBy(mhaSample);
        return mhaList.get(0).getMhaGroupId();
    }

    public Map<String, Object> selectCurrentInconsistencyRecordForTest(FullDataConsistencyCheckTestConfig testConfig, String schema, String table, String key, List<String> keyValueList) throws SQLException {
        String sql = String.format(SELECT_CURRENT_INCONSISTENCY_RECORD_SQL, schema, table, key, String.join(",", keyValueList));
        Endpoint endpointA = new MySqlEndpoint(testConfig.getIpA(), testConfig.getPortA(), testConfig.getUserA(), testConfig.getPasswordA(), BooleanEnum.FALSE.isValue());
        Endpoint endpointB = new MySqlEndpoint(testConfig.getIpB(), testConfig.getPortB(), testConfig.getUserB(), testConfig.getPasswordB(), BooleanEnum.FALSE.isValue());
        WriteSqlOperatorWrapper writeSqlOperatorWrapperA = new WriteSqlOperatorWrapper(endpointA);
        WriteSqlOperatorWrapper writeSqlOperatorWrapperB = new WriteSqlOperatorWrapper(endpointB);
        try {
            writeSqlOperatorWrapperA.initialize();
            writeSqlOperatorWrapperA.start();
            writeSqlOperatorWrapperB.initialize();
            writeSqlOperatorWrapperB.start();
        } catch (Exception e) {
            logger.error("init sql operator error");
        }
        return compareResultSetForTest(writeSqlOperatorWrapperA, writeSqlOperatorWrapperB, sql, key);
    }

    private Map<String, Object> compareResultSetForTest(WriteSqlOperatorWrapper writeSqlOperatorWrapperA, WriteSqlOperatorWrapper writeSqlOperatorWrapperB, String sql, String key) throws SQLException {
        Map<String, Object> result = new HashMap<>();
        GeneralSingleExecution execution = new GeneralSingleExecution(sql);

        ReadResource srcResult = null;
        ReadResource dstResult = null;
        try {
            srcResult = writeSqlOperatorWrapperA.select(execution);
            dstResult = writeSqlOperatorWrapperB.select(execution);
            result = toCompare(srcResult.getResultSet(), dstResult.getResultSet(), key);
            return result;
        } catch (Exception e) {
            logger.error("compare error", e);
        } finally {
            if (srcResult != null) {
                srcResult.close();
            }
            if (dstResult != null) {
                dstResult.close();
            }
        }
        return null;
    }

    @Override
    public Map<String, Object> getFullDataCheckStatusForTest(String schema, String table, String key) throws SQLException {
        DataConsistencyMonitorTblDao dataConsistencyMonitorTblDao = new DataConsistencyMonitorTblDao();
        DataConsistencyMonitorTbl sample = new DataConsistencyMonitorTbl();
        sample.setMhaId(Constants.zero);
        sample.setMonitorSchemaName(schema);
        sample.setMonitorTableName(table);
        sample.setMonitorTableKey(key);
        DataConsistencyMonitorTbl dataConsistencyMonitorTbl = dataConsistencyMonitorTblDao.queryBy(sample).stream()
                .max(Comparator.comparing(DataConsistencyMonitorTbl::getCreateTime)).get();
        HashMap<String, Object> result = Maps.newHashMap();
        result.put("checkTime", dataConsistencyMonitorTbl.getCreateTime());
        result.put("checkStatus", dataConsistencyMonitorTbl.getFullDataCheckStatus());
        return result;
    }


    public Map<String, Object> selectCurrentInconsistencyRecord(long mhaGroupId, String dbName, String tableName, String key, List<String> keyValueList) throws SQLException {
        String sql = String.format(SELECT_CURRENT_INCONSISTENCY_RECORD_SQL, dbName, tableName, key, String.join(",", keyValueList));
        MhaGroupSqlOperator mhaGroupSqlOperator = getMhaGroupSqlOperator(mhaGroupId, BooleanEnum.FALSE);
        return compareResultSet(mhaGroupSqlOperator, sql, key);
    }

    public Map<String, Object> compareResultSet(MhaGroupSqlOperator mhaGroupSqlOperator, String sql, String key) throws SQLException {
        Map<String, Object> result = new HashMap<>();
        GeneralSingleExecution execution = new GeneralSingleExecution(sql);

        ReadResource srcResult = null;
        ReadResource dstResult = null;
        try {
            srcResult = mhaGroupSqlOperator.getMhaASqlOperatorWrapper().select(execution);
            dstResult = mhaGroupSqlOperator.getMhaBSqlOperatorWrapper().select(execution);
            result = toCompare(srcResult.getResultSet(), dstResult.getResultSet(), key);
            result.put("mhaAName", mhaGroupSqlOperator.getMhaAName());
            result.put("mhaBName", mhaGroupSqlOperator.getMhaBName());
            result.put("mhaADc", mhaGroupSqlOperator.getMhaADc());
            result.put("mhaBDc", mhaGroupSqlOperator.getMhaBDc());
            return result;
        } catch (Exception e) {
            logger.error("compare error", e);
        } finally {
            if (srcResult != null) {
                srcResult.close();
            }
            if (dstResult != null) {
                dstResult.close();
            }
        }
        return null;
    }

    public Map<String, Object> toCompare(ResultSet srcResultSet, ResultSet destResultSet, String key) throws SQLException {
        Map<String, Object> result = new HashMap<>();
        CurrentRecordDto mhaACurrentRecordDto = extract(srcResultSet, key);
        CurrentRecordDto mhaBCurrentRecordDto = extract(destResultSet, key);
        CurrentRecordPairDto currentRecordPairDto = new CurrentRecordPairDto();
        currentRecordPairDto.setMhaAColumnNameList(mhaACurrentRecordDto.getColumnNameList());
        currentRecordPairDto.setMhaAColumnPattern(mhaACurrentRecordDto.getColumnPattern());
        currentRecordPairDto.setMhaBColumnNameList(mhaBCurrentRecordDto.getColumnNameList());
        currentRecordPairDto.setMhaBColumnPattern(mhaBCurrentRecordDto.getColumnPattern());
        currentRecordPairDto.setKeyAndCurrentResultSetPairMap(mhaACurrentRecordDto, mhaBCurrentRecordDto);
        boolean markDifferentRecord = currentRecordPairDto.markDifferentRecord();
        result.put("markDifferentRecord", markDifferentRecord);
        result.put("mhaAColumnPattern", currentRecordPairDto.getMhaAColumnPattern());
        result.put("mhaBColumnPattern", currentRecordPairDto.getMhaBColumnPattern());
        currentRecordPairDto.setCurrentResultList();
        result.put("mhaACurrentResultList", currentRecordPairDto.getMhaACurrentResultList());
        result.put("mhaBCurrentResultList", currentRecordPairDto.getMhaBCurrentResultList());
        result.put("differentCount", currentRecordPairDto.getDifferentCount());
        return result;
    }

    public CurrentRecordDto extract(ResultSet resultSet, String key) throws SQLException {
        CurrentRecordDto currentResultDto = new CurrentRecordDto();
        if (resultSet == null) {
            return currentResultDto;
        }
        ResultSetMetaData metaData = resultSet.getMetaData();
        if (metaData == null) {
            return currentResultDto;
        }
        List<Map<String, Object>> metaColumn = new ArrayList<>();
        List<String> columnNameList = new ArrayList<>();
        int columnCount = metaData.getColumnCount();
        for (int j = 1; j <= columnCount; j++) {
            Map<String, Object> columnData = new LinkedHashMap<>();
            columnData.put("title", metaData.getColumnName(j));
            columnData.put("key", metaData.getColumnName(j));
            columnData.put("width", 200);
            columnData.put("tooltip", true);
            metaColumn.add(columnData);
            columnNameList.add(metaData.getColumnName(j));
        }
        currentResultDto.setColumnPattern(metaColumn);
        currentResultDto.setColumnNameList(columnNameList);
        List<CurrentResultSetDto> currentResultSetDtoList = new ArrayList<>();
        List<String> keyValueList = new ArrayList<>();
        while (resultSet.next()) {
            Map<String, Object> rowData = new LinkedHashMap<>();
            String keyValue = null;
            CurrentResultSetDto currentResultSetDto = new CurrentResultSetDto();
            for (String columnName : columnNameList) {
                String value = resultSet.getString(columnName);
                if (columnName.equalsIgnoreCase(key)) {
                    keyValue = value;
                }
                rowData.put(columnName, resultSet.getString(columnName));
            }
            Map<String, String> cellClassName = new HashMap<>();
            rowData.put("cellClassName", cellClassName);
            currentResultSetDto.setKeyValue(keyValue);
            keyValueList.add(keyValue);
            currentResultSetDto.setCurrentResult(rowData);
            currentResultSetDtoList.add(currentResultSetDto);
        }
        currentResultDto.setCurrentResultSetDto(currentResultSetDtoList);
        currentResultDto.setKeyValueList(keyValueList);
        return currentResultDto;
    }

    public MhaGroupSqlOperator getMhaGroupSqlOperator(long mhaGroupId, BooleanEnum isMaster) throws SQLException {
        MhaGroupSqlOperator mhaGroupSqlOperator = new MhaGroupSqlOperator();
        MhaGroupTbl mhaGroup = mhaGroupTblDao.queryByPk(mhaGroupId);
        String monitorUser = mhaGroup.getWriteUser();
        String monitorPassword = mhaGroup.getWritePassword();

        MhaTbl mhaTblSample = new MhaTbl();
        mhaTblSample.setMhaGroupId(mhaGroupId);
        List<MhaTbl> mhaTblList = mhaTblDao.queryBy(mhaTblSample);
        MhaTbl mhaA = mhaTblList.get(0);
        MhaTbl mhaB = mhaTblList.get(1);
        mhaGroupSqlOperator.setMhaAName(mhaA.getMhaName());
        mhaGroupSqlOperator.setMhaBName(mhaB.getMhaName());

        List<DcTbl> dcTbls = dcTblDao.queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        Map<Long, DcTbl> idAndDcMap = dcTbls.stream().collect(Collectors.toMap(DcTbl::getId, DcTbl -> DcTbl));
        mhaGroupSqlOperator.setMhaADc(idAndDcMap.get(mhaA.getDcId()).getDcName());
        mhaGroupSqlOperator.setMhaBDc(idAndDcMap.get(mhaB.getDcId()).getDcName());
        long mhaAId = mhaA.getId();
        mhaGroupSqlOperator.setMhaASqlOperatorWrapper(getWriteSqlOperatorWrapper(monitorUser, monitorPassword, mhaAId, isMaster));
        long mhaBId = mhaB.getId();
        mhaGroupSqlOperator.setMhaBSqlOperatorWrapper(getWriteSqlOperatorWrapper(monitorUser, monitorPassword, mhaBId, isMaster));

        return mhaGroupSqlOperator;
    }

    public WriteSqlOperatorWrapper getWriteSqlOperatorWrapper(String userName, String password, long mhaId, BooleanEnum isMaster) throws SQLException {
        MachineTbl machineSample = new MachineTbl();
        machineSample.setMhaId(mhaId);
        machineSample.setMaster(isMaster.getCode());
        List<MachineTbl> machineList = machineTblDao.queryBy(machineSample);
        MachineTbl machine = machineList.get(0);
        Endpoint endpoint = new MySqlEndpoint(machine.getIp(), machine.getPort(), userName, password, BooleanEnum.TRUE.getCode().equals(machine.getMaster()));
        WriteSqlOperatorWrapper writeSqlOperatorWrapper = new WriteSqlOperatorWrapper(endpoint);
        try {
            writeSqlOperatorWrapper.initialize();
            writeSqlOperatorWrapper.start();
        } catch (Exception e) {
            logger.error("init sql operator error");
        }
        return writeSqlOperatorWrapper;
    }
    
    public String getSelectInconsistencyHistorySql(int pageNo, int pageSize, String dbName, String tableName, String startTime, String endTime) {
        StringBuffer sql = new StringBuffer("select * from data_inconsistency_history_tbl where 1 = 1");
        if (!dbName.isEmpty()) {
            sql.append(" and monitor_schema_name = ?");
        }
        if (!tableName.isEmpty()) {
            sql.append(" and monitor_table_name = ?");
        }
        if (!startTime.isEmpty()) {
            sql.append(" and datachange_lasttime >= ?");
        }
        if (!endTime.isEmpty()) {
            sql.append(" and datachange_lasttime <= ?");
        }
        sql.append(" and source_type = 1 order by datachange_lasttime desc limit " + (pageNo-1)*pageSize + "," + pageSize);
        return sql.toString();
    }

    public String getCountInconsistencyHistorySql(String dbName, String tableName, String startTime, String endTime) {
        StringBuffer sql = new StringBuffer("select count(*) from data_inconsistency_history_tbl where 1 = 1");
        if (!dbName.isEmpty()) {
            sql.append(" and monitor_schema_name = ?");
        }
        if (!tableName.isEmpty()) {
            sql.append(" and monitor_table_name = ?");
        }
        if (!startTime.isEmpty()) {
            sql.append(" and datachange_lasttime >= ?");
        }
        if (!endTime.isEmpty()) {
            sql.append(" and datachange_lasttime <= ?");
        }
        sql.append(" and source_type = 1 order by datachange_lasttime desc");
        return sql.toString();
    }
    
    public StatementParameters setStatementParameters(StatementParameters parameters,String dbName, String tableName, String startTime, String endTime) {
        int i = 1;
        if (!dbName.isEmpty()) {
            parameters.set(i++,"monitor_schema_name",Types.VARCHAR,dbName);
        }
        if (!tableName.isEmpty()) {
            parameters.set(i++,"monitor_table_name",Types.VARCHAR,tableName);
        }
        if (!startTime.isEmpty()) {
            parameters.set(i++,"datachange_lasttime",Types.VARCHAR,startTime);
        }
        if (!endTime.isEmpty()) {
            parameters.set(i++,Types.VARCHAR,endTime);
        }
        return parameters;
    }

    public List<DataInconsistencyHistoryTbl> selectInconsistencyHistory(int pageNo, int pageSize, String dbName, String tableName, String startTime, String endTime) throws SQLException {
        FreeSelectSqlBuilder<List<DataInconsistencyHistoryTbl>> builder = new FreeSelectSqlBuilder<>();
        String sqlTemplate = getSelectInconsistencyHistorySql(pageNo, pageSize, dbName, tableName, startTime, endTime);
        builder.setTemplate(sqlTemplate);
        StatementParameters parameters = new StatementParameters();
        parameters = setStatementParameters(parameters, dbName, tableName, startTime, endTime);
        builder.mapWith(dataInconsistencyHistoryTblMapper);
        return queryDao.query(builder, parameters, new DalHints());
    }

    public long selectInconsistencyHistoryCount(String dbName, String tableName, String startTime, String endTime) throws SQLException {
        FreeSelectSqlBuilder<Long> builder = new FreeSelectSqlBuilder<>();
        String sqlTemplate = getCountInconsistencyHistorySql(dbName, tableName, startTime, endTime);
        builder.setTemplate(sqlTemplate);
        StatementParameters parameters = new StatementParameters();
        parameters = setStatementParameters(parameters, dbName, tableName, startTime, endTime);
        builder.simpleType().requireSingle().nullable();
        return queryDao.query(builder, parameters, new DalHints());
    }
}
