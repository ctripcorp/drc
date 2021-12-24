package com.ctrip.framework.drc.console.monitor;

import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.enums.ActionEnum;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.monitor.consistency.container.ConsistencyCheckContainer;
import com.ctrip.framework.drc.console.monitor.consistency.instance.InstanceConfig;
import com.ctrip.framework.drc.console.monitor.delay.config.*;
import com.ctrip.framework.drc.console.pojo.MetaKey;
import com.ctrip.framework.drc.console.service.impl.MetaInfoServiceImpl;
import com.ctrip.framework.drc.console.service.impl.MetaInfoServiceTwoImpl;
import com.ctrip.framework.drc.core.service.utils.Constants;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.monitor.entity.ConsistencyEntity;
import com.ctrip.framework.drc.core.server.observer.endpoint.SlaveMySQLEndpointObservable;
import com.ctrip.framework.drc.core.server.observer.endpoint.SlaveMySQLEndpointObserver;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.observer.Observable;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.unidal.tuple.Triple;

import javax.annotation.PostConstruct;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.SWITCH_STATUS_ON;
import static com.ctrip.framework.drc.core.driver.config.GlobalConfig.BU;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONSOLE_DC_LOGGER;

/**
 * Created by mingdongli
 * 2019/12/19 下午12:20.
 */
@Order(2)
@Component
public class ConsistentMonitorContainer implements MonitorContainer, SlaveMySQLEndpointObserver {

    private Logger logger = LoggerFactory.getLogger(getClass());

    public static final int INITIAL_DELAY = 1;

    public static final int DELAY = 2;

    @Autowired
    private DbClusterSourceProvider dbClusterSourceProvider;

    @Autowired
    private MetaInfoServiceTwoImpl metaInfoServiceTwo;

    @Autowired
    private MetaInfoServiceImpl metaInfoService;

    @Autowired
    private DefaultCurrentMetaManager currentMetaManager;

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;

    private ConsistencyCheckContainer checkContainer = new ConsistencyCheckContainer();

    private ConsistencyCheckContainer fullDataCheckContainer = new ConsistencyCheckContainer();

    private DcTblDao dcTblDao;

    private MhaTblDao mhaTblDao;

    private MhaGroupTblDao mhaGroupTblDao;

    private MachineTblDao machineTblDao;

    private GroupMappingTblDao groupMappingTblDao;

    private DataConsistencyMonitorTblDao dataConsistencyMonitorTblDao;

    private DataInconsistencyHistoryTblDao dataInconsistencyHistoryTblDao;

    private ScheduledExecutorService updateDataConsistencyCheckTableScheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("UpdateDataConsistencyCheckTable-Check");

    @PostConstruct
    public void init() throws Exception {
        String generalDataConsistentMonitorSwitch = monitorTableSourceProvider.getGeneralDataConsistentMonitorSwitch();
        if (SWITCH_STATUS_ON.equalsIgnoreCase(generalDataConsistentMonitorSwitch)) {
            initDaos();
            schedule();
            checkContainer.initialize();
            checkContainer.start();
            currentMetaManager.addObserver(this);
            updateDataConsistencyCheckTableScheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    try {
                        schedule();
                        logger.info("startUpdateCheck");
                    } catch (Throwable t) {
                        logger.error("update data consistency check table error", t);
                    }
                }
            }, INITIAL_DELAY, DELAY, TimeUnit.MINUTES);
        }
    }

    public int schedule() throws Exception {
        int res = 0;
        List<MhaGroupTbl> mhaGroupTbls = mhaGroupTblDao.queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<MhaTbl> mhaTbls = mhaTblDao.queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<MachineTbl> machineTbls = machineTblDao.queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode()) && predicate.getMaster().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<DcTbl> dcTbls = dcTblDao.queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<DataConsistencyMonitorTbl> dataConsistencyMonitorTblList = dataConsistencyMonitorTblDao.queryAll().stream().filter(p -> p.getMonitorSwitch().equals(BooleanEnum.TRUE.getCode())).collect(Collectors.toList());

        Map<Long, MhaTbl> idAndMhaMap = mhaTbls.stream().collect(Collectors.toMap(MhaTbl::getId, MhaTbl -> MhaTbl));
        Map<Long, MachineTbl> mhaIdAndMachineMap = machineTbls.stream().collect(Collectors.toMap(MachineTbl::getMhaId, MachineTbl -> MachineTbl, (v1, v2) -> v1));
        Map<Long, MhaGroupTbl> idAndMhaGroupMap = mhaGroupTbls.stream().collect(Collectors.toMap(MhaGroupTbl::getId, MhaGroupTbl -> MhaGroupTbl));
        Map<Long, List<MhaTbl>> mhaGroupIdAndMhaTblMap = Maps.newHashMap();
        for (MhaGroupTbl mhaGroupTbl : mhaGroupTbls) {
            Long mhaGroupId = mhaGroupTbl.getId();
            try {
                Set<Long> mhaIds = groupMappingTblDao.queryAll().stream()
                        .filter(p -> BooleanEnum.FALSE.getCode().equals(p.getDeleted()) && p.getMhaGroupId().equals(mhaGroupId)).map(GroupMappingTbl::getMhaId).collect(Collectors.toSet());
                List<MhaTbl> mhaTblsInGroup = mhaTbls.stream().filter(p -> mhaIds.contains(p.getId())).collect(Collectors.toList());
                mhaGroupIdAndMhaTblMap.put(mhaGroupId, mhaTblsInGroup);
            } catch (SQLException e) {
                logger.warn("Fail generate mhaGroupId to mhaTbls map {}", mhaGroupId, e);
            }
        }
        Map<Long, DcTbl> idAndDcMap = dcTbls.stream().collect(Collectors.toMap(DcTbl::getId, DcTbl -> DcTbl));

        String localDcName = dbClusterSourceProvider.getLocalDcName();
        Set<String> consistencyCheckSet = checkContainer.getConsistencyCheckSet();
        Set<String> allConsistencyMonitorTableSchemaSet = Sets.newConcurrentHashSet();

        for (DataConsistencyMonitorTbl dataConsistencyMonitorTbl : dataConsistencyMonitorTblList) {
            allConsistencyMonitorTableSchemaSet.add(dataConsistencyMonitorTbl.getMonitorTableSchema());
            if (consistencyCheckSet.contains(dataConsistencyMonitorTbl.getMonitorTableSchema())) {
                continue;

            }
            DelayMonitorConfig delayMonitorConfig = new DelayMonitorConfig();
            delayMonitorConfig.setSchema(dataConsistencyMonitorTbl.getMonitorSchemaName());
            delayMonitorConfig.setTable(dataConsistencyMonitorTbl.getMonitorTableName());
            delayMonitorConfig.setKey(dataConsistencyMonitorTbl.getMonitorTableKey());
            delayMonitorConfig.setOnUpdate(dataConsistencyMonitorTbl.getMonitorTableOnUpdate());

            //get src endpoint
            MhaTbl srcMhaTbl = idAndMhaMap.get(dataConsistencyMonitorTbl.getMhaId().longValue());
            if (srcMhaTbl == null) {
                logger.info("[UpdateConsistencyMonitor] dataConsistencyMonitorTbl(id: {}) its srcMha is null", dataConsistencyMonitorTbl.getId());
                continue;
            }
            String srcMhaName = srcMhaTbl.getMhaName();
            String dcName = idAndDcMap.get(srcMhaTbl.getDcId()).getDcName();
            if (!dcName.equalsIgnoreCase(localDcName)) {
                continue;
            }
            Long mhaGroupId = metaInfoService.getMhaGroupId(srcMhaName);
            MhaGroupTbl mhaGroupTbl = idAndMhaGroupMap.get(mhaGroupId);
            if (null == mhaGroupTbl) {
                logger.info("[UpdateConsistencyMonitor] dataConsistencyMonitorTbl(id: {}) its mhaGroupTbl is null", dataConsistencyMonitorTbl.getId());
                continue;
            }
            String readUser = mhaGroupTbl.getReadUser();
            String readPassword = mhaGroupTbl.getReadPassword();
            Long srcMhaId = srcMhaTbl.getId();
            MachineTbl srcMachine = mhaIdAndMachineMap.get(srcMhaId);
            if (null == srcMachine) {
                logger.info("[UpdateConsistencyMonitor] dataConsistencyMonitorTbl(id: {}) its(srcMhaId:{}) slaveMachineTbl is null", dataConsistencyMonitorTbl.getId(), srcMhaId);
                continue;
            }
            Endpoint srcEndpoint = new MySqlEndpoint(srcMachine.getIp(), srcMachine.getPort(), readUser, readPassword, BooleanEnum.FALSE.isValue());
            Endpoint dstEndpoint = null;
            //get dst endpoint
            List<MhaTbl> mhaGroup = mhaGroupIdAndMhaTblMap.get(mhaGroupId);
            Long srcDcId = srcMhaTbl.getDcId();
            String srcDcName = idAndDcMap.get(srcDcId).getDcName();
            String dstDcName = null;
            String dstMhaName = null;
            for (MhaTbl mhaTbl : mhaGroup) {
                Long destDcId = mhaTbl.getDcId();
                if (!destDcId.equals(srcDcId)) {
                    dstDcName = idAndDcMap.get(destDcId).getDcName();
                    MachineTbl destMachine = mhaIdAndMachineMap.get(mhaTbl.getId());
                    if (null == destMachine) {
                        logger.info("[UpdateConsistencyMonitor] dataConsistencyMonitorTbl(id: {}) its(mhaId:{}) slaveMachineTbl is null", dataConsistencyMonitorTbl.getId(), mhaTbl.getId());
                        continue;
                    }
                    dstMhaName = mhaTbl.getMhaName();
                    dstEndpoint = new MySqlEndpoint(destMachine.getIp(), destMachine.getPort(), readUser, readPassword, BooleanEnum.FALSE.isValue());
                    break;
                }
            }
            if (dstEndpoint == null) {
                logger.warn("[UpdateConsistencyMonitor] dataConsistencyMonitorTbl(id: {}),dst endpoint is null");
                continue;
            }
            InstanceConfig instanceConfig = getInstanceConfig(dataConsistencyMonitorTbl.getMonitorTableName(), srcEndpoint, dstEndpoint, delayMonitorConfig);
            ConsistencyEntity consistencyEntity = new ConsistencyEntity.Builder()
                    .clusterAppId(null)
                    .buName(BU)
                    .srcDcName(srcDcName)
                    .destDcName(dstDcName)
                    .clusterName(srcMhaName)
                    .mhaName(srcMhaName)
                    .destMhaName(dstMhaName)
                    .registryKey(dataConsistencyMonitorTbl.getMonitorTableSchema())
                    .srcMysqlIp(srcEndpoint.getHost())
                    .srcMysqlPort(srcEndpoint.getPort())
                    .destMysqlIp(dstEndpoint.getHost())
                    .destMysqlPort(dstEndpoint.getPort())
                    .build();
            instanceConfig.setConsistencyEntity(consistencyEntity);
            if (checkContainer.addConsistencyCheck(instanceConfig)) {
                CONSOLE_DC_LOGGER.info("[Add] {}({}-{}) into data consistency check container for {}({}:{})-{}({}:{})", delayMonitorConfig.getTableSchema(), delayMonitorConfig.getKey(), delayMonitorConfig.getOnUpdate(), srcMhaName, srcDcName, srcEndpoint, dstMhaName, dstDcName, dstEndpoint);
                res++;
            }
        }

        // use schema.table as a unique identification maybe has a problem, should insure schema.table is unique
        consistencyCheckSet.removeAll(allConsistencyMonitorTableSchemaSet);
        for (String tableSchema : consistencyCheckSet) {
            checkContainer.removeConsistencyCheck(tableSchema);
            CONSOLE_DC_LOGGER.info("[Remove] tableSchema is:{}", tableSchema);
        }
        return res;
    }
//
//    private boolean removeReportRegister(String tableSchema) {
//        String[] schemaTable = tableSchema.split(".");
//        if (schemaTable.length != 2) return false;
//        String schema = schemaTable[0];
//        String table = schemaTable[1];
//
//    }

    public void initDaos() throws SQLException {
        dcTblDao = new DcTblDao();
        mhaTblDao = new MhaTblDao();
        mhaGroupTblDao = new MhaGroupTblDao();
        machineTblDao = new MachineTblDao();
        dataConsistencyMonitorTblDao = new DataConsistencyMonitorTblDao();
        groupMappingTblDao = new GroupMappingTblDao();
    }

    private InstanceConfig getInstanceConfig(String clusterId, Endpoint srcEndpoint, Endpoint dstEndpoint, DelayMonitorConfig delayMonitorConfig) {
        InstanceConfig instanceConfig = new InstanceConfig();
        instanceConfig.setCluster(clusterId);
        instanceConfig.setSrcEndpoint(srcEndpoint);
        instanceConfig.setDstEndpoint(dstEndpoint);
        instanceConfig.setDelayMonitorConfig(delayMonitorConfig);
        return instanceConfig;
    }

    public void addFullDataConsistencyCheck(FullDataConsistencyMonitorConfig fullDataConsistencyMonitorConfig) throws Exception {
        // generalDataConsistentMonitorSwitch maybe off
        if (mhaTblDao == null) mhaTblDao = new MhaTblDao();
        if (mhaGroupTblDao == null) mhaGroupTblDao = new MhaGroupTblDao();
        if (machineTblDao == null) machineTblDao = new MachineTblDao();
        if (dataConsistencyMonitorTblDao == null) dataConsistencyMonitorTblDao = new DataConsistencyMonitorTblDao();

        MhaTbl selectMhaA = new MhaTbl();
        selectMhaA.setMhaName(fullDataConsistencyMonitorConfig.getMhaAName());
        MhaTbl mhaAEntity = mhaTblDao.queryBy(selectMhaA).get(0);
        //through mapping  get group
        MhaGroupTbl mhaGroup;
        if (mhaAEntity.getMhaGroupId() == null) {
            mhaGroup = metaInfoService.getMhaGroup(fullDataConsistencyMonitorConfig.getMhaAName(), fullDataConsistencyMonitorConfig.getMhaBName());
        } else {
            mhaGroup = mhaGroupTblDao.queryByPk(mhaAEntity.getMhaGroupId());
        }
        MachineTbl selectMachineA = new MachineTbl();
        selectMachineA.setMhaId(mhaAEntity.getId());
        selectMachineA.setMaster(0);
        MachineTbl machineAEntity = machineTblDao.queryBy(selectMachineA).get(0);
        Endpoint mhaAEndpoint = new MySqlEndpoint(machineAEntity.getIp(), machineAEntity.getPort(), mhaGroup.getReadUser(), mhaGroup.getReadPassword(), BooleanEnum.FALSE.isValue());

        MhaTbl selectMhaB = new MhaTbl();
        selectMhaB.setMhaName(fullDataConsistencyMonitorConfig.getMhaBName());
        MhaTbl mhaBEntity = mhaTblDao.queryBy(selectMhaB).get(0);
        MachineTbl selectMachineB = new MachineTbl();
        selectMachineB.setMhaId(mhaBEntity.getId());
        selectMachineB.setMaster(0);
        MachineTbl machineBEntity = machineTblDao.queryBy(selectMachineB).get(0);
        Endpoint mhaBEndpoint = new MySqlEndpoint(machineBEntity.getIp(), machineBEntity.getPort(), mhaGroup.getReadUser(), mhaGroup.getReadPassword(), BooleanEnum.FALSE.isValue());


        DataConsistencyMonitorTbl dataConsistencyMonitorTbl = dataConsistencyMonitorTblDao.queryByPk(fullDataConsistencyMonitorConfig.getTableId());
        if (dataConsistencyMonitorTbl.getFullDataCheckStatus() != 1) {
            DataConsistencyMonitorTbl sample1 = new DataConsistencyMonitorTbl();
            sample1.setId(fullDataConsistencyMonitorConfig.getTableId());
            sample1.setFullDataCheckStatus(1);
            Timestamp startCheckTimestamp = new Timestamp((System.currentTimeMillis() / Constants.oneThousand) * Constants.oneThousand);
            sample1.setFullDataCheckLasttime(startCheckTimestamp);
            dataConsistencyMonitorTblDao.update(sample1);
            DataConsistencyMonitorTbl sample2 = new DataConsistencyMonitorTbl();
            try {
                Set<String> keys = fullDataCheckContainer.addAndFullDataConsistencyCheck(fullDataConsistencyMonitorConfig, mhaAEndpoint, mhaBEndpoint);
                recordInconsistencyResult(fullDataConsistencyMonitorConfig, mhaGroup.getId(), startCheckTimestamp, keys);

                sample2.setId(fullDataConsistencyMonitorConfig.getTableId());
                sample2.setFullDataCheckStatus(Constants.two);
            } catch (Exception e) {
                sample2.setFullDataCheckStatus(Constants.three);
                throw e;
            } finally {
                dataConsistencyMonitorTblDao.update(sample2);
            }
        } else {
            throw new Exception("the table is checking now");
        }
    }

    public void recordInconsistencyResult(FullDataConsistencyMonitorConfig fullDataConsistencyMonitorConfig, long mhaGroupId, Timestamp startCheckTimestamp, Set<String> keys) throws SQLException {
        dataInconsistencyHistoryTblDao = new DataInconsistencyHistoryTblDao();
        List<DataInconsistencyHistoryTbl> resultList = new ArrayList<>();
        for (String key : keys) {
            DataInconsistencyHistoryTbl sample = new DataInconsistencyHistoryTbl();
            sample.setSourceType(Constants.two);
            sample.setMonitorSchemaName(fullDataConsistencyMonitorConfig.getSchema());
            sample.setMonitorTableName(fullDataConsistencyMonitorConfig.getTable());
            sample.setMonitorTableKey(fullDataConsistencyMonitorConfig.getKey());
            sample.setMonitorTableKeyValue(key);
            sample.setMhaGroupId(mhaGroupId);
            sample.setCreateTime(startCheckTimestamp);
            resultList.add(sample);
        }
        dataInconsistencyHistoryTblDao.batchInsert(resultList);
    }

    @Override
    public void update(Object args, Observable observable) {
        if (observable instanceof SlaveMySQLEndpointObservable) {
            Triple<MetaKey, MySqlEndpoint, ActionEnum> message = (Triple<MetaKey, MySqlEndpoint, ActionEnum>) args;
            MetaKey metaKey = message.getFirst();
            MySqlEndpoint slaveMySQLEndpoint = message.getMiddle();
            ActionEnum action = message.getLast();

            try {
                List<DataConsistencyMonitorTbl> dataConsistencyMonitors = metaInfoServiceTwo.getAllDataConsistencyMonitorTbl(metaKey.getMhaName());

                if (ActionEnum.DELETE.equals(action)) {
                    CONSOLE_DC_LOGGER.info("[OBSERVE][{}] {} {}({})", getClass().getName(), action.name(), metaKey, slaveMySQLEndpoint.getSocketAddress());
                    if (dataConsistencyMonitors.size() != 0) {
                        for (DataConsistencyMonitorTbl dataConsistencyMonitorTbl : dataConsistencyMonitors) {
                            String schemaTableName = String.format("%s.%s", dataConsistencyMonitorTbl.getMonitorSchemaName(), dataConsistencyMonitorTbl.getMonitorTableName());
                            CONSOLE_DC_LOGGER.info("[Remove Consistency] {}", schemaTableName);
                            checkContainer.removeConsistencyCheck(schemaTableName);
                        }
                    }
                }
            } catch (Throwable t) {
                CONSOLE_DC_LOGGER.warn("Fail {} {} {}", action.name(), metaKey, slaveMySQLEndpoint.getSocketAddress());
            }
        }
    }

    // just for internalDataTest
    public void addFullDataConsistencyCheckForTest(FullDataConsistencyCheckTestConfig testConfig) throws Exception {
        MySqlEndpoint endPointA = new MySqlEndpoint(testConfig.getIpA(), testConfig.getPortA(), testConfig.getUserA(), testConfig.getPasswordA(), BooleanEnum.FALSE.isValue());
        MySqlEndpoint endPointB = new MySqlEndpoint(testConfig.getIpB(), testConfig.getPortB(), testConfig.getUserB(), testConfig.getPasswordB(), BooleanEnum.FALSE.isValue());

        Timestamp startCheckTimestamp = new Timestamp((System.currentTimeMillis() / Constants.oneThousand) * Constants.oneThousand);
        addOrUpdateCheckMonitor(testConfig, startCheckTimestamp, Constants.one);

        FullDataConsistencyMonitorConfig transFormConfig = configTransForm(testConfig);
        try {
            Set<String> keys = fullDataCheckContainer.addAndFullDataConsistencyCheck(transFormConfig, endPointA, endPointB);

            // lack of mhaGroupId, user 0L for not null to recordResult
            recordInconsistencyResult(transFormConfig, 0L, startCheckTimestamp, keys);

            addOrUpdateCheckMonitor(testConfig, startCheckTimestamp, Constants.two);
        } catch (Exception e) {
            addOrUpdateCheckMonitor(testConfig, startCheckTimestamp, Constants.three);
            logger.error("[internalTest] exception happen in call,reason is: {}", e);
        }

    }

    private void addOrUpdateCheckMonitor(FullDataConsistencyCheckTestConfig testConfig, Timestamp startCheckTimestamp, Integer targetCheckStatus) throws Exception {
        if (dataConsistencyMonitorTblDao == null) dataConsistencyMonitorTblDao = new DataConsistencyMonitorTblDao();
        DataConsistencyMonitorTbl sample1 = new DataConsistencyMonitorTbl();
        //  set MhaId 0 for not null field
        sample1.setMhaId(Constants.zero);
        sample1.setMonitorSchemaName(testConfig.getSchema());
        sample1.setMonitorTableName(testConfig.getTable());
        sample1.setMonitorTableKey(testConfig.getKey());
        sample1.setMonitorTableOnUpdate(testConfig.getOnUpdate());
        sample1.setCreateTime(startCheckTimestamp);
        List<DataConsistencyMonitorTbl> dataConsistencyMonitorTbls = dataConsistencyMonitorTblDao.queryBy(sample1);
        if (dataConsistencyMonitorTbls == null || dataConsistencyMonitorTbls.size() == 0) {
            sample1.setFullDataCheckStatus(targetCheckStatus);
            logger.info("[internalTest],addMonitorTime:{}", sample1.getCreateTime());
            dataConsistencyMonitorTblDao.insert(sample1);
        } else {
            if (targetCheckStatus.equals(Constants.one) && dataConsistencyMonitorTbls.get(0).getFullDataCheckStatus().equals(Constants.one)) {
                throw new Exception("the table is checking now");
            }
            logger.info("[internalTest],updateMonitorTime:{}", sample1.getCreateTime());
            sample1.setFullDataCheckStatus(targetCheckStatus);
            sample1.setId(dataConsistencyMonitorTbls.get(0).getId());
            dataConsistencyMonitorTblDao.update(sample1);
        }
    }

    private FullDataConsistencyMonitorConfig configTransForm(FullDataConsistencyCheckTestConfig testConfig) {
        FullDataConsistencyMonitorConfig config = new FullDataConsistencyMonitorConfig();
        config.setSchema(testConfig.getSchema());
        config.setTable(testConfig.getTable());
        config.setKey(testConfig.getKey());
        config.setOnUpdate(testConfig.getOnUpdate());
        config.setEndTimeStamp(testConfig.getEndTimeStamp());
        config.setStartTimestamp(testConfig.getStartTimestamp());
        return config;
    }
}
