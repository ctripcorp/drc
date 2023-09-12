package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaReplicationTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.*;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.ReadableErrorDefEnum;
import com.ctrip.framework.drc.console.enums.ResourceTagEnum;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.v2.MetaProviderV2;
import com.ctrip.framework.drc.console.param.v2.*;
import com.ctrip.framework.drc.console.service.v2.*;
import com.ctrip.framework.drc.console.service.v2.external.dba.DbaApiService;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.ClusterInfoDto;
import com.ctrip.framework.drc.console.service.v2.resource.ResourceService;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.utils.EnvUtils;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.console.vo.v2.DbReplicationView;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by dengquanliang
 * 2023/7/27 15:43
 */
@Service
public class DrcAutoBuildTaskServiceImpl implements DrcAutoBuildTaskService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;
    @Autowired
    private MetaInfoServiceV2 metaInfoService;
    @Autowired
    private MhaDbMappingService mhaDbMappingService;
    @Autowired
    private MhaTblV2Dao mhaTblDao;
    @Autowired
    private MhaReplicationTblDao mhaReplicationTblDao;
    @Autowired
    private ReplicatorGroupTblDao replicatorGroupTblDao;
    @Autowired
    private ReplicatorTblDao replicatorTblDao;
    @Autowired
    private ApplierGroupTblV2Dao applierGroupTblDao;
    @Autowired
    private ApplierTblV2Dao applierTblDao;
    @Autowired
    private ResourceTblDao resourceTblDao;
    @Autowired
    private DbTblDao dbTblDao;
    @Autowired
    private MhaDbMappingTblDao mhaDbMappingTblDao;
    @Autowired
    private DbReplicationTblDao dbReplicationTblDao;
    @Autowired
    private DbReplicationFilterMappingTblDao dbReplicationFilterMappingTblDao;
    @Autowired
    private ColumnsFilterTblV2Dao columnFilterTblV2Dao;
    @Autowired
    private RowsFilterTblV2Dao rowsFilterTblV2Dao;
    @Autowired
    private BuTblDao buTblDao;
    @Autowired
    private DcTblDao dcTblDao;
    @Autowired
    private CacheMetaService cacheMetaService;
    @Autowired
    private MessengerGroupTblDao messengerGroupTblDao;
    @Autowired
    private MessengerTblDao messengerTblDao;
    @Autowired
    private MetaProviderV2 metaProviderV2;
    @Autowired
    private MessengerServiceV2 messengerServiceV2;
    @Autowired
    private MysqlServiceV2 mysqlServiceV2;
    @Autowired
    private ResourceService resourceService;
    @Autowired
    private DbaApiService dbaApiService;
    @Autowired
    private MachineTblDao machineTblDao;
    @Autowired
    private DefaultConsoleConfig consoleConfig;
    @Autowired
    private DrcBuildServiceV2 drcBuildService;


    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void autoBuildDrc(DrcAutoBuildReq req) throws Exception {
        this.validateReq(req);

        List<DrcAutoBuildParam> params;
        boolean shardedBaseDb = this.isShardedBaseDb(req.getDbName());
        if (shardedBaseDb) {
            params = this.buildParamsFromBaseDb(req);
        } else {
            params = Collections.singletonList(this.buildParamFromSingleDb(req));
        }

        for (DrcAutoBuildParam param : params) {
            this.autoBuildDrcByDb(param);
        }
    }

    private void validateReq(DrcAutoBuildReq req) throws SQLException {
        if (StringUtils.isBlank(req.getBuName())) {
            throw ConsoleExceptionUtils.message("buName is required");
        }
        if (StringUtils.isBlank(req.getDbName())) {
            throw ConsoleExceptionUtils.message("dbName is required");
        }
        if (StringUtils.isBlank(req.getSrcMhaName())) {
            throw ConsoleExceptionUtils.message("srcRegionName is required");
        }
        if (StringUtils.isBlank(req.getDstMhaName())) {
            throw ConsoleExceptionUtils.message("dstRegionName is required");
        }
        if (req.getTblsFilterDetail() == null || StringUtils.isBlank(req.getTblsFilterDetail().getTableNames())) {
            throw ConsoleExceptionUtils.message("tableFilter is required");
        }

        if (this.isShardedDb(req.getDbName())) {
            throw ConsoleExceptionUtils.message("sharded db name not supported. enter base db instead");
        }
        DbTbl dbTbl = dbTblDao.queryByDbName(req.getDbName());
        if (dbTbl == null) {
            throw ConsoleExceptionUtils.message("db not found! ");
        }
    }

    private DrcAutoBuildParam buildParamFromSingleDb(DrcAutoBuildReq req) {
        List<ClusterInfoDto> clusterInfoDtoList = dbaApiService.getDatabaseClusterInfo(req.getDbName());
        Set<String> mhaSet = clusterInfoDtoList.stream().map(ClusterInfoDto::getClusterName).collect(Collectors.toSet());
        if(!mhaSet.contains(req.getSrcMhaName()) || !mhaSet.contains(req.getDstMhaName())) {
            throw ConsoleExceptionUtils.message(String.format("mha not found: %s -> %s", req.getSrcMhaName(), req.getDstMhaName()));
        }

        String srcMhaName = req.getSrcMhaName();
        String dstMhaName = req.getDstMhaName();
        DrcAutoBuildParam drcAutoBuildParam = new DrcAutoBuildParam();
        drcAutoBuildParam.setSrcMhaName(srcMhaName);
        drcAutoBuildParam.setDstMhaName(dstMhaName);
        drcAutoBuildParam.setBuName(req.getBuName());
        drcAutoBuildParam.setDbName(Sets.newHashSet(req.getBuName()));
        drcAutoBuildParam.setTableFilter(req.getTblsFilterDetail().getTableNames());
        drcAutoBuildParam.setSrcDcName(req.getSrcMhaName());
        drcAutoBuildParam.setDstDcName(req.getDstMhaName());
        return drcAutoBuildParam;
    }

    private Set<String> getMhaName(List<ClusterInfoDto> clusterInfoDtoList ){
        return clusterInfoDtoList.stream().map(ClusterInfoDto::getClusterName).collect(Collectors.toSet());
    }

    private Map<String, String> getDcToMhaNameMap(List<ClusterInfoDto> clusterInfoDtoList) {
        clusterInfoDtoList = clusterInfoDtoList.stream().filter(e -> EnvUtils.getEnvStr().equals(e.getEnv())).collect(Collectors.toList());
        Map<String, String> dbaDc2DrcDcMap = consoleConfig.getDbaDc2DrcDcMap();
        Map<String, String> regionToMhaMap = clusterInfoDtoList.stream().collect(Collectors.toMap(e -> {
            String dcName = dbaDc2DrcDcMap.get(e.getZoneId().toLowerCase());
            if (dcName == null) {
                throw ConsoleExceptionUtils.message("zoneId not found: " + e.getZoneId());
            }
            return dcName;
        }, ClusterInfoDto::getClusterName));
        return regionToMhaMap;
    }

    private List<DrcAutoBuildParam> buildParamsFromBaseDb(DrcAutoBuildReq req) {
        return Collections.emptyList();
    }

    private boolean isShardedDb(String dbName) {
        return false;
    }


    private void autoBuildDrcByDb(DrcAutoBuildParam param) throws Exception {

        String tag = !StringUtils.isBlank(param.getTag()) ? param.getTag() : this.getTagByBuName(param.getBuName());

        // 1.(if needed) build mha, mha replication
        DrcMhaBuildParam mhaBuildParam = new DrcMhaBuildParam(param.getSrcMhaName(), param.getDstMhaName(), param.getSrcDcName(), param.getDstDcName(), param.getBuName(), tag, tag);
        drcBuildService.buildMha(mhaBuildParam);

        MhaTblV2 srcMhaTbl = mhaTblDao.queryByMhaName(param.getSrcMhaName(), BooleanEnum.FALSE.getCode());
        MhaTblV2 dstMhaTbl = mhaTblDao.queryByMhaName(param.getDstMhaName(), BooleanEnum.FALSE.getCode());

        // check result
        if (srcMhaTbl == null || dstMhaTbl == null) {
            throw ConsoleExceptionUtils.message("init mha fail");
        }
        MhaReplicationTbl srcToDstMhaReplication = mhaReplicationTblDao.queryByMhaId(srcMhaTbl.getId(), dstMhaTbl.getId());
        MhaReplicationTbl dstToSrcMhaReplication = mhaReplicationTblDao.queryByMhaId(dstMhaTbl.getId(), srcMhaTbl.getId());
        if (srcToDstMhaReplication == null || dstToSrcMhaReplication == null) {
            throw ConsoleExceptionUtils.message("init mhaReplication fail");
        }
        List<DbReplicationView> existDbReplication = drcBuildService.getDbReplicationView(srcMhaTbl.getMhaName(), dstMhaTbl.getMhaName());

        // 2. sync mha db info
        drcBuildService.syncMhaDbInfoFormDbaApi(srcMhaTbl);
        drcBuildService.syncMhaDbInfoFormDbaApi(dstMhaTbl);

        // 3. config dbReplications
        // 3.1 base
        DbReplicationBuildParam dbReplicationBuildParam = new DbReplicationBuildParam();
        dbReplicationBuildParam.setSrcMhaName(srcMhaTbl.getMhaName());
        dbReplicationBuildParam.setDstMhaName(dstMhaTbl.getMhaName());
        dbReplicationBuildParam.setDbName("(" + String.join(",", param.getDbName()) + ")");
        dbReplicationBuildParam.setTableName(param.getTableFilter());
        this.checkTableFilter(dbReplicationBuildParam);
        drcBuildService.configureDbReplications(dbReplicationBuildParam);
        // 3.2 filters

        // 4. auto config replicators
        replicatorGroupTblDao.upsertIfNotExist(srcMhaTbl.getId());
        drcBuildService.autoConfigReplicatorsWithRealTimeGtid(srcMhaTbl);
        replicatorGroupTblDao.upsertIfNotExist(dstMhaTbl.getId());
        drcBuildService.autoConfigReplicatorsWithRealTimeGtid(dstMhaTbl);

        // 5. auto config appliers
        applierGroupTblDao.insertOrReCover(srcToDstMhaReplication.getId(), null);
        boolean updateApplierGtid = CollectionUtils.isEmpty(existDbReplication);
        drcBuildService.autoConfigAppliers(srcMhaTbl, dstMhaTbl, updateApplierGtid);
        // 6. end
    }

    private void checkTableFilter(DbReplicationBuildParam dbReplicationBuildParam) {
        String nameFilter = dbReplicationBuildParam.getDbName() + "\\." + dbReplicationBuildParam.getTableName();
        String srcMhaName = dbReplicationBuildParam.getSrcMhaName();
        String dstMhaName = dbReplicationBuildParam.getDstMhaName();
        Set<MySqlUtils.TableSchemaName> srcMatchTables = Sets.newHashSet(mysqlServiceV2.getMatchTable(srcMhaName, nameFilter));
        Set<MySqlUtils.TableSchemaName> dstMatchTables = Sets.newHashSet(mysqlServiceV2.getMatchTable(dstMhaName, nameFilter));
        if (CollectionUtils.isEmpty(srcMatchTables) || CollectionUtils.isEmpty(dstMatchTables)) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.DRC_TABLE_EMPTY, String.format("src num: %d, dst num: %d", srcMatchTables.size(), dstMatchTables.size()));
        }
        if (!srcMatchTables.equals(dstMatchTables)) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.DRC_TABLE_NOT_MATCH_BETWEEN_SRC_AND_DST, String.format("src: %s, dst: %s, nameFilter: %s", srcMhaName, dstMhaName, nameFilter));
        }

    }

    private DrcBuildBaseParam buildParam(MhaTblV2 mhaTblV2) throws SQLException {
        DrcBuildBaseParam drcBuildBaseParam = new DrcBuildBaseParam();
        drcBuildBaseParam.setMhaName(mhaTblV2.getClusterName());

        drcBuildBaseParam.setReplicatorIps(Collections.emptyList());
        drcBuildBaseParam.setApplierIps(Collections.emptyList());
        drcBuildBaseParam.setReplicatorInitGtid(null);
        drcBuildBaseParam.setApplierInitGtid(null);
        return drcBuildBaseParam;
    }

    private String getTagByBuName(String buName) {
        Map<String, String> bu2TagMap = consoleConfig.getBu2TagMap();
        return bu2TagMap.getOrDefault(buName, ResourceTagEnum.COMMON.getName());
    }


    private boolean isShardedBaseDb(String dbName) {
        return false;
    }
}
