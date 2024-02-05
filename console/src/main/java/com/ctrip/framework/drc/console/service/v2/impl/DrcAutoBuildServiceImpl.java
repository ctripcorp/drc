package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.ReplicatorGroupTblDao;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaReplicationTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.entity.v2.RegionTbl;
import com.ctrip.framework.drc.console.dao.v2.ApplierGroupTblV2Dao;
import com.ctrip.framework.drc.console.dao.v2.MhaReplicationTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.dao.v2.RegionTblDao;
import com.ctrip.framework.drc.console.dto.v2.MachineDto;
import com.ctrip.framework.drc.console.dto.v2.MhaDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.ReadableErrorDefEnum;
import com.ctrip.framework.drc.console.enums.error.AutoBuildErrorEnum;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.param.v2.DbReplicationBuildParam;
import com.ctrip.framework.drc.console.param.v2.DrcAutoBuildParam;
import com.ctrip.framework.drc.console.param.v2.DrcAutoBuildReq;
import com.ctrip.framework.drc.console.param.v2.DrcMhaBuildParam;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;
import com.ctrip.framework.drc.console.service.v2.DrcAutoBuildService;
import com.ctrip.framework.drc.console.service.v2.DrcBuildServiceV2;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.service.v2.external.dba.DbaApiService;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.ClusterInfoDto;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.DbClusterInfoDto;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.vo.check.TableCheckVo;
import com.ctrip.framework.drc.console.vo.display.v2.MhaReplicationPreviewDto;
import com.ctrip.framework.drc.console.vo.v2.DbReplicationView;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class DrcAutoBuildServiceImpl implements DrcAutoBuildService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MetaInfoServiceV2 metaInfoService;
    @Autowired
    private MhaTblV2Dao mhaTblDao;
    @Autowired
    private MhaReplicationTblDao mhaReplicationTblDao;
    @Autowired
    private ReplicatorGroupTblDao replicatorGroupTblDao;
    @Autowired
    private ApplierGroupTblV2Dao applierGroupTblDao;
    @Autowired
    private RegionTblDao regionTblDao;
    @Autowired
    private MysqlServiceV2 mysqlServiceV2;
    @Autowired
    private DbaApiService dbaApiService;
    @Autowired
    private DefaultConsoleConfig consoleConfig;
    @Autowired
    private DrcBuildServiceV2 drcBuildService;

    private static final List<String> IBU_REGIONS = Lists.newArrayList("sinibuaws", "sinibualiyun");


    @Override
    public List<MhaReplicationPreviewDto> preCheckMhaReplication(DrcAutoBuildReq req) {
        List<DbClusterInfoDto> list = this.getDbClusterInfoDtos(req);
        return getMhaReplicationPreviewDtos(req.getSrcRegionName(), req.getDstRegionName(), list);
    }

    @Override
    public List<TableCheckVo> preCheckMysqlTables(DrcAutoBuildReq req) {
        List<DrcAutoBuildParam> drcBuildParam = this.getDrcBuildParam(req);

        List<TableCheckVo> matchTable = Lists.newArrayList();
        for (DrcAutoBuildParam param : drcBuildParam) {
            String nameFilter = param.getDbNameFilter() + "\\." + param.getTableFilter();
            String mhaName = param.getSrcMhaName();
            matchTable.addAll(mysqlServiceV2.preCheckMySqlTables(mhaName, nameFilter));
        }
        return matchTable;
    }

    @Override
    public List<String> getRegionOptions(DrcAutoBuildReq req) {
        List<DbClusterInfoDto> list = this.getDbClusterInfoDtos(req);
        return getRegionNameOptions(list);
    }

    @Override
    public List<String> getCommonColumn(DrcAutoBuildReq req) {
        List<DrcAutoBuildParam> drcBuildParam = this.getDrcBuildParam(req);
        HashSet<String> commonColumns = Sets.newHashSet();

        for (DrcAutoBuildParam param : drcBuildParam) {
            String srcMhaName = param.getSrcMhaName();
            String dbNameFilter = param.getDbNameFilter();
            String tableFilter = param.getTableFilter();
            if (StringUtils.isBlank(srcMhaName) || StringUtils.isBlank(dbNameFilter) || StringUtils.isBlank(tableFilter)) {
                throw ConsoleExceptionUtils.message("get common column error, invalid param: " + param);
            }
            Set<String> columns = mysqlServiceV2.getCommonColumnIn(srcMhaName, dbNameFilter, tableFilter);
            if (commonColumns.isEmpty()) {
                commonColumns.addAll(columns);
            } else {
                commonColumns.retainAll(columns);
            }
            if (commonColumns.isEmpty()) {
                break;
            }
        }

        return Lists.newArrayList(commonColumns);
    }

    @VisibleForTesting
    protected List<DbClusterInfoDto> getDbClusterInfoDtos(DrcAutoBuildReq req) {
        DrcAutoBuildReq.BuildMode modeEnum = req.getModeEnum();
        List<DbClusterInfoDto> list;
        if (modeEnum == DrcAutoBuildReq.BuildMode.SINGLE_DB_NAME) {
            String dbName = req.getDbName();
            List<ClusterInfoDto> clusterInfoDtoList = getClusterInfoDtosByDbName(dbName);
            list = Collections.singletonList(new DbClusterInfoDto(dbName, clusterInfoDtoList));
        } else if (modeEnum == DrcAutoBuildReq.BuildMode.DAL_CLUSTER_NAME) {
            if (StringUtils.isBlank(req.getDalClusterName())) {
                throw new IllegalArgumentException("dal cluster name is required!");
            }
            list = dbaApiService.getDatabaseClusterInfoList(req.getDalClusterName());
        } else if (modeEnum == DrcAutoBuildReq.BuildMode.MULTI_DB_NAME) {
            String dbNames = req.getDbName();
            if (StringUtils.isBlank(dbNames)) {
                throw new IllegalArgumentException("db names is required!");
            }
            String[] split = dbNames.split(",");
            list = Arrays.stream(split)
                    .map(String::trim).distinct()
                    .map(dbName -> new DbClusterInfoDto(dbName, getClusterInfoDtosByDbName(dbName)))
                    .collect(Collectors.toList());
        } else {
            throw new IllegalArgumentException("illegal build mode: " + req.getMode());
        }
        return list;
    }

    private List<ClusterInfoDto> getClusterInfoDtosByDbName(String dbName) {
        if (StringUtils.isBlank(dbName)) {
            throw new IllegalArgumentException("db name is required!");
        }
        List<ClusterInfoDto> clusterInfoDtoList = dbaApiService.getDatabaseClusterInfo(dbName);
        return clusterInfoDtoList;
    }

    private List<String> getRegionNameOptions(List<DbClusterInfoDto> databaseClusterInfoList) {
        Map<String, String> dbaDc2DrcDcMap = consoleConfig.getDbaDc2DrcDcMap();
        List<DcDo> dcDos = metaInfoService.queryAllDcWithCache();
        Map<String, DcDo> dcMap = dcDos.stream().collect(Collectors.toMap(DcDo::getDcName, e -> e));

        Set<String> regions = dcDos.stream().map(DcDo::getRegionName).collect(Collectors.toSet());
        for (DbClusterInfoDto dbClusterInfoDto : databaseClusterInfoList) {
            List<ClusterInfoDto> clusterList = dbClusterInfoDto.getClusterList();
            Set<String> regionNames = Sets.newHashSet();
            for (ClusterInfoDto clusterInfoDto : clusterList) {
                String dcName = dbaDc2DrcDcMap.get(clusterInfoDto.getZoneId().toLowerCase());
                DcDo dcDo = dcMap.get(dcName);
                regionNames.add(dcDo.getRegionName());
            }
            regions.retainAll(regionNames);
        }
        return Lists.newArrayList(regions);
    }

    private List<MhaReplicationPreviewDto> getMhaReplicationPreviewDtos(String srcRegionName, String dstRegionName, List<DbClusterInfoDto> databaseClusterInfoList) {
        Map<String, String> dbaDc2DrcDcMap = consoleConfig.getDbaDc2DrcDcMap();
        List<DcDo> dcDos = metaInfoService.queryAllDcWithCache();
        Map<String, DcDo> dcMap = dcDos.stream().collect(Collectors.toMap(DcDo::getDcName, e -> e));

        List<MhaReplicationPreviewDto> list = Lists.newArrayList();
        for (DbClusterInfoDto dbClusterInfoDto : databaseClusterInfoList) {
            String dbName = dbClusterInfoDto.getDbName();
            MhaReplicationPreviewDto mhaReplicationPreviewDto = new MhaReplicationPreviewDto();
            mhaReplicationPreviewDto.setSrcRegionName(srcRegionName);
            mhaReplicationPreviewDto.setDstRegionName(dstRegionName);
            List<MhaDto> srcOptionalList = Lists.newArrayList();
            List<MhaDto> dstOptionalList = Lists.newArrayList();
            mhaReplicationPreviewDto.setSrcOptionalMha(srcOptionalList);
            mhaReplicationPreviewDto.setDstOptionalMha(dstOptionalList);

            mhaReplicationPreviewDto.setDbName(dbName);
            List<ClusterInfoDto> clusterList = dbClusterInfoDto.getClusterList();
            for (ClusterInfoDto clusterInfoDto : clusterList) {
                String dcName = dbaDc2DrcDcMap.get(clusterInfoDto.getZoneId().toLowerCase());
                DcDo dcDo = dcMap.get(dcName);
                String regionName = dcDo.getRegionName();

                if (regionName.equals(srcRegionName)) {
                    MhaDto dto = buildMhaDto(clusterInfoDto, dcDo);
                    srcOptionalList.add(dto);
                }
                if (regionName.equals(dstRegionName)) {
                    MhaDto dto = buildMhaDto(clusterInfoDto, dcDo);
                    dstOptionalList.add(dto);
                }
            }
            list.add(mhaReplicationPreviewDto);
        }
        this.fillDrcStatus(list);
        return list;
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void autoBuildDrc(DrcAutoBuildReq req) {
        this.validateReq(req);
        List<DrcAutoBuildParam> params = this.getDrcBuildParam(req);
        logger.info("autoBuildDrc params: {}", params);
        try {
            for (DrcAutoBuildParam param : params) {
                DefaultTransactionMonitorHolder.getInstance().logTransaction(
                        "DRC.auto.build",
                        "all",
                        () -> this.autoBuildDrc(param)
                );
            }
        } catch (Throwable e) {
            logger.error("auto build error", e);
            throw new ConsoleException("auto build error:" + e.getMessage(), e);
        }
    }

    @Override
    public List<String> getAllRegions() throws Exception {
        List<RegionTbl> regionTbls = regionTblDao.queryAllExist();
        return regionTbls.stream().map(RegionTbl::getRegionName).filter(e -> !IBU_REGIONS.contains(e)).collect(Collectors.toList());
    }

    @Override
    public List<DrcAutoBuildParam> getDrcBuildParam(DrcAutoBuildReq req) {
        DrcAutoBuildReq.BuildMode modeEnum = req.getModeEnum();
        if (modeEnum == null) {
            throw new IllegalArgumentException("illegal build mode: " + req.getMode());
        }
        this.validReqRegions(req);
        List<DrcAutoBuildParam> drcAutoBuildParams = this.buildParam(req);
        this.buildCommonParams(req, drcAutoBuildParams);
        return drcAutoBuildParams;
    }

    private void buildCommonParams(DrcAutoBuildReq req, List<DrcAutoBuildParam> params) {
        for (DrcAutoBuildParam param : params) {
            param.setBuName(req.getBuName());
            if (req.getTblsFilterDetail() == null) {
                throw new IllegalArgumentException("table filter is needed!");
            }
            param.setTableFilter(req.getTblsFilterDetail().getTableNames());
            param.setTag(req.getTag());
            if (Boolean.TRUE.equals(req.getOpenRowsFilterConfig())) {
                param.setRowsFilterCreateParam(req.getRowsFilterDetail());
            }
            if (Boolean.TRUE.equals(req.getOpenColsFilterConfig())) {
                param.setColumnsFilterCreateParam(req.getColsFilterDetail());
            }
        }
    }

    private void validateReq(DrcAutoBuildReq req) {
        req.validAndTrim();
    }

    private void validReqRegions(DrcAutoBuildReq req) {
        List<DcDo> dcDos = metaInfoService.queryAllDcWithCache();
        Set<String> regionSet = dcDos.stream().map(DcDo::getRegionName).collect(Collectors.toSet());
        if (StringUtils.isBlank(req.getSrcRegionName()) || StringUtils.isBlank(req.getSrcRegionName())) {
            throw ConsoleExceptionUtils.message("region name is blank!");
        }
        if (req.getSrcRegionName().equals(req.getDstRegionName())) {
            throw ConsoleExceptionUtils.message(AutoBuildErrorEnum.DRC_SAME_REGION_NOT_SUPPORTED);
        }
        if (!regionSet.contains(req.getSrcRegionName())) {
            throw new IllegalArgumentException("srcRegionName illegal: " + req.getSrcRegionName());
        }
        if (!regionSet.contains(req.getDstRegionName())) {
            throw new IllegalArgumentException("dstRegionName illegal: " + req.getDstRegionName());
        }
    }

    public List<DrcAutoBuildParam> buildParam(DrcAutoBuildReq req) {
        List<MhaReplicationPreviewDto> mhaReplicationPreviewDtos = this.preCheckMhaReplication(req);
        List<MhaReplicationPreviewDto> invalidList = mhaReplicationPreviewDtos.stream().filter(e -> !e.normalCase()).collect(Collectors.toList());
        if (!CollectionUtils.isEmpty(invalidList)) {
            throw ConsoleExceptionUtils.message(AutoBuildErrorEnum.DRC_NO_OR_MULTI_MHA_OPTIONS_NOT_SUPPORTED, this.buildErrorMessage(invalidList));
        }

        List<DrcAutoBuildParam> list = new ArrayList<>();
        // grouping by mha replication
        Map<Pair<String, String>, List<MhaReplicationPreviewDto>> map = mhaReplicationPreviewDtos.stream().collect(Collectors.groupingBy(e -> Pair.of(e.getSrcMha().getName(), e.getDstMha().getName())));
        for (List<MhaReplicationPreviewDto> replicationPreviewDtoList : map.values()) {
            MhaDto srcMha = replicationPreviewDtoList.get(0).getSrcMha();
            MhaDto dstMha = replicationPreviewDtoList.get(0).getDstMha();

            Set<String> dbNames = replicationPreviewDtoList.stream().map(MhaReplicationPreviewDto::getDbName).collect(Collectors.toCollection(TreeSet::new));
            DrcAutoBuildParam param = new DrcAutoBuildParam();
            param.setSrcMhaName(srcMha.getName());
            param.setDstMhaName(dstMha.getName());
            param.setSrcDcName(srcMha.getDcName());
            param.setDstDcName(dstMha.getDcName());
            param.setDbName(dbNames);
            param.setSrcMachines(srcMha.getMachineDtos());
            param.setDstMachines(dstMha.getMachineDtos());
            DrcAutoBuildParam.ViewOnlyInfo viewOnlyInfo = new DrcAutoBuildParam.ViewOnlyInfo();
            viewOnlyInfo.setDrcStatus(replicationPreviewDtoList.get(0).getDrcStatus());
            param.setViewOnlyInfo(viewOnlyInfo);
            list.add(param);
        }
        if (!StringUtils.isBlank(req.getGtidInit())) {
            if (list.size() == 1) {
                list.get(0).setGtidInit(req.getGtidInit());
            } else {
                throw ConsoleExceptionUtils.message(AutoBuildErrorEnum.GTID_ONLY_FOR_SINGLE_MHA_REPLICATION);
            }
        }
        return list;
    }

    private void fillDrcStatus(List<MhaReplicationPreviewDto> list) {
        Set<String> mhaNames = new HashSet<>();
        list.forEach(e -> {
            mhaNames.add(e.getSrcMha().getName());
            mhaNames.add(e.getDstMha().getName());
        });
        try {
            List<MhaTblV2> mhaTblV2List = mhaTblDao.queryByMhaNames(Lists.newArrayList(mhaNames), BooleanEnum.FALSE.getCode());
            Map<String, MhaTblV2> mhaMap = mhaTblV2List.stream().collect(Collectors.toMap(MhaTblV2::getMhaName, e -> e, (e1, e2) -> e1));
            for (MhaReplicationPreviewDto param : list) {
                MhaTblV2 srcMha = mhaMap.get(param.getSrcMha().getName());
                MhaTblV2 dstMha = mhaMap.get(param.getDstMha().getName());
                if (srcMha == null || dstMha == null) {
                    param.setDrcStatus(-1);
                    continue;
                }
                MhaReplicationTbl mhaReplicationTbl = mhaReplicationTblDao.queryByMhaId(srcMha.getId(), dstMha.getId(), BooleanEnum.FALSE.getCode());
                if (mhaReplicationTbl == null) {
                    param.setDrcStatus(-1);
                    continue;
                }
                param.setDrcStatus(mhaReplicationTbl.getDrcStatus());
            }
        } catch (SQLException e) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    private String buildErrorMessage(List<MhaReplicationPreviewDto> notValidDB) {
        StringBuilder sb = new StringBuilder();
        for (MhaReplicationPreviewDto previewDto : notValidDB) {
            if (previewDto.normalCase()) {
                continue;
            }
            sb.append("db: ").append(previewDto.getDbName());
            if (CollectionUtils.isEmpty(previewDto.getSrcOptionalMha())) {
                sb.append("no mha options: src region: ").append(previewDto.getSrcRegionName());
            } else if (previewDto.getSrcOptionalMha().size() > 2) {
                sb.append("multi options: src mha: ").append(previewDto.getSrcOptionalMha()).append(" in region ").append(previewDto.getSrcRegionName());
            }
            if (CollectionUtils.isEmpty(previewDto.getDstOptionalMha())) {
                sb.append("no mha options: dst region: ").append(previewDto.getDstRegionName());
            } else if (previewDto.getDstOptionalMha().size() > 2) {
                sb.append("multi options: dst mha: ").append(previewDto.getDstOptionalMha()).append(" in region ").append(previewDto.getDstRegionName());
            }
        }
        return sb.toString();
    }

    private void autoBuildDrc(DrcAutoBuildParam param) throws Exception {
        // 1.(if needed) build mha, mha replication
        DrcMhaBuildParam mhaBuildParam = new DrcMhaBuildParam(param.getSrcMhaName(), param.getDstMhaName(), param.getSrcDcName(), param.getDstDcName(), param.getBuName(), param.getTag(), param.getTag());
        drcBuildService.buildMha(mhaBuildParam);
        if (param.getSrcMhaName().equals(param.getDstMhaName())) {
            throw ConsoleExceptionUtils.message(AutoBuildErrorEnum.DRC_SAME_MHA_NOT_SUPPORTED, String.format("src: %s, dst: %s", param.getSrcMhaName(), param.getDstMhaName()));
        }
        MhaTblV2 srcMhaTbl = mhaTblDao.queryByMhaName(param.getSrcMhaName(), BooleanEnum.FALSE.getCode());
        MhaTblV2 dstMhaTbl = mhaTblDao.queryByMhaName(param.getDstMhaName(), BooleanEnum.FALSE.getCode());

        // check result
        if (srcMhaTbl == null || dstMhaTbl == null) {
            throw ConsoleExceptionUtils.message("init mha fail");
        }
        MhaReplicationTbl srcToDstMhaReplication = mhaReplicationTblDao.queryByMhaId(srcMhaTbl.getId(), dstMhaTbl.getId(), BooleanEnum.FALSE.getCode());
        MhaReplicationTbl dstToSrcMhaReplication = mhaReplicationTblDao.queryByMhaId(dstMhaTbl.getId(), srcMhaTbl.getId(), BooleanEnum.FALSE.getCode());
        if (srcToDstMhaReplication == null || dstToSrcMhaReplication == null) {
            throw ConsoleExceptionUtils.message("init mhaReplication fail");
        }
        List<DbReplicationView> existDbReplication = drcBuildService.getDbReplicationView(srcMhaTbl.getMhaName(), dstMhaTbl.getMhaName());

        // 2. sync mha db info
        drcBuildService.syncMhaDbInfoFromDbaApiIfNeeded(srcMhaTbl, param.getSrcMachines());
        drcBuildService.syncMhaDbInfoFromDbaApiIfNeeded(dstMhaTbl, param.getDstMachines());

        // 3. config dbReplications
        // 3.1 base
        DbReplicationBuildParam dbReplicationBuildParam = new DbReplicationBuildParam();
        dbReplicationBuildParam.setSrcMhaName(srcMhaTbl.getMhaName());
        dbReplicationBuildParam.setDstMhaName(dstMhaTbl.getMhaName());
        dbReplicationBuildParam.setDbName(param.getDbNameFilter());
        dbReplicationBuildParam.setTableName(param.getTableFilter());

        // 3.2 filters
        dbReplicationBuildParam.setRowsFilterCreateParam(param.getRowsFilterCreateParam());
        dbReplicationBuildParam.setColumnsFilterCreateParam(param.getColumnsFilterCreateParam());
        drcBuildService.buildDbReplicationConfig(dbReplicationBuildParam);

        boolean drcOff = !BooleanEnum.TRUE.getCode().equals(srcToDstMhaReplication.getDrcStatus());
        boolean drcConfigEmpty = CollectionUtils.isEmpty(existDbReplication);
        if(drcOff && !drcConfigEmpty){
            throw ConsoleExceptionUtils.message("drc has db replication but is stopped. could not auto build.");
        }
        boolean newDrc = drcOff;
        String gtidInit = param.getGtidInit();
        if (!newDrc && !StringUtils.isBlank(gtidInit)) {
            throw ConsoleExceptionUtils.message(AutoBuildErrorEnum.GTID_NOT_CONFIGURABLE_FOR_EXIST_REPLICATION);
        }
        if (StringUtils.isBlank(gtidInit)) {
            gtidInit = mysqlServiceV2.getMhaExecutedGtid(srcMhaTbl.getMhaName());
            if (StringUtils.isBlank(gtidInit)) {
                throw ConsoleExceptionUtils.message("fail to query realtime gtid for mha: " + srcMhaTbl.getMhaName());
            }
        }
        // 4. auto config replicators
        String replicatorGtid = gtidInit;
        this.checkGtidLegal(srcMhaTbl, replicatorGtid);
        replicatorGroupTblDao.upsertIfNotExist(srcMhaTbl.getId());
        drcBuildService.autoConfigReplicatorsWithGtid(srcMhaTbl, replicatorGtid);
        replicatorGroupTblDao.upsertIfNotExist(dstMhaTbl.getId());
        drcBuildService.autoConfigReplicatorsWithRealTimeGtid(dstMhaTbl);

        // 5. auto config appliers
        String applierGtid = newDrc ? gtidInit : null;
        applierGroupTblDao.insertOrReCover(srcToDstMhaReplication.getId(), null);
        drcBuildService.autoConfigAppliers(srcMhaTbl, dstMhaTbl, applierGtid);

        // 6. end
        logger.info("build success: {}", param);
    }

    private void checkGtidLegal(MhaTblV2 srcMhaTbl, String gtidInit) {
        GtidSet purgedGtid = new GtidSet(mysqlServiceV2.getMhaPurgedGtid(srcMhaTbl.getMhaName()));
        GtidSet configGtid = new GtidSet(gtidInit);
        boolean legal = purgedGtid.isContainedWithin(configGtid);
        if (!legal) {
            throw ConsoleExceptionUtils.message(AutoBuildErrorEnum.GTID_PURGED, "given:" + gtidInit + "\npurged:" + purgedGtid);
        }
    }

    private static MhaDto buildMhaDto(ClusterInfoDto clusterInfoDto, DcDo dcDo) {
        MhaDto mhaDto = new MhaDto();
        mhaDto.setName(clusterInfoDto.getClusterName());
        mhaDto.setDcId(dcDo.getDcId());
        mhaDto.setDcName(dcDo.getDcName());
        mhaDto.setRegionName(dcDo.getRegionName());
        mhaDto.setMachineDtos(clusterInfoDto.getNodes().stream().map(MachineDto::from).collect(Collectors.toList()));
        return mhaDto;
    }
}
