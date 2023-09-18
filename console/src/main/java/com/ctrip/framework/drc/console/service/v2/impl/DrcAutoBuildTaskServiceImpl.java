package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.ReplicatorGroupTblDao;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaReplicationTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.ApplierGroupTblV2Dao;
import com.ctrip.framework.drc.console.dao.v2.MhaReplicationTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.dto.v2.MhaDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.ResourceTagEnum;
import com.ctrip.framework.drc.console.enums.error.AutoBuildErrorEnum;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.param.v2.DbReplicationBuildParam;
import com.ctrip.framework.drc.console.param.v2.DrcAutoBuildParam;
import com.ctrip.framework.drc.console.param.v2.DrcAutoBuildReq;
import com.ctrip.framework.drc.console.param.v2.DrcMhaBuildParam;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;
import com.ctrip.framework.drc.console.service.v2.DrcAutoBuildTaskService;
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
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class DrcAutoBuildTaskServiceImpl implements DrcAutoBuildTaskService {

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
    private DbTblDao dbTblDao;
    @Autowired
    private MysqlServiceV2 mysqlServiceV2;
    @Autowired
    private DbaApiService dbaApiService;
    @Autowired
    private DefaultConsoleConfig consoleConfig;
    @Autowired
    private DrcBuildServiceV2 drcBuildService;


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
                if (commonColumns.isEmpty()) {
                    break;
                }
            }
        }

        return Lists.newArrayList(commonColumns);
    }

    private List<DbClusterInfoDto> getDbClusterInfoDtos(DrcAutoBuildReq req) {
        DrcAutoBuildReq.BuildMode modeEnum = req.getModeEnum();
        List<DbClusterInfoDto> list;
        if (modeEnum == DrcAutoBuildReq.BuildMode.SINGLE_DB_NAME) {
            String dbName = req.getDbName();
            List<ClusterInfoDto> clusterInfoDtoList = dbaApiService.getDatabaseClusterInfo(dbName);
            list = Collections.singletonList(new DbClusterInfoDto(dbName, clusterInfoDtoList));
        } else {
            list = dbaApiService.getDatabaseClusterInfoList(req.getDalClusterName());
        }
        return list;
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
        return list;
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void autoBuildDrc(DrcAutoBuildReq req) throws Exception {
        this.validateReq(req);
        List<DrcAutoBuildParam> params = this.getDrcBuildParam(req);
        logger.info("autoBuildDrc params: {}", params);
        for (DrcAutoBuildParam param : params) {
            DefaultTransactionMonitorHolder.getInstance()
                    .logTransaction("DRC.auto.build", "all", () -> this.autoBuildDrc(param));
        }
    }

    @Override
    public List<DrcAutoBuildParam> getDrcBuildParam(DrcAutoBuildReq req) {
        this.validReqRegions(req);
        List<DrcAutoBuildParam> drcAutoBuildParams;
        DrcAutoBuildReq.BuildMode modeEnum = req.getModeEnum();
        if (modeEnum == DrcAutoBuildReq.BuildMode.DAL_CLUSTER_NAME) {
            drcAutoBuildParams = this.buildParamFromDalCluster(req);
        } else if (modeEnum == DrcAutoBuildReq.BuildMode.SINGLE_DB_NAME) {
            drcAutoBuildParams = Collections.singletonList(this.buildParamFromSingleDb(req));
        } else {
            throw new IllegalArgumentException("req is not DrcAutoBuildByDalClusterNameReq or DrcAutoBuildBySingleDbNameReq");
        }
        this.buildCommonParams(req, drcAutoBuildParams);
        return drcAutoBuildParams;
    }

    private void buildCommonParams(DrcAutoBuildReq req, List<DrcAutoBuildParam> params) {
        for (DrcAutoBuildParam param : params) {
            param.setBuName(req.getBuName());
            param.setTableFilter(req.getTblsFilterDetail().getTableNames());
            param.setTag(this.getTagByBuName(req.getBuName()));
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
        this.validReqRegions(req);
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

    public DrcAutoBuildParam buildParamFromSingleDb(DrcAutoBuildReq req) {
        List<ClusterInfoDto> clusterInfoDtoList = dbaApiService.getDatabaseClusterInfo(req.getDbName());

        DbClusterInfoDto dbClusterInfoDto = new DbClusterInfoDto(req.getDbName(), clusterInfoDtoList);
        List<MhaReplicationPreviewDto> mhaReplicationPreviewDtos = this.getMhaReplicationPreviewDtos(req.getSrcRegionName(), req.getDstRegionName(), Collections.singletonList(dbClusterInfoDto));
        if (CollectionUtils.isEmpty(mhaReplicationPreviewDtos) || mhaReplicationPreviewDtos.size() != 1) {
            throw new ConsoleException("unexpected mhaReplicationPreviewDtos: " + mhaReplicationPreviewDtos);
        }
        MhaReplicationPreviewDto previewDto = mhaReplicationPreviewDtos.get(0);
        if (!previewDto.normalCase()) {
            throw ConsoleExceptionUtils.message(AutoBuildErrorEnum.DRC_MULTI_MHA_OPTIONS_IN_SAME_REGION_NOT_SUPPORTED, this.buildErrorMessage(Collections.singletonList(previewDto)));
        }
        MhaDto srcMha = previewDto.getSrcMha();
        MhaDto dstMha = previewDto.getDstMha();

        DrcAutoBuildParam drcAutoBuildParam = new DrcAutoBuildParam();
        drcAutoBuildParam.setSrcMhaName(srcMha.getName());
        drcAutoBuildParam.setDstMhaName(dstMha.getName());
        drcAutoBuildParam.setSrcDcName(srcMha.getDcName());
        drcAutoBuildParam.setDstDcName(dstMha.getDcName());
        drcAutoBuildParam.setDbName(Sets.newHashSet(req.getDbName()));

        return drcAutoBuildParam;
    }

    public List<DrcAutoBuildParam> buildParamFromDalCluster(DrcAutoBuildReq req) {
        List<MhaReplicationPreviewDto> mhaReplicationPreviewDtos = this.preCheckMhaReplication(req);
        List<MhaReplicationPreviewDto> multiMhaOptions = mhaReplicationPreviewDtos.stream().filter(e -> !e.normalCase()).collect(Collectors.toList());
        if (!CollectionUtils.isEmpty(multiMhaOptions)) {
            throw ConsoleExceptionUtils.message(AutoBuildErrorEnum.DRC_MULTI_MHA_OPTIONS_IN_SAME_REGION_NOT_SUPPORTED, this.buildErrorMessage(multiMhaOptions));
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
            list.add(param);
        }
        return list;
    }

    private String buildErrorMessage(List<MhaReplicationPreviewDto> notValidDB) {
        StringBuilder sb = new StringBuilder();
        for (MhaReplicationPreviewDto previewDto : notValidDB) {
            if (!CollectionUtils.isEmpty(previewDto.getSrcOptionalMha()) && previewDto.getSrcOptionalMha().size() > 2) {
                sb.append("multi options: src mha: ").append(previewDto.getSrcOptionalMha()).append(" in region ").append(previewDto.getSrcRegionName());
            }

            if (!CollectionUtils.isEmpty(previewDto.getDstOptionalMha()) && previewDto.getDstOptionalMha().size() > 2) {
                sb.append("multi options: src mha: ").append(previewDto.getDstOptionalMha()).append(" in region ").append(previewDto.getDstRegionName());
            }
        }
        return sb.toString();
    }

    private void autoBuildDrc(DrcAutoBuildParam param) throws Exception {
        // 1.(if needed) build mha, mha replication
        DrcMhaBuildParam mhaBuildParam = new DrcMhaBuildParam(param.getSrcMhaName(), param.getDstMhaName(), param.getSrcDcName(), param.getDstDcName(), param.getBuName(), param.getTag(), param.getTag());
        drcBuildService.buildMha(mhaBuildParam);
        if (param.getSrcMhaName().equals(param.getDstMhaName())) {
            throw ConsoleExceptionUtils.message(AutoBuildErrorEnum.DRC_MHA_NOT_SUPPORTED, String.format("src: %s, dst: %s", param.getSrcMhaName(), param.getDstMhaName()));
        }
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
        drcBuildService.syncMhaDbInfoFromDbaApiIfNeeded(srcMhaTbl);
        drcBuildService.syncMhaDbInfoFromDbaApiIfNeeded(dstMhaTbl);

        // 3. config dbReplications
        // 3.1 base
        DbReplicationBuildParam dbReplicationBuildParam = new DbReplicationBuildParam();
        dbReplicationBuildParam.setSrcMhaName(srcMhaTbl.getMhaName());
        dbReplicationBuildParam.setDstMhaName(dstMhaTbl.getMhaName());
        dbReplicationBuildParam.setDbName(param.getDbNameFilter());
        dbReplicationBuildParam.setTableName(param.getTableFilter());
        List<Long> replicationIds = drcBuildService.configureDbReplications(dbReplicationBuildParam);
        // 3.2 filters
        if (param.getRowsFilterCreateParam() != null) {
            param.getRowsFilterCreateParam().setDbReplicationIds(replicationIds);
            drcBuildService.buildRowsFilter(param.getRowsFilterCreateParam());
        }

        if (param.getColumnsFilterCreateParam() != null) {
            param.getColumnsFilterCreateParam().setDbReplicationIds(replicationIds);
            drcBuildService.buildColumnsFilter(param.getColumnsFilterCreateParam());
        }

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

    private static MhaDto buildMhaDto(ClusterInfoDto clusterInfoDto, DcDo dcDo) {
        MhaDto mhaDto = new MhaDto();
        mhaDto.setName(clusterInfoDto.getClusterName());
        mhaDto.setDcId(dcDo.getDcId());
        mhaDto.setDcName(dcDo.getDcName());
        mhaDto.setRegionName(dcDo.getRegionName());
        return mhaDto;
    }

    private String getTagByBuName(String buName) {
        Map<String, String> bu2TagMap = consoleConfig.getBu2TagMap();
        return bu2TagMap.getOrDefault(buName, ResourceTagEnum.COMMON.getName());
    }
}
