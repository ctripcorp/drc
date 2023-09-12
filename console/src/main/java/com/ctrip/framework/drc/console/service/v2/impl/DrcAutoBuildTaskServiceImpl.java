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
import com.ctrip.framework.drc.console.enums.ReadableErrorDefEnum;
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
import com.ctrip.framework.drc.console.utils.EnvUtils;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.console.vo.display.v2.MhaReplicationPreviewDto;
import com.ctrip.framework.drc.console.vo.v2.DbReplicationView;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.collections.keyvalue.MultiKey;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by dengquanliang
 * 2023/7/27 15:43
 */
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
    public List<MhaReplicationPreviewDto> previewAutoBuildOptions(DrcAutoBuildReq req) {
        List<DbClusterInfoDto> list = this.getDbClusterInfoDtos(req);
        return getMhaReplicationPreviewDtos(req.getSrcRegionName(), req.getDstRegionName(), list);
    }

    @Override
    public List<String> getRegionOptions(DrcAutoBuildReq req) {
        List<DbClusterInfoDto> list = this.getDbClusterInfoDtos(req);
        return getRegionNameOptions(list);
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

                if (srcRegionName.equals(regionName)) {
                    MhaDto dto = buildMhaDto(clusterInfoDto, dcDo);
                    srcOptionalList.add(dto);
                }
                if (dstRegionName.equals(regionName)) {
                    MhaDto dto = buildMhaDto(clusterInfoDto, dcDo);
                    dstOptionalList.add(dto);
                }
            }
            list.add(mhaReplicationPreviewDto);
        }
        return list;
    }

    @Override
    public void autoBuildDrc(DrcAutoBuildReq req) throws Exception {
        this.validateReq(req);
        List<DrcAutoBuildParam> params = this.getDrcBuildParam(req);
        logger.info("autoBuildDrc params: {}", params);
        for (DrcAutoBuildParam param : params) {
            this.autoBuildDrc(param);
        }
    }

    @Override
    public List<DrcAutoBuildParam> getDrcBuildParam(DrcAutoBuildReq req) {

        DrcAutoBuildReq.BuildMode modeEnum = req.getModeEnum();
        if (modeEnum == DrcAutoBuildReq.BuildMode.DAL_CLUSTER_NAME) {
            return this.buildParamFromDalCluster(req);
        } else if (modeEnum == DrcAutoBuildReq.BuildMode.SINGLE_DB_NAME) {
            return Collections.singletonList(this.buildParamFromSingleDb(req));
        } else {
            throw new IllegalArgumentException("req is not DrcAutoBuildByDalClusterNameReq or DrcAutoBuildBySingleDbNameReq");
        }
    }

    private void validateReq(DrcAutoBuildReq req) {
        req.validAndTrim();
        List<DcDo> dcDos = metaInfoService.queryAllDcWithCache();
        Set<String> regionSet = dcDos.stream().map(DcDo::getRegionName).collect(Collectors.toSet());
        if (req.getSrcRegionName().equals(req.getDstRegionName())) {
            throw ConsoleExceptionUtils.message(AutoBuildErrorEnum.DRC_SAME_REGION_NOT_SUPPORTED);
        }
        if (!regionSet.contains(req.getSrcRegionName())) {
            throw new IllegalArgumentException("srcRegionName is not in dcDos: " + req.getSrcRegionName());
        }
        if (!regionSet.contains(req.getDstRegionName())) {
            throw new IllegalArgumentException("dstRegionName is not in dcDos: " + req.getDstRegionName());
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
        drcAutoBuildParam.setBuName(req.getBuName());
        drcAutoBuildParam.setDbName(Sets.newHashSet(req.getBuName()));
        drcAutoBuildParam.setTableFilter(req.getTblsFilterDetail().getTableNames());
        return drcAutoBuildParam;
    }

    public List<DrcAutoBuildParam> buildParamFromDalCluster(DrcAutoBuildReq req) {
        List<MhaReplicationPreviewDto> mhaReplicationPreviewDtos = this.previewAutoBuildOptions(req);
        List<MhaReplicationPreviewDto> multiMhaOptions = mhaReplicationPreviewDtos.stream().filter(e -> !e.normalCase()).collect(Collectors.toList());
        if (!CollectionUtils.isEmpty(multiMhaOptions)) {
            throw ConsoleExceptionUtils.message(AutoBuildErrorEnum.DRC_MULTI_MHA_OPTIONS_IN_SAME_REGION_NOT_SUPPORTED, this.buildErrorMessage(multiMhaOptions));
        }

        List<DrcAutoBuildParam> list = new ArrayList<>();
        // grouping by mha replication
        Map<MultiKey, List<MhaReplicationPreviewDto>> map = mhaReplicationPreviewDtos.stream().collect(Collectors.groupingBy(e -> new MultiKey(e.getSrcMha().getName(), e.getDstMha().getName())));
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

            param.setBuName(req.getBuName());
            param.setTableFilter(req.getTblsFilterDetail().getTableNames());
            param.setTag(null);
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

    private Set<String> getMhaName(List<ClusterInfoDto> clusterInfoDtoList) {
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


    private void autoBuildDrc(DrcAutoBuildParam param) throws Exception {

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
        // todo by yongnian: 2023/9/13 check conflicts with exist
        // todo by yongnian: 2023/9/13 test
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
