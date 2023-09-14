package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.param.v2.DbQuery;
import com.ctrip.framework.drc.console.param.v2.DrcAutoBuildParam;
import com.ctrip.framework.drc.console.param.v2.DrcAutoBuildReq;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;
import com.ctrip.framework.drc.console.service.v2.DrcAutoBuildTaskService;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.ctrip.framework.drc.console.service.v2.external.dba.DbaApiService;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.ClusterInfoDto;
import com.ctrip.framework.drc.console.vo.check.TableCheckVo;
import com.ctrip.framework.drc.console.vo.display.v2.MhaReplicationPreviewDto;
import com.ctrip.framework.drc.console.vo.display.v2.MhaVo;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by dengquanliang
 * 2023/8/9 16:58
 */
@RestController
@RequestMapping("/api/drc/v2/autoconfig/")
public class DbDrcBuildControllerV2 {
    private static final Logger logger = LoggerFactory.getLogger(DbDrcBuildControllerV2.class);

    @Autowired
    private DrcAutoBuildTaskService drcAutoBuildTaskService;

    @Autowired
    private DbaApiService dbaApiService;

    @Autowired
    private DefaultConsoleConfig consoleConfig;

    @Autowired
    private MetaInfoServiceV2 metaInfoService;


    @GetMapping("getDbClusterInfo")
    @SuppressWarnings("unchecked")
    public ApiResult<List<MhaVo>> getDbClusterInfo(@RequestParam("dbName") String dbName) {
        logger.info("[meta] getClusterInfo, dbName: {}", dbName);
        try {
            List<ClusterInfoDto> clusterInfoDtoList = dbaApiService.getDatabaseClusterInfo(dbName);
            Map<String, String> dbaDc2DrcDcMap = consoleConfig.getDbaDc2DrcDcMap();
            List<DcDo> dcDos = metaInfoService.queryAllDcWithCache();
            Map<String, DcDo> dcMap = dcDos.stream().collect(Collectors.toMap(DcDo::getDcName, e -> e));

            List<MhaVo> mhaVos = clusterInfoDtoList.stream().map(e -> {
                String dcName = dbaDc2DrcDcMap.get(e.getZoneId().toLowerCase());
                DcDo dcDo = dcMap.get(dcName);

                MhaVo vo = new MhaVo();
                vo.setName(e.getClusterName());
                vo.setDcId(dcDo.getDcId());
                vo.setDcName(dcDo.getDcName());
                vo.setRegionName(dcDo.getRegionName());
                return vo;
            }).collect(Collectors.toList());
            return ApiResult.getSuccessInstance(mhaVos);
        } catch (Throwable e) {
            logger.error("[meta] getClusterInfo, dbName {}", dbName, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("getExistDb")
    @SuppressWarnings("unchecked")
    public ApiResult<List<DbTbl>> getExistDb(@RequestParam("dbName") String dbName) {
        try {
            DbQuery query = new DbQuery();
            query.setLikeByDbNameFromBeginning(dbName);
            query.setPageIndex(1);
            query.setPageSize(20);
            List<DbTbl> dbTbls = metaInfoService.queryDbByPage(query);
            return ApiResult.getSuccessInstance(dbTbls);
        } catch (Throwable e) {
            logger.error("[meta] getClusterInfo, dbName {}", dbName, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("preCheck")
    @SuppressWarnings("unchecked")
    public ApiResult<List<MhaReplicationPreviewDto>> preCheck(DrcAutoBuildReq req) {
        logger.info("[meta] preCheck, req: {}", req);
        try {
            List<MhaReplicationPreviewDto> list = drcAutoBuildTaskService.preCheckMhaReplication(req);
            return ApiResult.getSuccessInstance(list);
        } catch (Throwable e) {
            logger.error("[meta] preCheck, req {}", req, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("preCheckTable")
    @SuppressWarnings("unchecked")
    public ApiResult<List<TableCheckVo>> preCheckTable(DrcAutoBuildReq req) {
        logger.info("[meta] preCheck, req: {}", req);
        try {
            List<TableCheckVo> list = drcAutoBuildTaskService.preCheckMysqlTables(req);
            return ApiResult.getSuccessInstance(list);
        } catch (Throwable e) {
            logger.error("[meta] preCheck, req {}", req, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("regionOptions")
    @SuppressWarnings("unchecked")
    public ApiResult<List<String>> getRegionOptions(DrcAutoBuildReq req) {
        logger.info("[meta] preCheck, req: {}", req);
        try {
            List<String> list = drcAutoBuildTaskService.getRegionOptions(req);
            return ApiResult.getSuccessInstance(list);
        } catch (Throwable e) {
            logger.error("[meta] preCheck, req {}", req, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("commonColumns")
    @SuppressWarnings("unchecked")
    public ApiResult<List<String>> getCommonColumns(DrcAutoBuildReq req) {
        logger.info("[meta] commonColumns, req: {}", req);
        try {
            List<String> list = drcAutoBuildTaskService.getCommonColumn(req);
            return ApiResult.getSuccessInstance(list);
        } catch (Throwable e) {
            logger.error("[meta] commonColumns, req {}", req, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }


    @PostMapping("preCheckBuildParam")
    @SuppressWarnings("unchecked")
    public ApiResult<List<DrcAutoBuildParam>> preCheckBuildParam(@RequestBody DrcAutoBuildReq req) {
        logger.info("[meta] preCheckBuildParam, req: {}", req);
        try {
            List<DrcAutoBuildParam> drcAutoBuildParams = drcAutoBuildTaskService.getDrcBuildParam(req);
            return ApiResult.getSuccessInstance(drcAutoBuildParams);
        } catch (Throwable e) {
            logger.error("[meta] preCheckBuildParam, req {}", req, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PostMapping("submit")
    @SuppressWarnings("unchecked")
    public ApiResult<Void> submit(@RequestBody DrcAutoBuildReq req) {
        logger.info("[meta] auto build drc, req: {}", req);
        try {
            drcAutoBuildTaskService.autoBuildDrc(req);
            return ApiResult.getSuccessInstance(null);
        } catch (Throwable e) {
            logger.error("[meta] auto build drc, req {}", req, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }


}
