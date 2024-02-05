package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.aop.log.LogRecord;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.enums.operation.OperateAttrEnum;
import com.ctrip.framework.drc.console.enums.operation.OperateTypeEnum;
import com.ctrip.framework.drc.console.param.v2.DbQuery;
import com.ctrip.framework.drc.console.param.v2.DrcAutoBuildParam;
import com.ctrip.framework.drc.console.param.v2.DrcAutoBuildReq;
import com.ctrip.framework.drc.console.service.v2.DrcAutoBuildService;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.ctrip.framework.drc.console.vo.check.TableCheckVo;
import com.ctrip.framework.drc.console.vo.display.v2.MhaReplicationPreviewDto;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/drc/v2/autoconfig/")
public class DbDrcBuildControllerV2 {
    private static final Logger logger = LoggerFactory.getLogger(DbDrcBuildControllerV2.class);

    @Autowired
    private DrcAutoBuildService drcAutoBuildService;

    @Autowired
    private MetaInfoServiceV2 metaInfoService;

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
            List<MhaReplicationPreviewDto> list = drcAutoBuildService.preCheckMhaReplication(req);
            return ApiResult.getSuccessInstance(list);
        } catch (Throwable e) {
            logger.error("[meta] preCheck, req {}", req, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("preCheckTable")
    @SuppressWarnings("unchecked")
    public ApiResult<List<TableCheckVo>> preCheckTable(DrcAutoBuildReq req) {
        logger.info("[meta] preCheckTable, req: {}", req);
        try {
            List<TableCheckVo> list = drcAutoBuildService.preCheckMysqlTables(req);
            return ApiResult.getSuccessInstance(list);
        } catch (Throwable e) {
            logger.error("[meta] preCheckTable, req {}", req, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("regionOptions")
    @SuppressWarnings("unchecked")
    public ApiResult<List<String>> getRegionOptions(DrcAutoBuildReq req) {
        logger.info("[meta] regionOptions, req: {}", req);
        try {
            List<String> list = drcAutoBuildService.getRegionOptions(req);
            return ApiResult.getSuccessInstance(list);
        } catch (Throwable e) {
            logger.error("[meta] regionOptions, req {}", req, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("region/all")
    @SuppressWarnings("unchecked")
    public ApiResult<List<String>> getAllRegions() {
        try {
            List<String> list = drcAutoBuildService.getAllRegions();
            return ApiResult.getSuccessInstance(list);
        } catch (Throwable e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("commonColumns")
    @SuppressWarnings("unchecked")
    public ApiResult<List<String>> getCommonColumns(DrcAutoBuildReq req) {
        logger.info("[meta] commonColumns, req: {}", req);
        try {
            List<String> list = drcAutoBuildService.getCommonColumn(req);
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
            List<DrcAutoBuildParam> drcAutoBuildParams = drcAutoBuildService.getDrcBuildParam(req);
            return ApiResult.getSuccessInstance(drcAutoBuildParams);
        } catch (Throwable e) {
            logger.error("[meta] preCheckBuildParam, req {}", req, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PostMapping("submit")
    @SuppressWarnings("unchecked")
    @LogRecord(type = OperateTypeEnum.MHA_REPLICATION, attr = OperateAttrEnum.UPDATE,
            success = "DbDrcBuildControllerV2 submit with DrcAutoBuildReq: {#req.toString()}")
    public ApiResult<Void> submit(@RequestBody DrcAutoBuildReq req) {
        logger.info("[meta] auto build drc, req: {}", req);
        try {
            drcAutoBuildService.autoBuildDrc(req);
            return ApiResult.getSuccessInstance(null);
        } catch (Throwable e) {
            logger.error("[meta] auto build drc, req {}", req, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }


}
