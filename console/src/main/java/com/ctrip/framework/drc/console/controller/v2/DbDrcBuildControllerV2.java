package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.aop.log.LogRecord;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dto.v3.DbDrcConfigInfoDto;
import com.ctrip.framework.drc.console.dto.v3.DbReplicationCreateDto;
import com.ctrip.framework.drc.console.dto.v3.DbReplicationEditDto;
import com.ctrip.framework.drc.console.dto.v3.MhaDbReplicationCreateDto;
import com.ctrip.framework.drc.console.enums.operation.OperateAttrEnum;
import com.ctrip.framework.drc.console.enums.operation.OperateTypeEnum;
import com.ctrip.framework.drc.console.param.v2.DbQuery;
import com.ctrip.framework.drc.console.param.v2.DrcAutoBuildParam;
import com.ctrip.framework.drc.console.param.v2.DrcAutoBuildReq;
import com.ctrip.framework.drc.console.service.v2.DbDrcBuildService;
import com.ctrip.framework.drc.console.service.v2.DrcAutoBuildService;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.ctrip.framework.drc.console.utils.NumberUtils;
import com.ctrip.framework.drc.console.vo.check.TableCheckVo;
import com.ctrip.framework.drc.console.vo.display.v2.MhaReplicationPreviewDto;
import com.ctrip.framework.drc.console.vo.v2.ColumnsConfigView;
import com.ctrip.framework.drc.console.vo.v2.RowsFilterConfigView;
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
    private DbDrcBuildService dbDrcBuildService;

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

    @GetMapping("getExistDbReplicationDirections")
    @SuppressWarnings("unchecked")
    public ApiResult<List<DbDrcConfigInfoDto>> getExistDbDrcConfigDcOption(DrcAutoBuildReq req) {
        logger.info("[meta] getDbDrcConfig, req: {}", req);
        try {
            return ApiResult.getSuccessInstance(dbDrcBuildService.getExistDbReplicationDirections(req.getDbName()));
        } catch (Throwable e) {
            logger.error("[meta] getDbDrcConfig, req {}", req, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }


    @GetMapping("getDbDrcConfig")
    @SuppressWarnings("unchecked")
    public ApiResult<DbDrcConfigInfoDto> getDbDrcConfig(DrcAutoBuildReq req) {
        logger.info("[meta] getDbDrcConfig, req: {}", req);
        try {
            DbDrcConfigInfoDto dbDrcConfig = dbDrcBuildService.getDbDrcConfig(req.getDbName(), req.getSrcRegionName(), req.getDstRegionName());
            return ApiResult.getSuccessInstance(dbDrcConfig);
        } catch (Throwable e) {
            logger.error("[meta] getDbDrcConfig, req {}", req, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("getRowsFilterView")
    @SuppressWarnings("unchecked")
    public ApiResult<RowsFilterConfigView> getRowsFilterView(@RequestParam("rowsFilterId") Long rowsFilterId) {
        logger.info("[meta] getRowsFilterView, id: {}", rowsFilterId);
        try {
            if (!NumberUtils.isPositive(rowsFilterId)) {
                return ApiResult.getFailInstance(null, "illegal rowsFilterId: " + rowsFilterId);
            }
            RowsFilterConfigView view = dbDrcBuildService.getRowsConfigViewById(rowsFilterId);
            return ApiResult.getSuccessInstance(view);
        } catch (Throwable e) {
            logger.error("[meta] getRowsFilterView, req {}", rowsFilterId, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("getColsFilterView")
    @SuppressWarnings("unchecked")
    public ApiResult<ColumnsConfigView> getColsFilterView(@RequestParam("colsFilterId") Long colsFilterId) {
        logger.info("[meta] ColumnsConfigView, id: {}", colsFilterId);
        try {
            if (!NumberUtils.isPositive(colsFilterId)) {
                return ApiResult.getFailInstance(null, "illegal colsFilterId: " + colsFilterId);
            }
            ColumnsConfigView view = dbDrcBuildService.getColumnsConfigViewById(colsFilterId);
            return ApiResult.getSuccessInstance(view);
        } catch (Throwable e) {
            logger.error("[meta] ColumnsConfigView, req {}", colsFilterId, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PostMapping("dbReplication/create")
    @SuppressWarnings("unchecked")
    public ApiResult<Boolean> createDbReplication(@RequestBody DbReplicationCreateDto dbReplicationCreateDto) {
        logger.info("[meta] createDbReplication, req {}", dbReplicationCreateDto);
        try {
            dbDrcBuildService.createDbReplication(dbReplicationCreateDto);
            return ApiResult.getSuccessInstance(true);
        } catch (Throwable e) {
            logger.error("[meta] createDbReplication, req {}", dbReplicationCreateDto, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }
    @PostMapping("dbReplication/edit")
    @SuppressWarnings("unchecked")
    public ApiResult<Boolean> editDbReplication(@RequestBody DbReplicationEditDto dbReplicationEditDto) {
        logger.info("[meta] editDbReplication, req {}", dbReplicationEditDto);
        try {
            dbDrcBuildService.editDbReplication(dbReplicationEditDto);
            return ApiResult.getSuccessInstance(true);
        } catch (Throwable e) {
            logger.error("[meta] editDbReplication, req {}", dbReplicationEditDto, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PostMapping("dbReplication/delete")
    @SuppressWarnings("unchecked")
    public ApiResult<Boolean> deleteDbReplication(@RequestBody DbReplicationEditDto dbReplicationEditDto) {
        logger.info("[meta] deleteDbReplication, req {}", dbReplicationEditDto);
        try {
            dbDrcBuildService.deleteDbReplication(dbReplicationEditDto);
            return ApiResult.getSuccessInstance(true);
        } catch (Throwable e) {
            logger.error("[meta] deleteDbReplication, req {}", dbReplicationEditDto, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PostMapping("mhaDbReplication/create")
    @SuppressWarnings("unchecked")
    public ApiResult<Boolean> createMhaDbDrcReplication(@RequestBody MhaDbReplicationCreateDto createDto) {
        logger.info("[meta] createDto, req {}", createDto);
        try {
            dbDrcBuildService.createMhaDbDrcReplication(createDto);
            return ApiResult.getSuccessInstance(true);
        } catch (Throwable e) {
            logger.error("[meta] createDto, req {}", createDto, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }


}
