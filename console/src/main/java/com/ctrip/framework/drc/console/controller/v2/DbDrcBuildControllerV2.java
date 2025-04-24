package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.aop.log.LogRecord;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dto.v3.*;
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
import com.ctrip.framework.drc.console.vo.display.v2.MhaPreCheckVo;
import com.ctrip.framework.drc.console.vo.display.v2.MhaReplicationPreviewDto;
import com.ctrip.framework.drc.console.vo.v2.ColumnsConfigView;
import com.ctrip.framework.drc.console.vo.v2.MqMetaCreateResultView;
import com.ctrip.framework.drc.console.vo.v2.RowsFilterConfigView;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.mq.MqType;
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
    
    @PostMapping("mhaInitBeforeBuild")
    @SuppressWarnings("unchecked")
    public ApiResult<Void> mhaInitBeforeBuild(@RequestBody DrcAutoBuildReq req) {
        logger.info("[meta] mhaInitBeforeBuild, req: {}", req);
        try {
            drcAutoBuildService.mhaInitBeforeBuild(req);
            return ApiResult.getSuccessInstance(null);
        } catch (Throwable e) {
            logger.error("[meta] mhaInitBeforeBuild, req {}", req, e);
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

    @GetMapping("preCheckMysqlConfig")
    @SuppressWarnings("unchecked")
    public ApiResult<MhaPreCheckVo> preCheckMysqlConfig(@RequestParam List<String> mhaList) {
        try {
            return ApiResult.getSuccessInstance(drcAutoBuildService.preCheckMysqlConfig(mhaList));
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

    @PostMapping("applicationForm/submit")
    @SuppressWarnings("unchecked")
    @LogRecord(type = OperateTypeEnum.MHA_REPLICATION, attr = OperateAttrEnum.UPDATE,
            success = "DbDrcBuildControllerV2 submitApplicationForm with DrcAutoBuildReq: {#req.toString()}")
    public ApiResult<Void> submitApplicationForm(@RequestBody DrcAutoBuildReq req) {
        logger.info("[meta] auto build drc, req: {}", req);
        try {
            drcAutoBuildService.autoBuildDrcFromApplication(req);
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

    @GetMapping("getExistDbMqConfigDcOption")
    @SuppressWarnings("unchecked")
    public ApiResult<List<DbMqConfigInfoDto>> getExistDbMqConfigDcOption(DrcAutoBuildReq req) {
        logger.info("[meta] getDbDrcConfig, req: {}", req);
        try {
            return ApiResult.getSuccessInstance(dbDrcBuildService.getExistDbMqConfigDcOption(req.getDbName(), MqType.valueOf(req.getMqType())));
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

    @GetMapping("getDbMqConfig")
    @SuppressWarnings("unchecked")
    public ApiResult<DbMqConfigInfoDto> getDbMQConfig(DrcAutoBuildReq req) {
        logger.info("[meta] getDbMQConfig, req: {}", req);
        try {
            DbMqConfigInfoDto dbMqConfig = dbDrcBuildService.getDbMqConfig(req.getDbName(), req.getSrcRegionName(), req.getMqTypeEnum());
            return ApiResult.getSuccessInstance(dbMqConfig);
        } catch (Throwable e) {
            logger.error("[meta] getDbMQConfig, req {}", req, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PostMapping("switchAppliers")
    @SuppressWarnings("unchecked")
    public ApiResult<Boolean> switchAppliers(@RequestBody List<DbApplierSwitchReqDto> reqDtos) {
        logger.info("[meta] autoBuildReq, req {}", reqDtos);
        try {
            dbDrcBuildService.switchAppliers(reqDtos);
            return ApiResult.getSuccessInstance(true);
        } catch (Throwable e) {
            logger.error("[meta] autoBuildReq, req {}", reqDtos, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PostMapping("switchMessengers")
    @SuppressWarnings("unchecked")
    public ApiResult<Boolean> switchMessengers(@RequestBody List<MessengerSwitchReqDto> reqDtos) {
        logger.info("[meta] autoBuildReq, req {}", reqDtos);
        try {
            dbDrcBuildService.switchMessengers(reqDtos);
            return ApiResult.getSuccessInstance(true);
        } catch (Throwable e) {
            logger.error("[meta] autoBuildReq, req {}", reqDtos, e);
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

    @PostMapping("dbMq/create")
    @SuppressWarnings("unchecked")
    public ApiResult<Boolean> createDbMqReplication(@RequestBody DbMqCreateDto dbMqCreateDto) {
        logger.info("[meta] createDbReplication, req {}", dbMqCreateDto);
        try {
            dbDrcBuildService.createDbMqReplication(dbMqCreateDto);
            return ApiResult.getSuccessInstance(true);
        } catch (Throwable e) {
            logger.error("[meta] createDbReplication, req {}", dbMqCreateDto, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PostMapping("dbMq/edit")
    @SuppressWarnings("unchecked")
    public ApiResult<Boolean> editDMqbReplication(@RequestBody DbMqEditDto dbMqEditDto) {
        logger.info("[meta] editDMqbReplication, req {}", dbMqEditDto);
        try {
            dbDrcBuildService.editDbMqReplication(dbMqEditDto);
            return ApiResult.getSuccessInstance(true);
        } catch (Throwable e) {
            logger.error("[meta] editDMqbReplication, req {}", dbMqEditDto, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PostMapping("dbMq/delete")
    @SuppressWarnings("unchecked")
    public ApiResult<Boolean> deleteDbMqReplication(@RequestBody DbMqEditDto dbMqEditDto) {
        logger.info("[meta] deleteDbReplication, req {}", dbMqEditDto);
        try {
            dbDrcBuildService.deleteDbMqReplication(dbMqEditDto);
            return ApiResult.getSuccessInstance(true);
        } catch (Throwable e) {
            logger.error("[meta] deleteDbReplication, req {}", dbMqEditDto, e);
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

    @PostMapping("mhaDbReplicationForMq/create")
    @SuppressWarnings("unchecked")
    public ApiResult<Boolean> createMhaDbReplicationForMq(@RequestBody MhaDbReplicationCreateDto createDto) {
        logger.info("[meta] createMhaDbReplicationForMq, req {}", createDto);
        try {
            dbDrcBuildService.createMhaDbReplicationForMq(createDto);
            return ApiResult.getSuccessInstance(true);
        } catch (Throwable e) {
            logger.error("[meta] createMhaDbReplicationForMq, req {}", createDto, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    /**
     * for other users
     */
    @PostMapping("autoCreateMq")
    @SuppressWarnings("unchecked")
    @LogRecord(type = OperateTypeEnum.AUTO_MQ_API, attr = OperateAttrEnum.ADD, operator = "AutoConfig",
            success = "createMqMeta with MqAutoCreateRequestDto:{#createDto.toString()}")
    public ApiResult<MqMetaCreateResultView> autoCreateMq(@RequestBody MqAutoCreateRequestDto createDto) {
        try {
            MqMetaCreateResultView resultMessage = dbDrcBuildService.autoCreateMq(createDto);
            if (resultMessage.getContainTables() == 0) {
                return ApiResult.getSuccessInstance(resultMessage, "this config already existed in DRC");
            }
            return ApiResult.getSuccessInstance(resultMessage);
        } catch (Throwable e) {
            logger.error("[meta] createMqBinlogMessage, req {}", createDto, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

}
