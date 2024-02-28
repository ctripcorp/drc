package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.aop.log.LogRecord;
import com.ctrip.framework.drc.console.dto.MessengerMetaDto;
import com.ctrip.framework.drc.console.dto.v3.DbApplierDto;
import com.ctrip.framework.drc.console.enums.operation.OperateAttrEnum;
import com.ctrip.framework.drc.console.enums.operation.OperateTypeEnum;
import com.ctrip.framework.drc.console.param.v2.*;
import com.ctrip.framework.drc.console.service.v2.DbDrcBuildService;
import com.ctrip.framework.drc.console.service.v2.DrcBuildServiceV2;
import com.ctrip.framework.drc.console.vo.v2.ColumnsConfigView;
import com.ctrip.framework.drc.console.vo.v2.DbReplicationView;
import com.ctrip.framework.drc.console.vo.v2.RowsFilterConfigView;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
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
@RequestMapping("/api/drc/v2/config/")
public class DrcBuildControllerV2 {
    private static final Logger logger = LoggerFactory.getLogger(DrcBuildControllerV2.class);

    @Autowired
    private DrcBuildServiceV2 drcBuildServiceV2;
    @Autowired
    private DbDrcBuildService dbDrcBuildService;

    @PostMapping("mha")
    @LogRecord(type = OperateTypeEnum.MHA_REPLICATION, attr = OperateAttrEnum.ADD,
            success = "buildMha with DrcMhaBuildParam: {#param.toString()}")
    public ApiResult<Boolean> buildMha(@RequestBody DrcMhaBuildParam param) {
        try {
            drcBuildServiceV2.buildMha(param);
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @PostMapping("messengerMha")
    @LogRecord(type = OperateTypeEnum.MHA_REPLICATION, attr = OperateAttrEnum.ADD,
            success = "buildMessengerMha with MessengerMhaBuildParam: {#param.toString()}")
    public ApiResult<Boolean> buildMessengerMha(@RequestBody MessengerMhaBuildParam param) {
        logger.info("buildMessengerMha: {}", param);
        try {
            drcBuildServiceV2.buildMessengerMha(param);
            return ApiResult.getSuccessInstance(true);
        } catch (Throwable e) {
            logger.error("buildMessengerMha exception. req: " + param, e);
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @PostMapping("")
    @LogRecord(type = OperateTypeEnum.MHA_REPLICATION, attr = OperateAttrEnum.UPDATE,
            success = "buildDrc with DrcBuildParam: {#param.toString()}")
    public String buildDrc(@RequestBody DrcBuildParam param) {
        try {
            return drcBuildServiceV2.buildDrc(param);
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    @PostMapping("db/applier")
    @LogRecord(type = OperateTypeEnum.MHA_REPLICATION, attr = OperateAttrEnum.UPDATE,
            success = "buildDbApplier with DrcBuildParam: {#param.toString()}")
    public ApiResult<String> buildDrcDbAppliers(@RequestBody DrcBuildParam param) {
        try {
            return ApiResult.getSuccessInstance(dbDrcBuildService.buildDbApplier(param));
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PostMapping("db/messenger")
    @LogRecord(type = OperateTypeEnum.MESSENGER_REPLICATION, attr = OperateAttrEnum.UPDATE,
            success = "buildDbMessenger with DrcBuildBaseParam: {#param.toString()}")
    public ApiResult<String> buildDrcDbMessenger(@RequestBody DrcBuildBaseParam param) {
        try {
            return ApiResult.getSuccessInstance(dbDrcBuildService.buildDbMessenger(param));
        } catch (Exception e) {
            return ApiResult.getFailInstance(e);
        }
    }

    @GetMapping("mha/applier")
    public ApiResult<List<String>> getMhaAppliers(@RequestParam String srcMhaName, @RequestParam String dstMhaName) {
        try {
            return ApiResult.getSuccessInstance(drcBuildServiceV2.getMhaAppliers(srcMhaName, dstMhaName));
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("mha/dbApplier")
    public ApiResult<List<DbApplierDto>> getMhaDbAppliers(@RequestParam String srcMhaName, @RequestParam String dstMhaName) {
        try {
            return ApiResult.getSuccessInstance(dbDrcBuildService.getMhaDbAppliers(srcMhaName, dstMhaName));
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("mha/dbApplier/gtidTruncated")
    public ApiResult<String> getMhaDrcExecutedGtidTruncate(@RequestParam String srcMhaName, @RequestParam String dstMhaName) {
        try {
            return ApiResult.getSuccessInstance(dbDrcBuildService.getMhaDrcExecutedGtidTruncate(srcMhaName, dstMhaName));
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("mha/dbApplier/dbGtidTruncated")
    public ApiResult<String> getDbDrcExecutedGtidTruncate(@RequestParam String srcMhaName, @RequestParam String dstMhaName) {
        try {
            return ApiResult.getSuccessInstance(dbDrcBuildService.getDbDrcExecutedGtidTruncate(srcMhaName, dstMhaName));
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("mha/dbApplier/gtid")
    public ApiResult<String> getMhaDrcExecutedGtid(@RequestParam String srcMhaName, @RequestParam String dstMhaName) {
        try {
            return ApiResult.getSuccessInstance(dbDrcBuildService.getMhaDrcExecutedGtid(srcMhaName, dstMhaName).toString());
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("mha/dbApplier/dbGtid")
    public ApiResult<String> getDbDrcExecutedGtid(@RequestParam String srcMhaName, @RequestParam String dstMhaName) {
        try {
            Map<String, GtidSet> dbDrcExecutedGtid = dbDrcBuildService.getDbDrcExecutedGtid(srcMhaName, dstMhaName);
            Map<String, String> map = dbDrcExecutedGtid.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
            return ApiResult.getSuccessInstance(map);
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("mha/dbMessenger")
    public ApiResult<List<DbApplierDto>> getMhaDbMessengers(@RequestParam String mhaName) {
        try {
            return ApiResult.getSuccessInstance(dbDrcBuildService.getMhaDbMessengers(mhaName));
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("mha/dbApplier/switch")
    public ApiResult<Boolean> getMhaDbAppliers(@RequestParam String mhaName) {
        try {
            return ApiResult.getSuccessInstance(dbDrcBuildService.isDbApplierConfigurable(mhaName));
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("mha/applierGtid")
    public ApiResult<String> getApplierGtid(@RequestParam String srcMhaName, @RequestParam String dstMhaName) {
        try {
            return ApiResult.getSuccessInstance(drcBuildServiceV2.getApplierGtid(srcMhaName, dstMhaName));
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PostMapping("dbReplications")
    @LogRecord(type = OperateTypeEnum.MHA_REPLICATION, attr = OperateAttrEnum.UPDATE,
            success = "configureDbReplication with DbReplicationBuildParam: {#param.toString()}")
    public ApiResult<Boolean> configureDbReplication(@RequestBody DbReplicationBuildParam param) {
        try {
            drcBuildServiceV2.buildDbReplicationConfig(param);
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("dbReplication")
    public ApiResult<List<DbReplicationView>> getDbReplicationView(@RequestParam String srcMhaName, @RequestParam String dstMhaName) {
        try {
            return ApiResult.getSuccessInstance(drcBuildServiceV2.getDbReplicationView(srcMhaName, dstMhaName));
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @DeleteMapping("dbReplications")
    @LogRecord(type = OperateTypeEnum.MHA_REPLICATION, attr = OperateAttrEnum.DELETE,
            success = "deleteDbReplications with dbReplicationIds: {#dbReplicationIds}")
    public ApiResult<Boolean> deleteDbReplications(@RequestParam List<Long> dbReplicationIds) {
        try {
            drcBuildServiceV2.deleteDbReplications(dbReplicationIds);
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @GetMapping("dbReplications/check")
    public ApiResult<Boolean> checkDbReplicationFilter(@RequestParam List<Long> dbReplicationIds) {
        try {
            drcBuildServiceV2.checkDbReplicationFilter(dbReplicationIds);
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @GetMapping("columnsFilter")
    public ApiResult<ColumnsConfigView> getColumnsConfigView(@RequestParam long dbReplicationId) {
        try {
            return ApiResult.getSuccessInstance(drcBuildServiceV2.getColumnsConfigView(dbReplicationId));
        } catch (Exception e) {
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @GetMapping("rowsFilter")
    public ApiResult<RowsFilterConfigView> getRowsConfigView(@RequestParam long dbReplicationId) {
        try {
            return ApiResult.getSuccessInstance(drcBuildServiceV2.getRowsConfigView(dbReplicationId));
        } catch (Exception e) {
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @PostMapping("messenger/submitConfig")
    @LogRecord(type = OperateTypeEnum.MESSENGER_REPLICATION, attr = OperateAttrEnum.UPDATE,
            success = "submitConfig with MessengerMetaDto: {#dto.toString()}")
    public ApiResult<Void> submitConfig(@RequestBody MessengerMetaDto dto) {
        logger.info("[meta] submit meta config for {}", dto);
        try {
            String xml = drcBuildServiceV2.buildMessengerDrc(dto);
            return ApiResult.getSuccessInstance(xml);
        } catch (Throwable e) {
            logger.error("[meta] submit meta config for for {}", dto, e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PostMapping("initReplicationTables")
    public ApiResult<Boolean> initReplicationTables() {
        try {
            drcBuildServiceV2.initReplicationTables();
            return ApiResult.getSuccessInstance(true);
        } catch (Throwable e) {
            logger.error("initReplicationTables fail, ", e);
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @DeleteMapping("replicationTables")
    public ApiResult<Boolean> deleteReplicationTables() {
        try {
            drcBuildServiceV2.deleteAllReplicationTables();
            return ApiResult.getSuccessInstance(true);
        } catch (Throwable e) {
            logger.error("deleteReplicationTables fail, ", e);
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }
}
