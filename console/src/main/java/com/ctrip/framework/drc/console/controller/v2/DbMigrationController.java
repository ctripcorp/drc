package com.ctrip.framework.drc.console.controller.v2;


import com.ctrip.framework.drc.console.aop.log.LogRecord;
import com.ctrip.framework.drc.console.dao.entity.v2.MigrationTaskTbl;
import com.ctrip.framework.drc.console.dto.v2.DbMigrationParam;
import com.ctrip.framework.drc.console.dto.v2.MhaApplierDto;
import com.ctrip.framework.drc.console.enums.MigrationStatusEnum;
import com.ctrip.framework.drc.console.enums.operation.OperateAttrEnum;
import com.ctrip.framework.drc.console.enums.operation.OperateTypeEnum;
import com.ctrip.framework.drc.console.enums.v2.MigrationTypeEnum;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.param.v2.MigrationTaskQuery;
import com.ctrip.framework.drc.console.service.v2.dbmigration.DbMigrationService;
import com.ctrip.framework.drc.console.vo.display.MigrationTaskVo;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.PageResult;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.*;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @ClassName DbMigrationController
 * @Author haodongPan
 * @Date 2023/8/14 11:30
 * @Version: $
 */
@RestController
@RequestMapping("/api/drc/v2/migration/")
public class DbMigrationController {

    private static final Logger logger = LoggerFactory.getLogger(DbMigrationController.class);
    @Autowired
    @Qualifier("dbMigrationServiceImplV2")
    private DbMigrationService dbMigrationServiceV2;


    @DeleteMapping("abandon")
    @LogRecord(type = OperateTypeEnum.DB_MIGRATION, attr = OperateAttrEnum.DELETE,operator = "admin",
            success = "abandonMigrationTask with taskId:{#taskId}")
    public ApiResult abandonMigrationTask(@RequestParam(name = "taskId") Long taskId) {
        try {
            if (dbMigrationServiceV2.abandonTask(taskId)) {
                return ApiResult.getInstance(null,0,"abandonMigrationTask: " + taskId + " success!");
            } else {
                return ApiResult.getInstance(null,1,"abandonMigrationTask: " + taskId + " fail!");
            }
        } catch (SQLException e) {
            logger.error("sql error in abandonMigrationTask", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        } catch (ConsoleException e) {
            logger.warn("abandonMigrationTask forbidden", e);
            return ApiResult.getInstance(null,1, e.getMessage());
        }
    }

    @PutMapping("beforeDataMigration/checkAndCreateTask")
    @LogRecord(type = OperateTypeEnum.DB_MIGRATION, attr = OperateAttrEnum.ADD,operator = "DBA",
            success = "dbMigrationCheckAndInit with DbMigrationParam:{#dbMigrationParam.toString()}")
    public ApiResult dbMigrationCheckAndInit(@RequestBody DbMigrationParam dbMigrationParam) {
        try {
            Pair<String, Long> tipsAndTaskId = dbMigrationServiceV2.dbMigrationCheckAndCreateTask(dbMigrationParam, MigrationTypeEnum.COMMON_INIT);
            if (tipsAndTaskId.getRight() == null) {
                return ApiResult.getInstance(null,2,"no dbDrcRelated");
            } else {
                return ApiResult.getInstance(tipsAndTaskId.getRight(),0,tipsAndTaskId.getLeft());
            }
        } catch (SQLException e) {
            logger.error("sql error in dbMigrationCheckAndInit", e);
            return ApiResult.getInstance(null,1, e.getMessage());
        } catch (ConsoleException e) {
            logger.warn("dbMigrationCheckAndInit forbidden", e);
            return ApiResult.getInstance(null,1, e.getMessage());
        }
    }
    
    @PostMapping("afterDataMigration/preStart")
    @LogRecord(type = OperateTypeEnum.DB_MIGRATION, attr = OperateAttrEnum.UPDATE,operator = "DBA",
            success = "preStartDbMigrationTask with taskId:{#taskId}")
    public ApiResult preStartDbMigrationTask(@RequestParam(name = "taskId") Long taskId) {
        try {
            boolean preStartResult = dbMigrationServiceV2.preStartDbMigrationTask(taskId, MigrationTypeEnum.COMMON_PRESTART);
            if (preStartResult) {
                return ApiResult.getInstance(null,0,"exStartDbMigrationTask: " + taskId + " success!");
            } else {
                return ApiResult.getInstance(null,1,"exStartDbMigrationTask: " + taskId + " fail!");
            }
        } catch (SQLException e) {
            logger.error("sql error in exStartDbMigrationTask", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        } catch (ConsoleException e) {
            logger.warn("exStartDbMigrationTask forbidden", e);
            return ApiResult.getInstance(null,1, e.getMessage());
        }
    }

    @GetMapping("beforeDrcStart/preStartStatus")
    public ApiResult checkPreStartStatus(@RequestParam(name = "taskId") Long taskId) {
        try {
            Pair<Boolean, String> res = dbMigrationServiceV2.checkPreStartStatus(taskId);
            if (res.getLeft()) {
                return ApiResult.getSuccessInstance(res.getRight());
            } else {
                return ApiResult.getSuccessInstance("notReady");
            }
        } catch (Throwable e) {
            logger.error("checkPreStartStatus error", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }
    
    @PostMapping("beforeDrcStart/cancel")
    @LogRecord(type = OperateTypeEnum.DB_MIGRATION, attr = OperateAttrEnum.UPDATE,operator = "DBA",
            success = "cancelDbMigrationTask with taskId:{#taskId}")
    public ApiResult cancelDbMigrationTask(@RequestParam(name = "taskId") Long taskId) {
        try {
            boolean cancelResult = dbMigrationServiceV2.cancelTask(taskId);
            if (cancelResult) {
                return ApiResult.getInstance(null,0,"cancelDbMigrationTask: " + taskId + " success!");
            } else {
                return ApiResult.getInstance(null,1,"cancelDbMigrationTask: " + taskId + " fail!");
            }
        } catch (SQLException e) {
            logger.error("sql error in cancelDbMigrationTask", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        } catch (ConsoleException e) {
            logger.warn("cancelDbMigrationTask forbidden", e);
            return ApiResult.getInstance(null,1, e.getMessage());
        } catch (Exception e) {
            logger.error("cancelDbMigrationTask unExcepted error", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }
    
    @PostMapping("beforeDalSwtich/start")
    @LogRecord(type = OperateTypeEnum.DB_MIGRATION, attr = OperateAttrEnum.UPDATE,operator = "DBA",
            success = "startDbMigrationTask with taskId:{#taskId}")
    public ApiResult startDbMigrationTask (@RequestParam(name = "taskId") Long taskId) {
        try {
            boolean startResult = dbMigrationServiceV2.startDbMigrationTask(taskId, MigrationTypeEnum.COMMON_START);
            if (startResult) {
                return ApiResult.getInstance(null,+
                        0,"startDbMigrationTask " + taskId + " success!");
            } else {
                return ApiResult.getInstance(null,1,"startDbMigrationTask " + taskId + " fail!");
            }
        } catch (SQLException e) {
            logger.error("sql error in startDbMigrationTask", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        } catch (ConsoleException e) {
            logger.warn("startDbMigrationTask forbidden", e);
            return ApiResult.getInstance(null,1, e.getMessage());
        }
    }
    

    @GetMapping("status")
    @SuppressWarnings("unchecked")
    public ApiResult<String> refreshAndGetTaskStatus(@RequestParam(name = "taskId") Long taskId, @RequestParam boolean careNewMha) {
        try {
            Pair<String, String> statusAndTips;
            MigrationTypeEnum typeEnum = careNewMha ?  MigrationTypeEnum.COMMON_CHECK_NEW_MHA : MigrationTypeEnum.COMMON_CHECK_OLD_MHA;
            statusAndTips = dbMigrationServiceV2.getAndUpdateTaskStatus(taskId,careNewMha, typeEnum);
            String tip = statusAndTips.getLeft();
            String status = statusAndTips.getRight();
            if (StringUtils.isEmpty(status)) {
                return ApiResult.getFailInstance(null, "task not exist: " + taskId);
            }
            return StringUtils.isEmpty(tip) ? ApiResult.getSuccessInstance(status) : ApiResult.getSuccessInstance(status, tip);
        } catch (Throwable e) {
            logger.error("getTaskStatus", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("query")
    @SuppressWarnings("unchecked")
    public ApiResult<PageResult<MigrationTaskVo>> queryByPage(MigrationTaskQuery queryDto) {
        logger.info("[meta] get allOrderedGroup,drcGroupQueryDto:{}", queryDto);
        if (queryDto == null) {
            queryDto = new MigrationTaskQuery();
        }
        try {
            queryDto.clean();
            PageResult<MigrationTaskTbl> tblPageResult = dbMigrationServiceV2.queryByPage(queryDto);
            if (tblPageResult.getTotalCount() == 0) {
                return ApiResult.getSuccessInstance(PageResult.emptyResult());
            }
            List<MigrationTaskTbl> data = tblPageResult.getData();
            List<MigrationTaskVo> res = data.stream().map(MigrationTaskVo::from).collect(Collectors.toList());

            return ApiResult.getSuccessInstance(
                    PageResult.newInstance(res, tblPageResult.getPageIndex(), tblPageResult.getPageSize(), tblPageResult.getTotalCount())
            );
        } catch (Throwable e) {
            logger.error("queryByPage error", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("allStatus")
    @SuppressWarnings("unchecked")
    public ApiResult<List<String>> allStatus() {
        List<String> statusList = Arrays.stream(MigrationStatusEnum.values())
                .map(MigrationStatusEnum::getStatus)
                .collect(Collectors.toList());

        return ApiResult.getSuccessInstance(statusList);
    }

    @PostMapping("afterDalSwtich/commit")
    @LogRecord(type = OperateTypeEnum.DB_MIGRATION, attr = OperateAttrEnum.DELETE,operator = "DBA",
            success = "offlineOldDrcConfig with taskId:{#taskId}")
    public ApiResult<String> offlineOldDrcConfig(@RequestParam long taskId) {
        try {
            dbMigrationServiceV2.offlineOldDrcConfig(taskId);
            return ApiResult.getSuccessInstance("success");
        } catch (Exception e) {
            return ApiResult.getFailInstance("fail", e.getMessage());
        }
    }

    @PostMapping("afterDalSwtich/rollback")
    @LogRecord(type = OperateTypeEnum.DB_MIGRATION, attr = OperateAttrEnum.DELETE,operator = "DBA",
            success = "rollBackNewDrcConfig with taskId:{#taskId}")
    public ApiResult<String> rollBackNewDrcConfig(@RequestParam long taskId) {
        try {
            dbMigrationServiceV2.rollBackNewDrcConfig(taskId);
            return ApiResult.getSuccessInstance("success");
        } catch (Exception e) {
            return ApiResult.getFailInstance("fail", e.getMessage());
        }
    }

    @DeleteMapping("/mha/replicator")
    @LogRecord(type = OperateTypeEnum.DB_MIGRATION, attr = OperateAttrEnum.DELETE,operator = "admin",
            success = "deleteReplicator with mhaName:{#mhaName}")
    public ApiResult<String> deleteReplicator(@RequestParam String mhaName) {
        try {
            dbMigrationServiceV2.deleteReplicator(mhaName);
            return ApiResult.getSuccessInstance("success");
        } catch (Exception e) {
            return ApiResult.getFailInstance("fail", e.getMessage());
        }
    }

    // 之前sgp搬迁使用
    @PostMapping("mhaReplication")
    @LogRecord(type = OperateTypeEnum.MHA_MIGRATION, attr = OperateAttrEnum.UPDATE,operator = "admin",
            success = "migrateMhaReplication with newMha:{#newMha}, oldMha:{#oldMha}")
    public ApiResult<Boolean> migrateMhaReplication(@RequestParam String newMha, @RequestParam String oldMha) {
        try {
            dbMigrationServiceV2.migrateMhaReplication(newMha, oldMha);
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            logger.error("migrateMhaReplication fail", e);
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @PostMapping("mha/replicator/preStart")
    @LogRecord(type = OperateTypeEnum.MHA_MIGRATION, attr = OperateAttrEnum.ADD,operator = "admin",
            success = "preStartReplicator with newMha:{#newMha}, oldMha:{#oldMha}")
    public ApiResult<Boolean> preStartReplicator(@RequestParam String newMha, @RequestParam String oldMha) {
        try {
            dbMigrationServiceV2.preStartReplicator(newMha, oldMha);
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            logger.error("preStartReplicator fail", e);
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @GetMapping("mhaDbReplicationDelay")
    @SuppressWarnings("unchecked")
    public ApiResult<List<MhaApplierDto>> getDelay(@RequestParam(name = "taskId") Long taskId) {
        try {
            List<MhaApplierDto> delay = dbMigrationServiceV2.getMhaDbReplicationDelayFromMigrateTask(taskId);
            return ApiResult.getSuccessInstance(delay);
        } catch (Exception e) {
            logger.error("getDelay fail", e);
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    @GetMapping("cleanApplierDirtyData")
    public ApiResult<Map<String, List<Long>>> cleanApplierDirtyData (@RequestParam(name = "showOnly", defaultValue = "true") boolean showOnly) {
        try {
            Map<String, List<Long>> result =  dbMigrationServiceV2.cleanApplierDirtyData(showOnly);
            return ApiResult.getSuccessInstance(result);
        } catch (Exception e) {
            logger.error("cleanApplierDirtyData fail", e);
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }

    /**
     * 迁移sgp db步骤：
     * 1.新增sha->new sgp DBA断临时同步
     * 2.新增new sgp -> sha
     * 3.删除sha<->old sgp DBA刷数据
     * 全部都是实时位点
     */

    @PutMapping("oversea/checkAndCreateTask")
    @LogRecord(type = OperateTypeEnum.DB_MIGRATION, attr = OperateAttrEnum.ADD, operator = "DBA",
            success = "overseaDbMigrationCheckAndInit with DbMigrationParam:{#dbMigrationParam.toString()}")
    public ApiResult overseaDbMigrationCheckAndInit(@RequestBody DbMigrationParam dbMigrationParam) {
        try {
            Pair<String, Long> tipsAndTaskId = dbMigrationServiceV2.dbMigrationCheckAndCreateTask(dbMigrationParam, MigrationTypeEnum.OVERSEA_INIT);
            if (tipsAndTaskId.getRight() == null) {
                return ApiResult.getInstance(null, 2, "no dbDrcRelated");
            } else {
                return ApiResult.getInstance(tipsAndTaskId.getRight(), 0, tipsAndTaskId.getLeft());
            }
        } catch (SQLException e) {
            logger.error("sql error in overseaDbMigrationCheckAndInit", e);
            return ApiResult.getInstance(null, 1, e.getMessage());
        } catch (ConsoleException e) {
            logger.warn("overseaDbMigrationCheckAndInit forbidden", e);
            return ApiResult.getInstance(null, 1, e.getMessage());
        }
    }

    @PostMapping("oversea/preStart")
    @LogRecord(type = OperateTypeEnum.DB_MIGRATION, attr = OperateAttrEnum.UPDATE, operator = "DBA",
            success = "preStartOverseaDbMigrationTask with taskId:{#taskId}")
    public ApiResult preStartOverseaDbMigrationTask(@RequestParam(name = "taskId") Long taskId) {
        try {
            boolean preStartResult = dbMigrationServiceV2.preStartDbMigrationTask(taskId, MigrationTypeEnum.OVERSEA_PRESTART);
            if (preStartResult) {
                return ApiResult.getInstance(null, 0, "exStartDbMigrationTask: " + taskId + " success!");
            } else {
                return ApiResult.getInstance(null, 1, "exStartDbMigrationTask: " + taskId + " fail!");
            }
        } catch (SQLException e) {
            logger.error("sql error in exStartOverseaDbMigrationTask", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        } catch (ConsoleException e) {
            logger.warn("exStartOverseaDbMigrationTask forbidden", e);
            return ApiResult.getInstance(null, 1, e.getMessage());
        }
    }

    @GetMapping("oversea/preStartStatus")
    public ApiResult overseaDbMigrationTaskPreStartStatus(@RequestParam(name = "taskId") Long taskId) {
        try {
            Pair<Boolean, String> res = dbMigrationServiceV2.checkPreStartStatus(taskId);
            if (res.getLeft()) {
                return ApiResult.getSuccessInstance(res.getRight());
            } else {
                return ApiResult.getSuccessInstance("notReady");
            }
        } catch (Throwable e) {
            logger.error("overseaDbMigrationTaskPreStartStatus error", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }


    @PostMapping("oversea/start/shaToOversea")
    @LogRecord(type = OperateTypeEnum.DB_MIGRATION, attr = OperateAttrEnum.UPDATE, operator = "DBA",
            success = "startShaToOversea with taskId:{#taskId}")
    public ApiResult startShaToOversea(@RequestParam(name = "taskId") Long taskId) {
        try {
            boolean startResult = dbMigrationServiceV2.startDbMigrationTask(taskId, MigrationTypeEnum.OVERSEA_START_SHA_TO_OVERSEA);
            if (startResult) {
                return ApiResult.getInstance(null, +
                        0, "startShaToOversea " + taskId + " success!");
            } else {
                return ApiResult.getInstance(null, 1, "startDbMigrationTask " + taskId + " fail!");
            }
        } catch (SQLException e) {
            logger.error("sql error in startShaToOversea", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        } catch (ConsoleException e) {
            logger.warn("startShaToOversea forbidden", e);
            return ApiResult.getInstance(null, 1, e.getMessage());
        }
    }

    @PostMapping("oversea/start/overseaToSha")
    @LogRecord(type = OperateTypeEnum.DB_MIGRATION, attr = OperateAttrEnum.UPDATE, operator = "DBA",
            success = "startOverseaToSha with taskId:{#taskId}")
    public ApiResult startOverseaToSha(@RequestParam(name = "taskId") Long taskId) {
        try {
            boolean startResult = dbMigrationServiceV2.startDbMigrationTask(taskId, MigrationTypeEnum.OVERSEA_START_OVERSEA_TO_SHA);
            if (startResult) {
                return ApiResult.getInstance(null, +
                        0, "startOverseaToSha " + taskId + " success!");
            } else {
                return ApiResult.getInstance(null, 1, "startDbMigrationTask " + taskId + " fail!");
            }
        } catch (SQLException e) {
            logger.error("sql error in startOverseaToSha", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        } catch (ConsoleException e) {
            logger.warn("startOverseaToSha forbidden", e);
            return ApiResult.getInstance(null, 1, e.getMessage());
        }
    }

    //查询sha -> 新海外集群的同步延迟
    @GetMapping("oversea/shaToOverseaStatus")
    @SuppressWarnings("unchecked")
    public ApiResult<String> refreshAndGetShaToOverseaStatus(@RequestParam(name = "taskId") Long taskId) {
        try {
            Pair<String, String> statusAndTips;
            statusAndTips = dbMigrationServiceV2.getAndUpdateTaskStatus(taskId, true, MigrationTypeEnum.OVERSEA_CHECK_SHA_TO_OVERSEA);
            String tip = statusAndTips.getLeft();
            String status = statusAndTips.getRight();
            if (StringUtils.isEmpty(status)) {
                return ApiResult.getFailInstance(null, "task not exist: " + taskId);
            }
            return StringUtils.isEmpty(tip) ? ApiResult.getSuccessInstance(status) : ApiResult.getSuccessInstance(status, tip);
        } catch (Throwable e) {
            logger.error("getShaToOverseaStatus", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    //查询新海外集群 -> sha的同步延迟
    @GetMapping("oversea/overseaToShaStatus")
    @SuppressWarnings("unchecked")
    public ApiResult<String> refreshAndGetOverseaToShaStatus(@RequestParam(name = "taskId") Long taskId) {
        try {
            Pair<String, String> statusAndTips;
            statusAndTips = dbMigrationServiceV2.getAndUpdateTaskStatus(taskId, true, MigrationTypeEnum.OVERSEA_CHECK_OVERSEA_TO_SHA);
            String tip = statusAndTips.getLeft();
            String status = statusAndTips.getRight();
            if (StringUtils.isEmpty(status)) {
                return ApiResult.getFailInstance(null, "task not exist: " + taskId);
            }
            return StringUtils.isEmpty(tip) ? ApiResult.getSuccessInstance(status) : ApiResult.getSuccessInstance(status, tip);
        } catch (Throwable e) {
            logger.error("getOverseaToShaStatus", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PostMapping("oversea/cancel")
    @LogRecord(type = OperateTypeEnum.DB_MIGRATION, attr = OperateAttrEnum.UPDATE, operator = "DBA",
            success = "cancelOverseaDbMigrationTask with taskId:{#taskId}")
    public ApiResult cancelOverseaDbMigrationTask(@RequestParam(name = "taskId") Long taskId) {
        try {
            boolean cancelResult = dbMigrationServiceV2.cancelTask(taskId);
            if (cancelResult) {
                return ApiResult.getInstance(null, 0, "cancelOverseaDbMigrationTask: " + taskId + " success!");
            } else {
                return ApiResult.getInstance(null, 1, "cancelOverseaDbMigrationTask: " + taskId + " fail!");
            }
        } catch (SQLException e) {
            logger.error("sql error in cancelOverseaDbMigrationTask", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        } catch (ConsoleException e) {
            logger.warn("cancelOverseaDbMigrationTask forbidden", e);
            return ApiResult.getInstance(null, 1, e.getMessage());
        } catch (Exception e) {
            logger.error("cancelOverseaDbMigrationTask unExcepted error", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PostMapping("oversea/commit")
    @LogRecord(type = OperateTypeEnum.DB_MIGRATION, attr = OperateAttrEnum.DELETE, operator = "DBA",
            success = "overseaDbMigrationOfflineOldDrcConfig with taskId:{#taskId}")
    public ApiResult<String> overseaDbMigrationOfflineOldDrcConfig(@RequestParam long taskId) {
        try {
            dbMigrationServiceV2.offlineOldDrcConfig(taskId);
            return ApiResult.getSuccessInstance("success");
        } catch (Exception e) {
            return ApiResult.getFailInstance("fail", e.getMessage());
        }
    }


    @PostMapping("oversea/rollback")
    @LogRecord(type = OperateTypeEnum.DB_MIGRATION, attr = OperateAttrEnum.DELETE, operator = "DBA",
            success = "overseaDbMigrationRollBackNewDrcConfig with taskId:{#taskId}")
    public ApiResult<String> overseaDbMigrationRollBackNewDrcConfig(@RequestParam long taskId) {
        try {
            dbMigrationServiceV2.rollBackNewDrcConfig(taskId);
            return ApiResult.getSuccessInstance("success");
        } catch (Exception e) {
            return ApiResult.getFailInstance("fail", e.getMessage());
        }
    }


    @PutMapping("test/checkAndCreateTask")
    @LogRecord(type = OperateTypeEnum.DB_MIGRATION, attr = OperateAttrEnum.ADD,operator = "DBA",
            success = "dbMigrationCheckAndInit with DbMigrationParam:{#dbMigrationParam.toString()}")
    public ApiResult dbMigrationCheckAndInitTestEnv(@RequestBody DbMigrationParam dbMigrationParam) {
        try {
            Pair<String, Long> tipsAndTaskId = dbMigrationServiceV2.dbMigrationCheckAndCreateTask(dbMigrationParam, MigrationTypeEnum.TEST_INIT);
            if (tipsAndTaskId.getRight() == null) {
                return ApiResult.getInstance(null,2,"no dbDrcRelated");
            } else {
                dbMigrationServiceV2.quickCheckFwsNewMha(tipsAndTaskId.getRight());
                return ApiResult.getInstance(tipsAndTaskId.getRight(),0,tipsAndTaskId.getLeft());
            }
        } catch (SQLException e) {
            logger.error("sql error in dbMigrationCheckAndInit", e);
            return ApiResult.getInstance(null,1, e.getMessage());
        } catch (ConsoleException e) {
            logger.warn("dbMigrationCheckAndInit forbidden", e);
            return ApiResult.getInstance(null,1, e.getMessage());
        }
    }

    @PostMapping("test/startMigrate")
    @LogRecord(type = OperateTypeEnum.DB_MIGRATION, attr = OperateAttrEnum.UPDATE,operator = "DBA",
            success = "startMigrate with taskId:{#taskId}")
    public ApiResult startDbMigrationTaskTestEnv(@RequestParam(name = "taskId") Long taskId) {
        try {
            boolean preStartResult = dbMigrationServiceV2.preStartDbMigrationTask(taskId, MigrationTypeEnum.TEST_PRESTART);
            if (!preStartResult) {
                return ApiResult.getInstance(null,1,"preStartDbMigrationTask: " + taskId + " fail!");
            }
        } catch (Exception e) {
            logger.error("error in exStartDbMigrationTask", e);
            return ApiResult.getFailInstance("preStart fail", e.getMessage());
        }
        try {
            dbMigrationServiceV2.quickPassForFwsMigration(taskId, MigrationTypeEnum.TEST_PRESTART);
        } catch (SQLException e) {
            logger.error("sql error in quickPathForFwsMigration {}",MigrationTypeEnum.TEST_PRESTART, e);
            return ApiResult.getFailInstance("quick pass fail", e.getMessage());
        }
        try {
            boolean startResult = dbMigrationServiceV2.startDbMigrationTask(taskId, MigrationTypeEnum.COMMON_START);
            if (!startResult) {
                return ApiResult.getInstance("start fail",1,"startDbMigrationTask " + taskId + " fail!");
            }
        } catch (Exception e) {
            logger.error("error in startDbMigrationTask", e);
            return ApiResult.getFailInstance("start fail", e.getMessage());
        }
        try {
            dbMigrationServiceV2.quickPassForFwsMigration(taskId, MigrationTypeEnum.COMMON_START);
        } catch (SQLException e) {
            logger.error("sql error in quickPathForFwsMigration {}",MigrationTypeEnum.COMMON_START, e);
            return ApiResult.getFailInstance("quick pass fail", e.getMessage());
        }
        try {
            dbMigrationServiceV2.offlineOldDrcConfig(taskId);
            return ApiResult.getSuccessInstance("success");
        } catch (Exception e) {
            return ApiResult.getFailInstance("fail", e.getMessage());
        }

    }

}
