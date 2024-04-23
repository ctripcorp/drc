package com.ctrip.framework.drc.console.controller.v2;


import com.ctrip.framework.drc.console.aop.log.LogRecord;
import com.ctrip.framework.drc.console.dao.entity.v2.MigrationTaskTbl;
import com.ctrip.framework.drc.console.dto.v2.DbMigrationParam;
import com.ctrip.framework.drc.console.enums.MigrationStatusEnum;
import com.ctrip.framework.drc.console.enums.operation.OperateAttrEnum;
import com.ctrip.framework.drc.console.enums.operation.OperateTypeEnum;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.param.v2.MigrationTaskQuery;
import com.ctrip.framework.drc.console.service.v2.dbmigration.DbMigrationService;
import com.ctrip.framework.drc.console.vo.display.MigrationTaskVo;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.PageResult;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

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
    private DbMigrationService dbMigrationService;

    
    @DeleteMapping("abandon")
    @LogRecord(type = OperateTypeEnum.DB_MIGRATION, attr = OperateAttrEnum.DELETE,operator = "admin",
            success = "abandonMigrationTask with taskId:{#taskId}")
    public ApiResult abandonMigrationTask(@RequestParam(name = "taskId") Long taskId) {
        try {
            if (dbMigrationService.abandonTask(taskId)) {
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
            Pair<String, Long> tipsAndTaskId = dbMigrationService.dbMigrationCheckAndCreateTask(dbMigrationParam);
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
            if (dbMigrationService.preStartDbMigrationTask(taskId)) {
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
    
    @PostMapping("beforeDrcStart/cancel")
    @LogRecord(type = OperateTypeEnum.DB_MIGRATION, attr = OperateAttrEnum.UPDATE,operator = "DBA",
            success = "cancelDbMigrationTask with taskId:{#taskId}")
    public ApiResult cancelDbMigrationTask(@RequestParam(name = "taskId") Long taskId) {
        try {
            if (dbMigrationService.cancelTask(taskId)) {
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
            if (dbMigrationService.startDbMigrationTask(taskId)) {
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
    public ApiResult<String> refreshAndGetTaskStatus(@RequestParam(name = "taskId") Long taskId,@RequestParam boolean careAllDelay) {
        try {
            Pair<String, String> statusAndTips = dbMigrationService.getAndUpdateTaskStatus(taskId,careAllDelay);
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
            PageResult<MigrationTaskTbl> tblPageResult = dbMigrationService.queryByPage(queryDto);
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
            dbMigrationService.offlineOldDrcConfig(taskId);
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
            dbMigrationService.rollBackNewDrcConfig(taskId);
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
            dbMigrationService.deleteReplicator(mhaName);
            return ApiResult.getSuccessInstance("success");
        } catch (Exception e) {
            return ApiResult.getFailInstance("fail", e.getMessage());
        }
    }

    @PostMapping("mhaReplication")
    @LogRecord(type = OperateTypeEnum.MHA_MIGRATION, attr = OperateAttrEnum.UPDATE,operator = "admin",
            success = "migrateMhaReplication with newMha:{#newMha}, oldMha:{#oldMha}")
    public ApiResult<Boolean> migrateMhaReplication(@RequestParam String newMha, @RequestParam String oldMha) {
        try {
            dbMigrationService.migrateMhaReplication(newMha, oldMha);
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
            dbMigrationService.preStartReplicator(newMha, oldMha);
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            logger.error("preStartReplicator fail", e);
            return ApiResult.getFailInstance(false, e.getMessage());
        }
    }
}
