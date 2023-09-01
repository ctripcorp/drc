package com.ctrip.framework.drc.console.controller.v2;


import com.ctrip.framework.drc.console.dao.entity.v2.MigrationTaskTbl;
import com.ctrip.framework.drc.console.dto.v2.DbMigrationParam;
import com.ctrip.framework.drc.console.enums.MigrationStatusEnum;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.param.v2.MigrationTaskQuery;
import com.ctrip.framework.drc.console.service.v2.MhaReplicationServiceV2;
import com.ctrip.framework.drc.console.service.v2.dbmigration.DbMigrationService;
import com.ctrip.framework.drc.console.vo.display.MigrationTaskVo;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.PageResult;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
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
    private DbMigrationService dbMigrationService;
    @Autowired
    private MhaReplicationServiceV2 mhaReplicationServiceV2;

    
    @DeleteMapping("abandon")
    private ApiResult abandonMigrationTask(@RequestParam(name = "taskId") Long taskId) {
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

    @PutMapping("checkAndCreateTask")
    public ApiResult dbMigrationCheckAndInit(@RequestBody DbMigrationParam dbMigrationParam) {
        try {
            Pair<String, Long> tipsAndTaskId = dbMigrationService.dbMigrationCheckAndCreateTask(dbMigrationParam);
            if (tipsAndTaskId.getRight() == null) {
                return ApiResult.getInstance(null,2,"no dbDrcRelated");
            } else {
                return ApiResult.getInstance("taskInit success" + tipsAndTaskId.getRight(),0,tipsAndTaskId.getLeft());
            }
        } catch (SQLException e) {
            logger.error("sql error in dbMigrationCheckAndInit", e);
            return ApiResult.getInstance(null,1, e.getMessage());
        } catch (ConsoleException e) {
            logger.warn("dbMigrationCheckAndInit forbidden", e);
            return ApiResult.getInstance(null,1, e.getMessage());
        }
    }
    
    @PostMapping("preStart")
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
    
    @PostMapping("start")
    public ApiResult startDbMigrationTask (@RequestParam(name = "taskId") Long taskId) {
        try {
            if (dbMigrationService.startDbMigrationTask(taskId)) {
                return ApiResult.getInstance(null,0,"startDbMigrationTask " + taskId + " success!");
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
    public ApiResult<String> getTaskStatus(@RequestParam(name = "taskId") Long taskId) {
        try {
            Pair<String, String> statusAndTips = dbMigrationService.getAndUpdateTaskStatus(taskId);
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
    public ApiResult<String> offlineOldDrcConfig(@RequestParam long taskId) {
        try {
            dbMigrationService.offlineOldDrcConfig(taskId);
            return ApiResult.getSuccessInstance("success");
        } catch (Exception e) {
            return ApiResult.getFailInstance("fail", e.getMessage());
        }
    }

    @PostMapping("afterDalSwtich/rollback")
    public ApiResult<String> rollBackNewDrcConfig(@RequestParam long taskId) {
        try {
            dbMigrationService.rollBackNewDrcConfig(taskId);
            return ApiResult.getSuccessInstance("success");
        } catch (Exception e) {
            return ApiResult.getFailInstance("fail", e.getMessage());
        }
    }

    @DeleteMapping("/mha/replicator")
    public ApiResult<String> deleteReplicator(@RequestParam String mhaName) {
        try {
            dbMigrationService.deleteReplicator(mhaName);
            return ApiResult.getSuccessInstance("success");
        } catch (Exception e) {
            return ApiResult.getFailInstance("fail", e.getMessage());
        }
    }
}
