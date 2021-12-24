package com.ctrip.framework.drc.console.controller;

import com.ctrip.framework.drc.console.dao.entity.UnitRouteVerificationHistoryTbl;
import com.ctrip.framework.drc.console.dto.ConflictTransactionLog;
import com.ctrip.framework.drc.console.dto.LogDto;
import com.ctrip.framework.drc.console.service.LogService;
import com.ctrip.framework.drc.console.service.checker.ConflictLogChecker;
import com.ctrip.framework.drc.console.service.checker.UserAccessChecker;
import com.ctrip.framework.drc.console.service.impl.UnitServiceImpl;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.server.config.validation.dto.ValidationResultDto;
import com.ctrip.platform.dal.dao.DalHints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Created by jixinwang on 2020/6/22
 */
@Validated
@RestController
@RequestMapping("/api/drc/v1")
public class LogController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private LogService logService;

    private UserAccessChecker userAccessChecker;

    private ConflictLogChecker conflictLogChecker;

    @Autowired
    private UnitServiceImpl unitService;

    public LogController(LogService logService, UserAccessChecker userAccessChecker, ConflictLogChecker conflictLogChecker) {
        this.logService = logService;
        this.userAccessChecker = userAccessChecker;
        this.conflictLogChecker = conflictLogChecker;
    }

    @PostMapping(value = "/logs/conflicts")
    public ApiResult uploadConflictLog(@RequestBody List<ConflictTransactionLog> conflictTransactionLogList) {
        logger.info("[API] conflict log, LogList: {}", conflictTransactionLogList);
        // black list check
        if (conflictLogChecker.inBlackList(conflictTransactionLogList.get(0).getClusterName())) {
            return ApiResult.getInstance(null, ResultCode.HANDLE_FAIL.getCode(), "dal cluster no permission!");
        }
        logService.uploadConflictLog(conflictTransactionLogList);
        return ApiResult.getSuccessInstance(null);
    }

    @GetMapping(value = "/logs/conflicts/{pageNo}/{pageSize}")
    public ApiResult getConflictLog(@PathVariable int pageNo, @PathVariable int pageSize, @RequestParam(value = "keyWord", defaultValue = "") String keyWord) {
        return ApiResult.getSuccessInstance(logService.getConflictLog(pageNo, pageSize, keyWord));
    }

    @GetMapping(value = "/logs/record/conflicts/{primaryKey}")
    public ApiResult getCurrentRecord(@PathVariable long primaryKey) {
        try {
            Map<String, Object> ret = logService.getCurrentRecord(primaryKey);
            return ApiResult.getSuccessInstance(ret);
        } catch (Exception e) {
            return ApiResult.getFailInstance(e);
        }
    }

    @PostMapping(value = "/logs/samples")
    public ApiResult uploadSampleLog(@RequestBody LogDto logDto) {
        logger.info("[API] sample log, LogDto: {}", logDto);
        logService.uploadSampleLog(logDto);
        return ApiResult.getSuccessInstance(null);
    }

    @GetMapping(value = "logs/{pageNo}/{pageSize}")
    public ApiResult getLogs(@PathVariable int pageNo, @PathVariable int pageSize, @RequestParam(value = "keyWord", defaultValue = "") String keyWord) {
        return ApiResult.getSuccessInstance(logService.getLogs(pageNo, pageSize, keyWord));
    }


    @DeleteMapping(value = "/logs")
    public ApiResult deleteLog() {
        logService.deleteLog(new DalHints());
        return ApiResult.getSuccessInstance(null);
    }

    @PostMapping(value = "logs/record/conflicts/update")
    public ApiResult updateRecord(@RequestBody Map<String, String> updateInfo) {
        try {
            String userName = updateInfo.get("userName");
            // access check
            if (!userAccessChecker.isAllowed(userName)) {
                return ApiResult.getInstance(null, ResultCode.HANDLE_FAIL.getCode(), "No operation permission!");
            }
            logService.updateRecord(updateInfo);
            return ApiResult.getSuccessInstance("success");
        } catch (Exception e) {
            return ApiResult.getInstance(null, ResultCode.HANDLE_FAIL.getCode(), e.toString());
        }
    }

    @PutMapping(value = "/logs/unit")
    public ApiResult addUnitRouteVerificationResult(@RequestBody ValidationResultDto validationResultDto) {
        logger.info("[Unit] add unit route verification result for gtid: {}", validationResultDto.getGtid());
        logger.debug("[Unit] add unit route verification result: {}", validationResultDto.toString());
        try {
            if(unitService.addUnitRouteVerificationResult(validationResultDto)) {
                return ApiResult.getSuccessInstance(null);
            }
        } catch(SQLException e) {
            logger.error("[Unit] fail add unit route verification result: {}", validationResultDto, e);
        }
        return ApiResult.getFailInstance(null);
    }

    @PutMapping(value = "/logs/unit/batch")
    public ApiResult batchAddUnitRouteVerificationResult(@RequestBody List<ValidationResultDto> validationResultDtos) {
        boolean allAdded = true;
        for(ValidationResultDto validationResultDto : validationResultDtos) {
            logger.info("[Unit] add unit route verification result for gtid: {}", validationResultDto.getGtid());
            logger.debug("[Unit] add unit route verification result: {}", validationResultDto.toString());
            try {
                if(!unitService.addUnitRouteVerificationResult(validationResultDto)) {
                    allAdded = false;
                }
            } catch(Exception e) {
                logger.error("[Unit] fail add unit route verification result: {}", validationResultDto, e);
                allAdded = false;
            }
        }
        return allAdded ? ApiResult.getSuccessInstance(null) : ApiResult.getFailInstance(null);
    }

    @GetMapping(value = "/logs/unit/mhas/{mha}")
    public ApiResult getUnitRouteVerificationResult(@PathVariable String mha,
                                                    @RequestParam(value="schemaName", required=false) String schemaName,
                                                    @RequestParam(value="tableName", required=false) String tableName) {
        logger.info("[Unit] get unit route verification for group of {}[{}.{}]", mha, schemaName, tableName);
        List<UnitRouteVerificationHistoryTbl> verificationResult = unitService.getUnitRouteVerificationResult(mha, schemaName, tableName);
        return ApiResult.getSuccessInstance(verificationResult);
    }

}
