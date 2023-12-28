package com.ctrip.framework.drc.console.controller.log;

import com.ctrip.framework.drc.console.aop.log.LogRecord;
import com.ctrip.framework.drc.console.enums.operation.OperateAttrEnum;
import com.ctrip.framework.drc.console.enums.operation.OperateTypeEnum;
import com.ctrip.framework.drc.console.param.log.ConflictApprovalCreateParam;
import com.ctrip.framework.drc.console.param.log.ConflictApprovalQueryParam;
import com.ctrip.framework.drc.console.service.log.ConflictApprovalService;
import com.ctrip.framework.drc.console.vo.log.*;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/10/31 19:37
 */
@RestController
@RequestMapping("/api/drc/v2/log/approval/")
public class ConflictApprovalController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ConflictApprovalService conflictApprovalService;

    @GetMapping("/list")
    public ApiResult<ConflictApprovalView> getConflictApprovalViews(ConflictApprovalQueryParam param) {
        try {
            ApiResult apiResult = ApiResult.getSuccessInstance(conflictApprovalService.getConflictApprovalViews(param));
            apiResult.setPageReq(param.getPageReq());
            return apiResult;
        } catch (Exception e) {
            logger.error("getConflictApprovalViews fail, ", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("/rows/detail")
    public ApiResult<ConflictTrxLogDetailView> getConflictRowLogDetailView(@RequestParam Long approvalId) {
        try {
            ApiResult apiResult = ApiResult.getSuccessInstance(conflictApprovalService.getConflictRowLogDetailView(approvalId));
            return apiResult;
        } catch (Exception e) {
            logger.error("getConflictRowLogDetailView fail, ", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("/rows/records")
    public ApiResult<ConflictCurrentRecordView> getConflictRowRecordView(@RequestParam Long approvalId) {
        try {
            ApiResult apiResult = ApiResult.getSuccessInstance(conflictApprovalService.getConflictRecordView(approvalId));
            return apiResult;
        } catch (Exception e) {
            logger.error("getConflictRowRecordView fail, ", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("/detail")
    public ApiResult<List<ConflictAutoHandleView>> getConflictAutoHandleView(@RequestParam Long approvalId) {
        try {
            ApiResult apiResult = ApiResult.getSuccessInstance(conflictApprovalService.getConflictAutoHandleView(approvalId));
            return apiResult;
        } catch (Exception e) {
            logger.error("getConflictAutoHandleView fail, ", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("/writeSide")
    public ApiResult<Integer> getWriteSide(@RequestParam Long approvalId) {
        try {
            ApiResult apiResult = ApiResult.getSuccessInstance(conflictApprovalService.getWriteSide(approvalId));
            return apiResult;
        } catch (Exception e) {
            logger.error("getConflictAutoHandleView fail, ", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PostMapping("/callback")
    public ApiResult<Boolean> approvalCallBack(@RequestBody ConflictApprovalCallBackRequest request) {
        try {
            logger.info("approval callback, request: {}", request);
            conflictApprovalService.approvalCallBack(request);
            ApiResult apiResult = ApiResult.getSuccessInstance(true);
            return apiResult;
        } catch (Exception e) {
            logger.error("approvalCallBack fail, ", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PostMapping("/create")
    public ApiResult<Boolean> createConflictApproval(@RequestBody ConflictApprovalCreateParam param) {
        try {
            conflictApprovalService.createConflictApproval(param);
            ApiResult apiResult = ApiResult.getSuccessInstance(true);
            return apiResult;
        } catch (Exception e) {
            logger.error("createConflictApproval fail, ", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PostMapping("/execute")
    @LogRecord(type = OperateTypeEnum.CONFLICT_RESOLUTION, attr = OperateAttrEnum.UPDATE,
            success = "executeApproval with approvalId : {#approvalId}")
    public ApiResult<Boolean> executeApproval(@RequestParam Long approvalId) {
        logger.info("executeApproval approvalId: {}", approvalId);
        try {
            conflictApprovalService.executeApproval(approvalId);
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            logger.error("executeApproval fail, ", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }
}
