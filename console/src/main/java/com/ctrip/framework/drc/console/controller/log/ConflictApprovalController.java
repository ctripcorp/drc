package com.ctrip.framework.drc.console.controller.log;

import com.ctrip.framework.drc.console.param.log.ConflictApprovalQueryParam;
import com.ctrip.framework.drc.console.service.log.ConflictApprovalService;
import com.ctrip.framework.drc.console.vo.log.*;
import com.ctrip.framework.drc.core.http.ApiResult;
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

    @Autowired
    private ConflictApprovalService conflictApprovalService;

    @GetMapping("/list")
    public ApiResult<ConflictApprovalView> getConflictApprovalViews(@RequestBody ConflictApprovalQueryParam param) {
        try {
            ApiResult apiResult = ApiResult.getSuccessInstance(conflictApprovalService.getConflictApprovalViews(param));
            apiResult.setPageReq(param.getPageReq());
            return apiResult;
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("/rows/detail")
    public ApiResult<List<ConflictRowsLogDetailView>> getConflictRowLogDetailView(@RequestParam Long approvalId) {
        try {
            ApiResult apiResult = ApiResult.getSuccessInstance(conflictApprovalService.getConflictRowLogDetailView(approvalId));
            return apiResult;
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("/rows/records")
    public ApiResult<ConflictCurrentRecordView> getConflictRowRecordView(@RequestParam Long approvalId) {
        try {
            ApiResult apiResult = ApiResult.getSuccessInstance(conflictApprovalService.getConflictRecordView(approvalId));
            return apiResult;
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("/detail")
    public ApiResult<List<ConflictAutoHandleView>> getConflictAutoHandleView(@RequestParam Long batchId) {
        try {
            ApiResult apiResult = ApiResult.getSuccessInstance(conflictApprovalService.getConflictAutoHandleView(batchId));
            return apiResult;
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }


    @PostMapping("/callback")
    public ApiResult<Boolean> approvalCallBack(@RequestBody ConflictApprovalCallBackRequest request) {
        try {
            conflictApprovalService.approvalCallBack(request);
            ApiResult apiResult = ApiResult.getSuccessInstance(true);
            return apiResult;
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }
}
