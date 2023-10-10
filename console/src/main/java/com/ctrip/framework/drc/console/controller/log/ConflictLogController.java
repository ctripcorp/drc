package com.ctrip.framework.drc.console.controller.log;

import com.ctrip.framework.drc.console.param.log.ConflictRowsLogQueryParam;
import com.ctrip.framework.drc.console.param.log.ConflictTrxLogQueryParam;
import com.ctrip.framework.drc.console.service.log.ConflictLogService;
import com.ctrip.framework.drc.console.vo.log.ConflictRowsLogView;
import com.ctrip.framework.drc.console.vo.log.ConflictTrxLogDetailView;
import com.ctrip.framework.drc.console.vo.log.ConflictTrxLogView;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/9/27 14:19
 */
@RestController
@RequestMapping("/api/drc/v2/conflict/log/")
public class ConflictLogController {
    private static final Logger logger = LoggerFactory.getLogger(ConflictLogController.class);

    @Autowired
    private ConflictLogService conflictLogService;

    @GetMapping("trx")
    public ApiResult<List<ConflictTrxLogView>> getConflictTrxLogView(ConflictTrxLogQueryParam param) {
        try {
            ApiResult apiResult = ApiResult.getSuccessInstance(conflictLogService.getConflictTrxLogView(param));
            apiResult.setPageReq(param.getPageReq());
            return apiResult;
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("rows")
    public ApiResult<List<ConflictRowsLogView>> getConflictRowsLogView(ConflictRowsLogQueryParam param) {
        try {
            ApiResult apiResult = ApiResult.getSuccessInstance(conflictLogService.getConflictRowsLogView(param));
            apiResult.setPageReq(param.getPageReq());
            return apiResult;
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("detail")
    public ApiResult<ConflictTrxLogDetailView> getConflictTrxLogDetailView(@RequestParam long conflictTrxLogId) {
        try {
            ApiResult apiResult = ApiResult.getSuccessInstance(conflictLogService.getConflictTrxLogDetailView(conflictTrxLogId));
            return apiResult;
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }
}
