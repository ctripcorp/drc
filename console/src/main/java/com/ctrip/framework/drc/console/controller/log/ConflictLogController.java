package com.ctrip.framework.drc.console.controller.log;

import com.ctrip.framework.drc.console.param.log.ConflictAutoHandleParam;
import com.ctrip.framework.drc.console.param.log.ConflictRowsLogQueryParam;
import com.ctrip.framework.drc.console.param.log.ConflictTrxLogQueryParam;
import com.ctrip.framework.drc.console.service.log.ConflictLogService;
import com.ctrip.framework.drc.console.vo.log.*;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.fetcher.conflict.ConflictTransactionLog;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * Created by dengquanliang
 * 2023/9/27 14:19
 */
@RestController
@RequestMapping("/api/drc/v2/log/conflict/")
public class ConflictLogController {

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

    @GetMapping("rows/rowLogIds")
    public ApiResult<List<ConflictRowsLogView>> getConflictRowsLogView(@RequestParam List<Long> rowLogIds) {
        try {
            ApiResult apiResult = ApiResult.getSuccessInstance(conflictLogService.getConflictRowsLogView(rowLogIds));
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

    @GetMapping("records")
    public ApiResult<ConflictCurrentRecordView> getConflictCurrentRecordView(@RequestParam long conflictTrxLogId, @RequestParam int columnSize) {
        try {
            ApiResult apiResult = ApiResult.getSuccessInstance(conflictLogService.getConflictCurrentRecordView(conflictTrxLogId, columnSize));
            return apiResult;
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("/row/record")
    public ApiResult<ConflictCurrentRecordView> getConflictRowRecordView(@RequestParam long conflictRowLogId, @RequestParam int columnSize) {
        try {
            ApiResult apiResult = ApiResult.getSuccessInstance(conflictLogService.getConflictRowRecordView(conflictRowLogId, columnSize));
            return apiResult;
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("/records/compare")
    public ApiResult<ConflictRowsRecordCompareView> compareRowRecords(@RequestParam List<Long> conflictRowLogIds) {
        try {
            ApiResult apiResult = ApiResult.getSuccessInstance(conflictLogService.compareRowRecords(conflictRowLogIds));
            return apiResult;
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PostMapping("")
    public ApiResult<Boolean> createConflictLog(@RequestBody List<ConflictTransactionLog> trxLogs) {
        try {
            conflictLogService.createConflictLog(trxLogs);
            return ApiResult.getSuccessInstance(true);
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @DeleteMapping("/v2")
    public ApiResult<Long> deleteTrxLogs(@RequestParam long beginTime, @RequestParam long endTime) {
        try {
            long result = conflictLogService.deleteTrxLogs(beginTime, endTime);
            return ApiResult.getSuccessInstance(result);
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @DeleteMapping("")
    public ApiResult<Map<String, Integer>> deleteRowLogs(@RequestParam long beginTime, @RequestParam long endTime) {
        try {
            return ApiResult.getSuccessInstance(conflictLogService.deleteTrxLogsByTime(beginTime, endTime));
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("/rows/check")
    public ApiResult<Pair<String, String>> checkSameMhaReplication(@RequestParam List<Long> conflictRowLogIds) {
        try {
            ApiResult apiResult = ApiResult.getSuccessInstance(conflictLogService.checkSameMhaReplication(conflictRowLogIds));
            return apiResult;
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("/rows/detail")
    public ApiResult<List<ConflictRowsLogDetailView>> getConflictRowLogDetailView(@RequestParam List<Long> conflictRowLogIds) {
        try {
            ApiResult apiResult = ApiResult.getSuccessInstance(conflictLogService.getConflictRowLogDetailView(conflictRowLogIds));
            return apiResult;
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("/rows/records")
    public ApiResult<ConflictCurrentRecordView> getConflictRowRecordView(@RequestParam List<Long> conflictRowLogIds) {
        try {
            ApiResult apiResult = ApiResult.getSuccessInstance(conflictLogService.getConflictRowRecordView(conflictRowLogIds));
            return apiResult;
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PostMapping("/rows/handleSql")
    public ApiResult<ConflictAutoHandleView> createHandleSql(@RequestBody ConflictAutoHandleParam param) {
        try {
            ApiResult apiResult = ApiResult.getSuccessInstance(conflictLogService.createHandleSql(param));
            return apiResult;
        } catch (Exception e) {
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }
}
