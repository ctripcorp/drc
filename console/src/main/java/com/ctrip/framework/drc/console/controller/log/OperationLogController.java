package com.ctrip.framework.drc.console.controller.log;

import com.ctrip.framework.drc.console.enums.operation.OperateAttrEnum;
import com.ctrip.framework.drc.console.enums.operation.OperateTypeEnum;
import com.ctrip.framework.drc.console.param.log.OperationLogQueryParam;
import com.ctrip.framework.drc.console.service.log.OperationLogService;
import com.ctrip.framework.drc.console.vo.log.OperationLogView;
import com.ctrip.framework.drc.console.vo.log.OptionView;
import com.ctrip.framework.drc.core.http.ApiResult;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @ClassName OperationLogController
 * @Author haodongPan
 * @Date 2023/12/8 15:52
 * @Version: $
 */
@RestController
@RequestMapping("/api/drc/v2/log/operation/")
public class OperationLogController {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    @Autowired
    private OperationLogService operationLogService;
    
    @GetMapping("query")
    public ApiResult<List<OperationLogView>> getOperationLogView(OperationLogQueryParam param) {
        try {
            ApiResult apiResult = ApiResult.getSuccessInstance(operationLogService.getOperationLogView(param));
            apiResult.setPageReq(param.getPageReq());
            return apiResult;
        } catch (Exception e) {
            logger.error("getOperationLogView error", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("count")
    public ApiResult<Long> getOperationLogCount(OperationLogQueryParam param) {
        try {
            return ApiResult.getSuccessInstance(operationLogService.gerOperationLogCount(param));
        } catch (Exception e) {
            logger.error("getOperationLogCount fail", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }
    
    @GetMapping("type/all")
    public ApiResult<List<OptionView>> getAllType() {
        try {
            List<OptionView> res = Arrays.stream(OperateTypeEnum.values())
                    .map(operateTypeEnum -> new OptionView(operateTypeEnum.getName(), operateTypeEnum.getVal()))
                    .collect(Collectors.toList());
            return ApiResult.getSuccessInstance(res);
        } catch (Exception e) {
            logger.error("getAllType error", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
       
    }

    @GetMapping("attr/all")
    public ApiResult<List<OptionView>> getAllAttribute() {
        try {
            List<OptionView> res = Arrays.stream(OperateAttrEnum.values())
                    .map(operateType -> new OptionView(operateType.getName(), operateType.getVal()))
                    .collect(Collectors.toList());
            return ApiResult.getSuccessInstance(res);
        } catch (Exception e) {
            logger.error("getAllAttribute error", e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }
    
}
