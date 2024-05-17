package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.core.http.ApiResult;
import javax.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

/**
 * @ClassName PermissionController
 * @Author haodongPan
 * @Date 2023/10/25 17:07
 * @Version: $ check user permission to open a no api page
 * detail: see IamFilter in trip-service module
 */
@RestController
@RequestMapping("/api/drc/v2/permission/")
public class PermissionController {
    
    private static final Logger logger = LoggerFactory.getLogger(PermissionController.class);
    
    @GetMapping("meta/mhaReplication/query")
    public ApiResult<Boolean> mhaReplicationQuery() {
        return ApiResult.getSuccessInstance(true);
    }

    @GetMapping("meta/mhaReplication/modify")
    public ApiResult<Boolean> mhaReplicationModify() {
        return ApiResult.getSuccessInstance(true);
    }

    @GetMapping("meta/dbReplication/query")
    public ApiResult<Boolean> dbReplicationQuery() {
        return ApiResult.getSuccessInstance(true);
    }

    @GetMapping("meta/dbReplication/modify")
    public ApiResult<Boolean> dbReplicationModify() {
        ServletRequestAttributes requestAttributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        HttpServletRequest request = requestAttributes.getRequest();
        String sourceIp = request.getHeader("X-Forwarded-For");
        String header = request.toString();
        logger.info("sourceIp: {}, header: {}", sourceIp, header);
        return ApiResult.getSuccessInstance(true);
    }
    
    @GetMapping("meta/mqReplication/query")
    public ApiResult<Boolean> mqReplicationQuery() {
        return ApiResult.getSuccessInstance(true);
    }

    @GetMapping("meta/mqReplication/modify")
    public ApiResult<Boolean> mqReplicationModify() {
        return ApiResult.getSuccessInstance(true);
    }
    
    @GetMapping("meta/rowsFilterMark")
    public ApiResult<Boolean> rowsFilterMark() {
        return ApiResult.getSuccessInstance(true);
    }
    
    @GetMapping("ops/dbmigration")
    public ApiResult<Boolean> dbMigration() {
        return ApiResult.getSuccessInstance(true);
    }

    @GetMapping("ops/conflictLog")
    public ApiResult<Boolean> conflictLog() {
        return ApiResult.getSuccessInstance(true);
    }

    @GetMapping("ops/operationLog")
    public ApiResult<Boolean> operationLog() {
        return ApiResult.getSuccessInstance(true);
    }
    
    @GetMapping("resource/machine")
    public ApiResult<Boolean> resourceMachine() {
        return ApiResult.getSuccessInstance(true);
    }
    
    @GetMapping("resource/proxy")
    public ApiResult<Boolean> proxy() {
        return ApiResult.getSuccessInstance(true);
    }

    @GetMapping("resource/route")
    public ApiResult<Boolean> route() {
        return ApiResult.getSuccessInstance(true);
    }

    @GetMapping("approval/drc")
    public ApiResult<Boolean> drcApplication() {
        return ApiResult.getSuccessInstance(true);
    }
    
}
